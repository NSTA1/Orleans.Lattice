using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-shard write-ahead-log grain. Stores every captured
/// <see cref="WalRecord"/> destined for downstream shippers in a
/// monotonically-sequenced, append-only log via the configured
/// <see cref="IWalStorageProvider"/>. The append is the commit point -
/// a WAL failure surfaces to the originating writer rather than being
/// silently dropped.
/// <para>
/// Grain key format: <c>{treeId}/{partition}</c>. The foreground commit-log
/// writer hashes <see cref="WalRecord.Key"/> modulo
/// <see cref="LatticeOptions.WalPartitions"/> to pick the partition.
/// </para>
/// <para>
/// Implements the turn-safe batching protocol from the WAL design doc
/// (§4): callers receive a per-call <see cref="TaskCompletionSource{TResult}"/>
/// that completes once the containing batch is durably persisted by the
/// configured <see cref="IWalStorageProvider"/>. Batch limits
/// (<see cref="LatticeOptions.WalMaxBatchEntries"/> and
/// <see cref="LatticeOptions.WalMaxBatchBytes"/>) flush the
/// current batch before enqueueing an entry that would overflow it.
/// Up to <see cref="LatticeOptions.WalMaxPendingBatches"/> flushes can
/// be in-flight against the provider simultaneously - offset assignment
/// remains serialised under the grain turn so dense offsets are preserved
/// by construction, but each flush's <c>AppendBatchAsync</c> call runs
/// independently so the writer-side burst absorption is no longer capped
/// at <c>1 / provider_latency</c>. The default
/// (<see cref="LatticeOptions.DefaultWalMaxPendingBatches"/> = 8) is the
/// measured Azure Tables Standard sweet spot at the c2-iii operating
/// point; setting it to <c>1</c> restores the historical single-in-flight
/// protocol bit-for-bit.
/// On flush failure the affected batch's offsets are rolled back, every
/// TCS in the failed batch is faulted, and every later in-flight + the
/// currently-accumulating pending batch is faulted with the same
/// exception (their offsets are now stale). The grain re-synchronises
/// <c>_nextOffset</c> from <see cref="IWalStorageProvider.GetHighestOffsetAsync"/>
/// before re-opening for new appends so the dense-offset invariant
/// against the provider is restored even when later concurrent flushes
/// had already committed against now-orphaned offset windows.
/// </para>
/// </summary>
internal sealed class WalShardGrain(
    IGrainContext context,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILatticeMergeModeResolver modeResolver,
    ILatticeOriginClusterIdResolver clusterIdResolver,
    IWalRecordEncoder encoder) : IWalShardGrain, IGrainBase
{
    private string _treeId = "";
    private int _shardIndex;
    private IWalStorageProvider _provider = null!;
    private long _nextOffset;
    private bool _initialized;

    /// <summary>
    /// Placement version this activation resolved its <see cref="_provider"/>
    /// against (read fresh from the durable WAL placement pin at activation).
    /// Carried through <see cref="QuiesceForMoveAsync"/> so a move coordinator
    /// working from a stale view aborts rather than quiescing an activation that
    /// has already re-resolved against a newer placement.
    /// </summary>
    private long _placementVersion;

    /// <summary>
    /// Provider catalog key this activation's <see cref="_provider"/> was
    /// resolved under (<see cref="IWalStorageProviderCatalog.DefaultProviderKey"/>
    /// for the baseline / legacy resolver path). Reported back to the move
    /// coordinator for its audit receipt.
    /// </summary>
    private string _providerKey = IWalStorageProviderCatalog.DefaultProviderKey;

    /// <summary>
    /// When <see langword="true"/>, this activation is fenced for an in-progress
    /// administrative placement move: new appends are refused with
    /// <see cref="LatticeWalQuiescingException"/> so the move coordinator can
    /// copy a stable source tail and flip the placement pin without racing a
    /// concurrent writer. Mutated and read under <see cref="_stateGate"/>.
    /// </summary>
    private bool _moveFenced;

    /// <summary>
    /// <see cref="Stopwatch.GetTimestamp"/> deadline after which a fenced
    /// activation self-heals (deactivates so the next activation re-resolves
    /// placement from the durable pin) on the next append, bounding the outage
    /// if a move coordinator fails mid-move. Mutated and read under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private long _fenceDeadlineTicks;

    /// <summary>
    /// Cached <see cref="LatticeMetrics.TagTree"/> tag bound to this
    /// activation's <see cref="_treeId"/>. Reused on every WAL hot-path
    /// metric record to avoid per-record <see cref="KeyValuePair{TKey, TValue}"/>
    /// construction. Initialised in <see cref="OnActivateAsync"/> once
    /// the key has been parsed.
    /// </summary>
    private KeyValuePair<string, object?> _treeTag;

    /// <summary>
    /// Cached <see cref="LatticeMetrics.TagShard"/> tag bound to this
    /// activation's <see cref="_shardIndex"/>. Same allocation-free
    /// rationale as <see cref="_treeTag"/>.
    /// </summary>
    private KeyValuePair<string, object?> _shardTag;

    /// <summary>
    /// Cached <see cref="LatticeMetrics.TagWalPartitions"/> tag bound
    /// to this activation's effective <see cref="LatticeOptions.WalPartitions"/>
    /// setting. Resolved once on activation rather than per record so
    /// the WAL hot-path metric calls remain allocation-free. The plan's
    /// Phase A attribution sweep pivots on this tag (and on
    /// <see cref="_walMaxPendingBatchesTag"/>) to distinguish runs in
    /// a single Prometheus / dashboard query.
    /// </summary>
    private KeyValuePair<string, object?> _walPartitionsTag;

    /// <summary>
    /// Cached <see cref="LatticeMetrics.TagWalMaxPendingBatches"/> tag
    /// bound to this activation's effective
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> setting. Same
    /// activation-cached, allocation-free pattern as
    /// <see cref="_walPartitionsTag"/>; pivots the Phase A WAL
    /// instruments across in-flight-flush ceilings.
    /// </summary>
    private KeyValuePair<string, object?> _walMaxPendingBatchesTag;

    // Pending state shape after the zero-copy provider hand-off:
    // each pending entry contributes one pre-encoded payload segment
    // (carrying the bytes the canonical IWalRecordEncoder produced at
    // append time) and one parallel offset slot. The encoded bytes are
    // the same bytes the storage provider will see - the grain pays
    // exactly one encode per append rather than one for the byte budget
    // and a second one inside the provider.
    private List<ArraySegment<byte>> _pendingSegments = new();
    private List<long> _pendingOffsets = new();
    private List<TaskCompletionSource<long>> _pendingAcks = new();
    private long _pendingBatchSizeBytes;

    /// <summary>
    /// Ordered chain of in-flight flushes, oldest at the head. Each
    /// entry carries the offset window it owns and the ack TCSs parked
    /// against it. Offset assignment under the grain turn guarantees the
    /// windows are strictly increasing and non-overlapping.
    /// </summary>
    private readonly LinkedList<InFlightFlush> _inFlight = new();

    /// <summary>
    /// Gate covering every mutation of <see cref="_inFlight"/> and the
    /// pending-batch fields, plus reads that drive control flow off
    /// those fields. Under Orleans the grain-turn TaskScheduler already
    /// serialises continuations so the gate is uncontested; under unit
    /// tests there is no such scheduler and two <c>FlushAsync</c>
    /// continuations can run concurrently on threadpool threads, so the
    /// gate is required for correctness. Held only for the duration of
    /// a state mutation, never across an <c>await</c>.
    /// </summary>
    private readonly Lock _stateGate = new();

    /// <summary>
    /// Per-grain free-list of recycled segment-list batch buffers.
    /// Eliminates the per-flush
    /// <c>new List&lt;ArraySegment&lt;byte&gt;&gt;()</c> allocation in the
    /// steady-state hot path. Accessed only under
    /// <see cref="_stateGate"/>; the pre-existing gate makes a separate
    /// pool lock unnecessary. Depth is capped at
    /// <see cref="MaxPoolDepth"/> so a transient burst of concurrent
    /// flushes does not pin large buffers indefinitely.
    /// </summary>
    private readonly Stack<List<ArraySegment<byte>>> _segmentListPool = new();

    /// <summary>
    /// Per-grain free-list of recycled offset-list batch buffers.
    /// Same shape and invariants as <see cref="_segmentListPool"/>.
    /// </summary>
    private readonly Stack<List<long>> _offsetListPool = new();

    /// <summary>
    /// Per-grain free-list of recycled ack-TCS buffers. Same shape and
    /// invariants as <see cref="_batchListPool"/>. The lists themselves
    /// only ever hold strong references to TCSs that have been moved
    /// elsewhere (into an in-flight slot's <c>Acks</c>) before the list
    /// is returned, so recycling does not extend TCS lifetimes.
    /// </summary>
    private readonly Stack<List<TaskCompletionSource<long>>> _ackListPool = new();

    /// <summary>
    /// Maximum number of pooled lists retained per type. Bounded so a
    /// short burst at <see cref="LatticeOptions.WalMaxPendingBatches"/>
    /// concurrency does not pin lists across longer idle periods. The
    /// value is one greater than the largest practical
    /// <c>WalMaxPendingBatches</c> the design contemplates today, so
    /// the steady state under any reasonable cap allocates zero new
    /// lists after warm-up.
    /// </summary>
    private const int MaxPoolDepth = 16;

    /// <summary>
    /// Sticky failure latched the moment any flush in the chain fails.
    /// Until cleared by the post-failure resync, new appends short-circuit
    /// with this exception so a fault that already faulted later windows
    /// is not silently masked by a fresh successful append claiming
    /// offsets the provider may still hold from the orphaned windows.
    /// </summary>
    private Exception? _stickyFailure;

    /// <summary>
    /// Per-grain cancellation source signalled at deactivation entry
    /// by <see cref="OnDeactivateAsync"/> so every in-flight flush's
    /// provider call observes drain cancellation immediately rather than
    /// waiting for its own per-flush <see cref="LatticeOptions.WalFlushTimeout"/>
    /// deadline to fire. Constructed in
    /// <see cref="OnActivateAsync(CancellationToken)"/> /
    /// <see cref="InitializeForTestingAsync"/> at activation time so the
    /// source is available to link into every per-flush deadline CTS in
    /// <see cref="FlushAsync"/> (a deactivation that races a flush's
    /// deadline construction signals the already-linked token rather
    /// than missing the link). A co-operative provider (any
    /// implementation that observes its <see cref="CancellationToken"/>)
    /// gives up promptly when the drain is signalled; an un-cooperative
    /// provider's slot is force-faulted by the
    /// <see cref="LatticeOptions.WalDrainBudget"/> ceiling after the
    /// natural drain has had a chance to settle. Never explicitly
    /// disposed - a force-faulted FlushAsync's body may still be alive
    /// when deactivation returns, and
    /// <see cref="CancellationTokenSource.Token"/> throws
    /// <see cref="ObjectDisposedException"/> after disposal. The CTS is
    /// a small heap object that the GC reclaims with the activation.
    /// </summary>
    private CancellationTokenSource? _drainCts;

    /// <summary>
    /// Compiled-out diagnostic trace. Enabled by defining the
    /// <c>LATTICE_DIAG</c> preprocessor symbol on the build (e.g.
    /// <c>dotnet build /p:LatticeDiag=true</c>); emits a single line
    /// per flush lifecycle event with the slot's offset window and the
    /// thread id, so flush ordering can be reconstructed from the test
    /// output stream when the WAL turn-safe batching protocol is
    /// changed. Zero overhead in release builds because the method
    /// body is elided by <see cref="ConditionalAttribute"/>.
    /// </summary>
    [Conditional("LATTICE_DIAG")]
    private static void Trace(string message)
    {
        Console.WriteLine($"[WAL t{Environment.CurrentManagedThreadId,3}] {message}");
    }

    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// Per-call <see cref="LatticeOptions"/> resolved from the injected
    /// <see cref="IOptionsMonitor{TOptions}"/>. Resolving on every read
    /// (rather than capturing on activation) is the
    /// <c>BPlusTree/Grains/*Grain.cs</c> convention - it lets operators
    /// retune <see cref="LatticeOptions.WalMaxBatchEntries"/> and
    /// <see cref="LatticeOptions.WalMaxBatchBytes"/> at runtime without
    /// recycling the activation.
    /// </summary>
    private LatticeOptions Options => optionsMonitor.Get(_treeId);

    /// <summary>
    /// Recovers <c>_nextOffset</c> from the configured
    /// <see cref="IWalStorageProvider"/> on activation. The contract
    /// requires offsets to be dense and gap-free, so
    /// <c>_nextOffset = GetHighestOffsetAsync() + 1</c> is sufficient.
    /// </summary>
    public async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var key = context.GrainId.Key.ToString();
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(WalShardGrain)} activation key is empty; expected '{{treeId}}/{{partition}}'.");
        }

        var slash = key.LastIndexOf('/');
        if (slash <= 0 || slash >= key.Length - 1)
        {
            throw new InvalidOperationException(
                $"{nameof(WalShardGrain)} activation key '{key}' is not in the expected '{{treeId}}/{{partition}}' format.");
        }

        _treeId = key[..slash];
        if (!int.TryParse(key.AsSpan(slash + 1), out _shardIndex) || _shardIndex < 0)
        {
            throw new InvalidOperationException(
                $"{nameof(WalShardGrain)} activation key '{key}' has a non-integer or negative shard index suffix.");
        }

        _treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, _treeId);
        _shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, _shardIndex);

        var options = optionsMonitor.Get(_treeId);
        // Resolve WalPartitions through the tree-registry pin (via
        // LatticeOptionsResolver) so the metric tag reflects the
        // routing-truth shape used by WalCommitLogWriter and the
        // activation-time materialiser, not the live IOptionsMonitor
        // value that may have drifted since the tree was registered.
        var resolved = await optionsResolver.ResolveAsync(_treeId).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        // Phase A attribution tags. The values are captured once at
        // activation; if the operator retunes WalMaxPendingBatches
        // through IOptionsMonitor while activations are live, existing
        // activations continue to emit the value they activated under
        // (the WAL hot-path code already resolves WalMaxBatchEntries /
        // WalMaxBatchBytes per call, which is the documented dynamic-
        // tunability contract; the activation-cached tag is a deliberate
        // cardinality bound that prevents a high-frequency option toggle
        // from polluting the metric series). WalPartitions is tree-
        // immutable by virtue of the registry pin, so the activation-
        // cached tag matches the routing shape for the lifetime of the
        // tree.
        _walPartitionsTag = new KeyValuePair<string, object?>(
            LatticeMetrics.TagWalPartitions, resolved.WalPartitions);
        _walMaxPendingBatchesTag = new KeyValuePair<string, object?>(
            LatticeMetrics.TagWalMaxPendingBatches, options.WalMaxPendingBatches);

        // Resolve the provider through the durable per-partition WAL placement
        // pin (read fresh from the registry at activation - never on the append
        // hot path; the resolved provider is cached in _provider for the
        // activation lifetime). The default-key path preserves the legacy
        // LatticeOptions.WalStorageProvider resolver exactly; a partition pinned
        // to a named catalog key that this silo cannot resolve fails closed.
        var (resolvedProvider, placementVersion, providerKey) =
            await optionsResolver.GetWalProviderAsync(_treeId, _shardIndex).ConfigureAwait(true);
        _provider = resolvedProvider;
        _placementVersion = placementVersion;
        _providerKey = providerKey;
        // Reconcile any half-committed state a multi-phase backend
        // (e.g. Azure Table's per-batch partition + manifest layout)
        // may have left from a previous activation's crash between
        // commit phases, so GetHighestOffsetAsync's observable tail is
        // the actual durable tail before the grain accepts appends.
        // Single-transaction backends inherit the interface's default
        // no-op implementation.
        await _provider.ReconcileAsync(_treeId, _shardIndex, cancellationToken).ConfigureAwait(true);
        var highest = await _provider.GetHighestOffsetAsync(_treeId, _shardIndex, cancellationToken).ConfigureAwait(true);
        _nextOffset = highest + 1;
        // Construct the per-activation drain cancellation source up-front
        // so every FlushAsync (including the very first one) can link
        // its per-flush deadline to a stable token. A deactivation that
        // races a flush's deadline construction signals this already-
        // linked token rather than missing the link.
        _drainCts = new CancellationTokenSource();
        _initialized = true;
    }

    /// <summary>
    /// Drains every in-flight flush and any pending batch before
    /// returning, so a graceful deactivation never leaves callers
    /// observing a hung <see cref="TaskCompletionSource{TResult}"/>.
    /// <para>
    /// Bounded by <see cref="LatticeOptions.WalDrainBudget"/>: at
    /// drain entry the per-grain <see cref="_drainCts"/> is signalled
    /// so every in-flight provider call's linked deadline cancels
    /// immediately (co-operative providers give up promptly), the
    /// natural settle is awaited for up to the budget, and any slot
    /// that has not unlinked within the budget is force-faulted so
    /// the activation can finish tearing down. The
    /// <see cref="Orleans.Lattice.LatticeMetrics.WalShardDrainBudgetExpirations"/>
    /// counter and the
    /// <see cref="Orleans.Lattice.LatticeMetrics.WalShardDrainBudgetForceFaultedSlots"/>
    /// histogram attribute every budget-driven force-fault to the
    /// affected <c>(tree, shard)</c>.
    /// </para>
    /// </summary>
    public async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        // Deactivation-time diagnostic: record _inFlight.Count at
        // deactivation time, ONCE per deactivation, BEFORE we attempt
        // to drain. A non-zero observation means the activation is
        // being torn down with in-flight flushes still pending - the
        // slot population that defines the residual phase-1/activation
        // wedge fingerprint ("mid-call deactivation orphans").
        // Combined with a successor activation's
        // WalFlushPreflightTimeouts, a non-zero deactivate observation
        // immediately followed by a preflight timeout on the same
        // (tree, shard) is the smoking gun for the
        // mid-deactivation-orphan hypothesis.
        long inFlightAtDeactivate;
        lock (_stateGate)
        {
            inFlightAtDeactivate = _inFlight.Count;
        }
        LatticeMetrics.WalShardDeactivateInFlight.Record(inFlightAtDeactivate, _treeTag, _shardTag);
        if (inFlightAtDeactivate > 0)
        {
            Trace($"deactivate.inflight count={inFlightAtDeactivate} reason={reason}");
        }

        // Signal drain cancellation before awaiting the chain so
        // every in-flight flush's provider call observes the
        // cancellation immediately rather than waiting for its own
        // per-flush WalFlushTimeout deadline to fire. A co-operative
        // provider (any IWalStorageProvider implementation that observes
        // its CancellationToken) gives up promptly; an un-cooperative
        // provider's slot is force-faulted by the WalDrainBudget
        // ceiling below. The drain CTS is the per-activation source
        // constructed at OnActivateAsync / InitializeForTestingAsync
        // time so every FlushAsync has already linked its per-flush
        // deadline against it - signalling here cancels every linked
        // token in one shot regardless of when the flush started.
        // Not disposed here: a force-faulted FlushAsync may still be
        // alive (its body is unobserved but its deadline-construction
        // continuation might read the token), and CancellationTokenSource.Token
        // throws ObjectDisposedException after Dispose. The CTS is a
        // small heap object the GC reclaims with the activation.
        _drainCts?.Cancel();

        await DrainInFlightAsync(Options.WalDrainBudget).ConfigureAwait(true);

        bool hasPending;
        lock (_stateGate)
        {
            hasPending = _pendingSegments.Count > 0;
        }
        if (hasPending)
        {
            StartFlush();
            // A second bounded drain for the follow-on flush
            // started above. The budget is fresh because the
            // post-pending flush is a logically separate batch and
            // the SIGTERM-bound contract is "FINAL emits within
            // bounded time of the SIGTERM" - sharing the original
            // budget would silently shorten the second drain.
            await DrainInFlightAsync(Options.WalDrainBudget).ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Awaits every in-flight flush in chronological order, swallowing
    /// individual failures because they are already surfaced to their
    /// respective ack TCSs (and to <see cref="_stickyFailure"/>).
    /// Bounded by <paramref name="budget"/>; on expiry, force-faults
    /// every slot that has not yet unlinked so the activation can
    /// finish tearing down within bounded time of the SIGTERM.
    /// </summary>
    private async Task DrainInFlightAsync(TimeSpan budget)
    {
        // Snapshot the current chain so the force-fault path below has
        // a stable list to iterate over - new appends can race in only
        // on a different grain turn and the deactivation contract gives
        // them the same sticky-failure short-circuit a normal flush
        // failure does. The natural-drain loop reads the head pointer
        // per iteration so any slot that does settle naturally is still
        // unlinked by its own FlushAsync.finally and not by this method.
        var startTicks = Stopwatch.GetTimestamp();
        var deadlineTicks = budget == Timeout.InfiniteTimeSpan
            ? long.MaxValue
            : startTicks + (long)(budget.TotalSeconds * Stopwatch.Frequency);

        while (true)
        {
            Task? head;
            lock (_stateGate)
            {
                head = _inFlight.First?.Value.Task;
            }
            if (head is null)
            {
                return;
            }

            // Bound the per-head wait by the remaining budget. A budget
            // of InfiniteTimeSpan reproduces the historical unbounded
            // await; a finite budget gives the natural drain a chance
            // to settle but falls through to the force-fault path
            // below once the budget is exhausted.
            if (deadlineTicks == long.MaxValue)
            {
                try { await head.ConfigureAwait(true); } catch { /* failures surfaced via TCSs */ }
                continue;
            }

            var nowTicks = Stopwatch.GetTimestamp();
            if (nowTicks >= deadlineTicks)
            {
                ForceFaultRemainingSlotsForDrainBudgetExpiry(budget);
                return;
            }
            var remainingMs = (long)((deadlineTicks - nowTicks) * 1000.0 / Stopwatch.Frequency);
            try
            {
                await head.WaitAsync(TimeSpan.FromMilliseconds(remainingMs)).ConfigureAwait(true);
            }
            catch (TimeoutException)
            {
                // Drain budget expired before this slot's FlushAsync
                // settled. Force-fault every remaining slot so callers
                // parked on AppendAsync / AppendBatchAsync are released
                // (rather than parking through the rest of host
                // shutdown) and the activation can finish tearing down.
                ForceFaultRemainingSlotsForDrainBudgetExpiry(budget);
                return;
            }
            catch
            {
                // Per-slot fault already surfaced via TCSs / _stickyFailure.
            }
        }
    }

    /// <summary>
    /// Force-faults every in-flight slot that has not yet unlinked,
    /// invoked by <see cref="DrainInFlightAsync"/> when the
    /// <see cref="LatticeOptions.WalDrainBudget"/> expires. Each slot's
    /// ack TCSs receive a typed <see cref="TimeoutException"/> so
    /// callers parked on <c>AppendAsync</c> / <c>AppendBatchAsync</c>
    /// are released; the slot is removed from <c>_inFlight</c> so the
    /// natural drain loop terminates. The underlying provider tasks
    /// are abandoned (the task references are still held by the slot's
    /// FlushAsync continuation but no caller awaits them after this
    /// point); they are harmlessly collected when the activation is
    /// finalised. The matching counter /
    /// histogram attribute the trip per <c>(tree, shard)</c>.
    /// </summary>
    private void ForceFaultRemainingSlotsForDrainBudgetExpiry(TimeSpan budget)
    {
        // One shared exception instance for the drain-budget force-fault
        // path: latches as _stickyFailure (so any append racing in on a
        // future grain turn fails fast) AND faults every parked TCS so
        // callers are released. Mirrors HandleFlushFailureAsync's
        // single-exception pattern - separating "what the latch says"
        // from "what callers see" would cost an extra allocation per
        // drain expiration without any observable benefit; the
        // exception message names both roles.
        var ex = new TimeoutException(
            $"WAL deactivation drain for shard {_shardIndex} of tree '{_treeId}' "
            + $"exceeded the {budget} drain budget ({nameof(LatticeOptions.WalDrainBudget)}); "
            + "the in-flight chain was force-faulted so callers are released and the "
            + "activation can finish tearing down.");

        List<InFlightFlush> abandoned;
        List<TaskCompletionSource<long>> stalePending;
        lock (_stateGate)
        {
            // Snapshot and clear the chain inside the gate so a
            // concurrent FlushAsync.finally (running on a continuation
            // that happens to race the budget expiry) cannot
            // double-remove a node. We hold the in-flight list as the
            // authoritative reference to the slot data; the slot's own
            // task may still settle later but its ack TCSs are about to
            // be faulted unconditionally and its node is no longer in
            // _inFlight, so the FlushAsync.finally's _inFlight.Remove
            // call becomes a no-op on the already-removed node.
            abandoned = new List<InFlightFlush>(_inFlight.Count);
            foreach (var slot in _inFlight)
            {
                abandoned.Add(slot);
            }
            _inFlight.Clear();

            // The currently-accumulating pending batch's ack TCSs must
            // also be faulted: their offsets were assigned above a
            // window that is now logically orphaned, and no future
            // append will satisfy them. Recycle the rented lists so
            // the chain invariants are preserved if the activation is
            // somehow recovered (the standard pre-activation
            // initialise path does not re-enter the same activation,
            // but a fresh seed-list is cheaper than leaving the field
            // pointing at a half-consumed list).
            stalePending = _pendingAcks;
            for (var i = 0; i < _pendingSegments.Count; i++)
            {
                ReturnSegment(_pendingSegments[i]);
            }
            ReturnSegmentList(_pendingSegments);
            ReturnOffsetList(_pendingOffsets);
            _pendingSegments = RentSegmentList();
            _pendingOffsets = RentOffsetList();
            _pendingAcks = RentAckList();
            _pendingBatchSizeBytes = 0;

            // Latch a sticky failure so any append that races in on a
            // future grain turn fails fast rather than claiming offsets
            // we are about to fault.
            _stickyFailure ??= ex;
        }

        LatticeMetrics.WalShardDrainBudgetExpirations.Add(1, _treeTag, _shardTag);
        LatticeMetrics.WalShardDrainBudgetForceFaultedSlots.Record(abandoned.Count, _treeTag, _shardTag);
        Trace($"drain.budget.expired budget={budget} forceFaultedSlots={abandoned.Count}");

        foreach (var slot in abandoned)
        {
            var acks = slot.Acks;
            for (var i = 0; i < acks.Count; i++)
            {
                acks[i].TrySetException(ex);
            }
        }
        for (var i = 0; i < stalePending.Count; i++)
        {
            stalePending[i].TrySetException(ex);
        }
    }

    /// <inheritdoc />
    public async Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        ThrowIfMoveFenced();

        // Phase A horizontal-scaling diagnostic: stamp the wall-clock
        // entry timestamp so we can attribute caller-visible append
        // latency to (queue wait + cutover wait + provider duration).
        // The timestamp is cheap (Stopwatch.GetTimestamp) and recorded
        // unconditionally - the histogram is published on the shared
        // Lattice meter so it costs nothing when no listener subscribes.
        var appendStartTicks = Stopwatch.GetTimestamp();

        // If a previous flush in the chain failed, every subsequent
        // append fails fast with the same exception until the post-
        // failure resync re-aligns _nextOffset with the provider tail.
        // The resync runs inside DrainAndResyncAfterFailureAsync, which
        // is awaited by the failing flush before it returns; once it
        // completes _stickyFailure is cleared and new appends proceed.
        if (Volatile.Read(ref _stickyFailure) is { } sticky)
        {
            throw sticky;
        }

        // Encode the record once at append time. The exact encoded
        // byte count becomes the budget contribution (replacing the
        // historical heuristic) and the same bytes are handed to the
        // provider on flush via AppendEncodedBatchAsync - one encode per
        // append, no second pass inside the provider. The encoder
        // consumes the WalRecord directly: no producer-side
        // LatticeMutation round-trip on the append hot path.
        var writer = new PooledByteBufferWriter();
        try
        {
            encoder.Encode(in entry, writer);
        }
        catch
        {
            writer.Dispose();
            throw;
        }
        var segment = writer.DetachWrittenSegment();
        var size = segment.Count;
        var options = Options;
        var maxEntries = options.WalMaxBatchEntries;
        var maxBytes = options.WalMaxBatchBytes;
        var maxPending = options.WalMaxPendingBatches;

        // Cutover loop: flush the current pending batch when adding
        // `entry` would overflow the per-batch limits. The loop also
        // tolerates concurrent appends arriving across the await -
        // each iteration re-checks capacity against current pending
        // state.
        while (true)
        {
            bool needsCutover;
            bool atCap;
            Task? headTask = null;
            lock (_stateGate)
            {
                needsCutover = _pendingSegments.Count > 0
                    && (_pendingSegments.Count + 1 > maxEntries || _pendingBatchSizeBytes + size > maxBytes);
                if (!needsCutover)
                {
                    break;
                }
                atCap = _inFlight.Count >= maxPending;
                if (atCap)
                {
                    headTask = _inFlight.First!.Value.Task;
                }
            }
            if (atCap)
            {
                // At cap: apply back-pressure by awaiting the oldest
                // in-flight flush before starting another. This is the
                // only synchronisation point new appends see; under the
                // default cap of 1 it reproduces the original single-
                // in-flight protocol exactly.
                try { await headTask!.ConfigureAwait(true); } catch { /* surfaced via TCSs */ }
                if (Volatile.Read(ref _stickyFailure) is { } stickyMid)
                {
                    ReturnSegment(segment);
                    throw stickyMid;
                }
            }
            else
            {
                StartFlush();
            }
        }

        // Re-check sticky after any awaits in the cutover loop.
        if (Volatile.Read(ref _stickyFailure) is { } stickyPost)
        {
            ReturnSegment(segment);
            throw stickyPost;
        }

        TaskCompletionSource<long> tcs = null!;
        bool kickFlush = false;
        int queueDepth = 0;
        bool fenced = false;
        lock (_stateGate)
        {
            // Re-check the move fence under the gate: a quiesce may have raised
            // it during the cutover-loop await above. Refusing here guarantees
            // no offset is assigned (and so no append commits to the source
            // provider) once the fence is up.
            if (_moveFenced)
            {
                fenced = true;
            }
            else
            {
                var offset = _nextOffset++;
                _pendingSegments.Add(segment);
                _pendingOffsets.Add(offset);
                _pendingBatchSizeBytes += size;

                tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
                _pendingAcks.Add(tcs);

                // Decide whether to start a flush right now. Three triggers:
                //   1. _inFlight.Count < maxPending: there is spare capacity
                //      in the in-flight chain, so admitting this entry into
                //      its own flush keeps the latency floor flat while
                //      pipelining against any slots already in motion. The
                //      original protocol used `== 0` here, which made the
                //      pipelined cap unreachable under steady fan-in (every
                //      caller arriving while one flush was in motion parked
                //      on `acks[i].Task` and the chain depth never grew past
                //      one). With cap = 1 (the wire-compat default)
                //      `< maxPending` collapses back to `== 0`, so the
                //      single-in-flight protocol is unchanged.
                //   2. pending is full (reached WalMaxBatchEntries): kick a
                //      flush to fan out under multi-batch caps; otherwise the
                //      next entry's cutover would block on the head.
                //   3. pending is at the byte budget: same reasoning for the
                //      byte limit. Compared with the cutover loop's check,
                //      this is the "exact-fit" boundary - the next entry would
                //      definitely cut over.
                // Always honour the cap: never kick when at it. Once we
                // are under the cap the lone-entry latency floor (1) alone
                // is sufficient to admit this caller's flush; the pack
                // clauses (2) and (3) are subsumed by it. Keeping them out
                // of the predicate avoids re-evaluating the same boolean
                // twice on the hot path.
                kickFlush = _inFlight.Count < maxPending;
                queueDepth = _pendingSegments.Count;
            }
        }
        if (fenced)
        {
            ReturnSegment(segment);
            throw new LatticeWalQuiescingException(
                $"WAL shard {_treeId}/{_shardIndex} is quiesced for a placement move; retry shortly.");
        }
        LatticeMetrics.WalAppendQueueDepth.Record(queueDepth, _treeTag, _shardTag, _walPartitionsTag, _walMaxPendingBatchesTag);
        if (kickFlush)
        {
            StartFlush();
        }

        try
        {
            return await tcs.Task.ConfigureAwait(true);
        }
        finally
        {
            var elapsedMs = Stopwatch.GetElapsedTime(appendStartTicks).TotalMilliseconds;
            LatticeMetrics.WalAppendTurnWait.Record(elapsedMs, _treeTag, _shardTag, _walPartitionsTag, _walMaxPendingBatchesTag);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<long>> AppendBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        ThrowIfMoveFenced();

        if (entries.Count == 0)
        {
            return Array.Empty<long>();
        }

        // If a previous flush in the chain failed, every subsequent
        // append fails fast with the same exception until the post-
        // failure resync re-aligns _nextOffset with the provider tail.
        if (Volatile.Read(ref _stickyFailure) is { } sticky)
        {
            throw sticky;
        }

        // Encode every entry once at append time. The encoded bytes are
        // the same bytes the storage provider sees on flush via
        // AppendEncodedBatchAsync - one encode per entry, no second pass
        // inside the provider. We pre-allocate the segments and the
        // result-offsets array so the per-entry path inside the lock
        // is bound-checked but allocation-free. The encoder consumes
        // each WalRecord directly: no producer-side LatticeMutation
        // round-trip on the append hot path.
        var count = entries.Count;
        var segments = new ArraySegment<byte>[count];
        var sizes = new int[count];
        for (var i = 0; i < count; i++)
        {
            var record = entries[i];
            var writer = new PooledByteBufferWriter();
            try
            {
                encoder.Encode(in record, writer);
            }
            catch
            {
                writer.Dispose();
                // Return already-rented segments to the pool so an
                // encode failure mid-batch does not leak buffers.
                for (var j = 0; j < i; j++)
                {
                    ReturnSegment(segments[j]);
                }
                throw;
            }
            var segment = writer.DetachWrittenSegment();
            segments[i] = segment;
            sizes[i] = segment.Count;
        }

        var options = Options;
        var maxEntries = options.WalMaxBatchEntries;
        var maxBytes = options.WalMaxBatchBytes;
        var maxPending = options.WalMaxPendingBatches;

        // Enqueue every entry. For each one, follow the same cutover
        // protocol AppendAsync uses (flush the current pending batch
        // when adding the next entry would overflow the per-batch
        // limits, applying back-pressure when at the in-flight cap).
        // We hold no segments outside the loop; the segments[] array
        // and the parked TCSs together form the per-entry pending
        // state. The loop releases the gate around each await; offset
        // assignment is serialised under _stateGate so the assigned
        // offsets remain dense and ascending across the whole batch.
        var offsets = new long[count];
        var acks = new TaskCompletionSource<long>[count];
        for (var i = 0; i < count; i++)
        {
            var size = sizes[i];

            while (true)
            {
                bool needsCutover;
                bool atCap;
                Task? headTask = null;
                lock (_stateGate)
                {
                    needsCutover = _pendingSegments.Count > 0
                        && (_pendingSegments.Count + 1 > maxEntries || _pendingBatchSizeBytes + size > maxBytes);
                    if (!needsCutover)
                    {
                        break;
                    }
                    atCap = _inFlight.Count >= maxPending;
                    if (atCap)
                    {
                        headTask = _inFlight.First!.Value.Task;
                    }
                }
                if (atCap)
                {
                    try { await headTask!.ConfigureAwait(true); } catch { /* surfaced via TCSs */ }
                    if (Volatile.Read(ref _stickyFailure) is { } stickyMid)
                    {
                        // Failed mid-batch: return un-enqueued segments
                        // and surface the sticky failure. Segments at
                        // indexes < i are already owned by either the
                        // failed in-flight flush (returned by failure
                        // handling) or by the still-pending batch which
                        // will fault its own TCSs.
                        for (var j = i; j < count; j++)
                        {
                            ReturnSegment(segments[j]);
                        }
                        throw stickyMid;
                    }
                }
                else
                {
                    StartFlush();
                }
            }

            if (Volatile.Read(ref _stickyFailure) is { } stickyPost)
            {
                for (var j = i; j < count; j++)
                {
                    ReturnSegment(segments[j]);
                }
                throw stickyPost;
            }

            bool kickFlush = false;
            bool fenced = false;
            lock (_stateGate)
            {
                // Re-check the move fence under the gate (a quiesce may have
                // raised it during a cutover await earlier in this batch loop).
                if (_moveFenced)
                {
                    fenced = true;
                }
                else
                {
                    var offset = _nextOffset++;
                    offsets[i] = offset;
                    _pendingSegments.Add(segments[i]);
                    _pendingOffsets.Add(offset);
                    _pendingBatchSizeBytes += size;

                    var tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
                    acks[i] = tcs;
                    _pendingAcks.Add(tcs);

                    // Only kick a flush mid-batch when the per-batch caps
                    // are saturated; deferring the latency-floor trigger to
                    // the final entry of the batch is what gives the batched
                    // path its win - the whole batch shares a single flush
                    // window when it fits under the caps.
                    // The final-entry branch reproduces the AppendAsync
                    // protocol's latency-floor guarantee (a lone entry must
                    // not wait for a future cutover) once the entire batch
                    // has been enqueued, and now also opens a new flush when
                    // the chain has spare capacity below WalMaxPendingBatches
                    // so the cap is actually reachable under steady fan-in.
                    // With cap = 1 the outer `_inFlight.Count < maxPending`
                    // guard collapses to `_inFlight.Count == 0`, so the
                    // single-in-flight protocol is preserved bit-for-bit.
                    var isLast = i == count - 1;
                    kickFlush = _inFlight.Count < maxPending
                        && (_pendingSegments.Count >= maxEntries
                            || _pendingBatchSizeBytes >= maxBytes
                            || isLast);
                }
            }
            if (fenced)
            {
                // Refused mid-batch: return every not-yet-enqueued segment and
                // surface the transient quiesce signal. Entries already enqueued
                // at indexes < i are owned by the pending/in-flight batches and
                // settle (or fault) on their own TCSs.
                for (var j = i; j < count; j++)
                {
                    ReturnSegment(segments[j]);
                }
                throw new LatticeWalQuiescingException(
                    $"WAL shard {_treeId}/{_shardIndex} is quiesced for a placement move; retry shortly.");
            }
            if (kickFlush)
            {
                StartFlush();
            }
        }

        // Await every per-entry ack. The first one's completion is the
        // batch's commit point in the common case (every entry shares
        // one flush window); when the batch was cut over mid-stream
        // the later entries belong to later flushes and we observe
        // their independent completions here.
        for (var i = 0; i < count; i++)
        {
            await acks[i].Task.ConfigureAwait(true);
        }
        return offsets;
    }

    /// <inheritdoc />
    public async Task<WalShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (fromSequence < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fromSequence),
                fromSequence,
                "Sequence numbers start at 0; negative values are not valid.");
        }

        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per page.");
        }

        EnsureInitialized();

        var collected = new List<WalShardSequencedEntry>(Math.Min(maxEntries, 64));
        var fromOffsetExclusive = fromSequence - 1;
        await foreach (var walEntry in _provider
            .ReadAsync(_treeId, _shardIndex, fromOffsetExclusive, maxEntries, cancellationToken)
            .ConfigureAwait(true))
        {
            // Re-resolve the declared replication mode from the resolver:
            // the WAL deliberately does not carry the mode (it's
            // recoverable from a deterministic, side-effect-free source)
            // so the storage shape stays free of replication-only
            // metadata. A null resolver result means "tree no longer in
            // the replicated set"; ship as the canonical default so the
            // outbound batch is still typed.
            var mode = modeResolver.Resolve(walEntry.Mutation.TreeId) ?? LatticeMergeMode.LwwRegister;
            // The WAL is durability-only at the core layer; the origin
            // cluster id is stamped upstream on the mutation itself by
            // the replication observer (when the replication package is
            // registered). Single-cluster hosts have no cluster id, so
            // the converter receives an empty string and the resulting
            // record's OriginClusterId is empty too.
            // The mutation's own OriginClusterId wins when present (a
            // remote-replay path stamped it before reaching the WAL).
            // When it is null - i.e. a foreground commit on a host
            // where the replication observer has not yet stamped - the
            // resolver supplies the local cluster id. Single-cluster
            // hosts get string.Empty from the default resolver and the
            // resulting record's OriginClusterId is empty.
            var entry = WalRecordConverter.ToWalRecord(
                walEntry.Mutation,
                mode,
                originClusterId: clusterIdResolver.Resolve(walEntry.Mutation.TreeId));
            collected.Add(new WalShardSequencedEntry { Sequence = walEntry.Offset, Entry = entry });
            if (collected.Count >= maxEntries)
            {
                break;
            }
        }

        var nextSequence = collected.Count == 0 ? fromSequence : collected[^1].Sequence + 1;
        return new WalShardPage
        {
            Entries = collected,
            NextSequence = nextSequence,
        };
    }

    /// <inheritdoc />
    public async Task<WalShardShippingPage> ReadShippingAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (fromSequence < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fromSequence),
                fromSequence,
                "Sequence numbers start at 0; negative values are not valid.");
        }

        if (maxEntries < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxEntries),
                maxEntries,
                "At least one entry must be requested per page.");
        }

        EnsureInitialized();

        // Drain the bytes-shaped read seam so providers that natively
        // store encoded payloads (Azure Table Storage) hand the rows
        // through verbatim. Third-party providers fall back to the
        // default body on IWalStorageProvider, which decodes via
        // ReadAsync and re-encodes via the same encoder; the receiver
        // observes byte-for-byte identical payloads either way.
        var fromOffsetExclusive = fromSequence - 1;
        var page = await _provider
            .ReadEncodedAsync(_treeId, _shardIndex, fromOffsetExclusive, maxEntries, encoder, cancellationToken)
            .ConfigureAwait(true);

        var segments = page.EncodedEntries.Span;
        var offsets = page.Offsets.Span;
        var collected = new List<WalShardShippingEntry>(segments.Length);
        for (var i = 0; i < segments.Length; i++)
        {
            // The shipping page is dispatched across grain boundaries,
            // so the encoded payload must be a self-contained byte[]
            // (provider-owned segments are scoped to the provider call
            // only). The copy is unavoidable here; it is paid once per
            // entry per ship, not per peer.
            var seg = segments[i];
            var copy = seg.Count == 0 ? Array.Empty<byte>() : seg.AsSpan().ToArray();
            collected.Add(new WalShardShippingEntry { Sequence = offsets[i], EncodedPayload = copy });
        }

        var nextShippingSequence = collected.Count == 0 ? fromSequence : collected[^1].Sequence + 1;
        return new WalShardShippingPage
        {
            Entries = collected,
            NextSequence = nextShippingSequence,
        };
    }

    /// <inheritdoc />
    public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_nextOffset);
    }

    /// <inheritdoc />
    public async Task<long> GetLiveEntryCountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

        // The live entry count is `highest - lowest + 1` when the shard
        // holds at least one entry, otherwise zero. We read both
        // boundary offsets through the provider so the result reflects
        // the persisted footprint after any TrimAsync calls; `_nextOffset`
        // alone is trim-unaware. The two reads do not need to be a
        // consistent snapshot: a TrimAsync running concurrently can
        // only ever raise the lowest offset (never lower it), so a
        // brief race undercounts by at most the number of entries
        // trimmed between the two reads - a self-healing diagnostic
        // signal, not a correctness invariant.
        var highest = await _provider.GetHighestOffsetAsync(_treeId, _shardIndex, cancellationToken).ConfigureAwait(true);
        if (highest < 0)
        {
            return 0L;
        }
        var lowest = await _provider.GetLowestOffsetAsync(_treeId, _shardIndex, cancellationToken).ConfigureAwait(true);
        if (lowest < 0)
        {
            // Highest is set but lowest reports empty: every entry was
            // trimmed between the two reads. Live count is zero.
            return 0L;
        }
        var live = highest - lowest + 1L;
        return live < 0L ? 0L : live;
    }

    /// <inheritdoc />
    [Obsolete("Use GetLiveEntryCountAsync instead. GetEntryCountAsync is not trim-aware and will be removed in a future minor version.", DiagnosticId = "LATTICE0001")]
    public Task<long> GetEntryCountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_nextOffset);
    }

    /// <inheritdoc />
    public async Task<long> GetRetainedByteSizeAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return await _provider
            .GetRetainedByteSizeAsync(_treeId, _shardIndex, cancellationToken)
            .ConfigureAwait(true);
    }

    /// <summary>
    /// Captures the current pending batch as a new in-flight flush at
    /// the tail of <see cref="_inFlight"/> and resets the pending
    /// state for new arrivals. Offset assignment ran under the grain
    /// turn in <see cref="AppendAsync"/>, so the new window is strictly
    /// above every existing in-flight window by construction.
    /// </summary>
    private void StartFlush()
    {
        List<ArraySegment<byte>> segments;
        List<long> offsets;
        LinkedListNode<InFlightFlush> node;
        InFlightFlush slot;
        int batchEntries;
        long batchBytes;
        int inFlightBefore;
        int pendingSegmentsBefore;
        lock (_stateGate)
        {
            if (_pendingSegments.Count == 0)
            {
                return;
            }

            pendingSegmentsBefore = _pendingSegments.Count;
            segments = _pendingSegments;
            offsets = _pendingOffsets;
            var acks = _pendingAcks;
            batchBytes = _pendingBatchSizeBytes;
            batchEntries = segments.Count;
            inFlightBefore = _inFlight.Count;
            _pendingSegments = RentSegmentList();
            _pendingOffsets = RentOffsetList();
            _pendingAcks = RentAckList();
            _pendingBatchSizeBytes = 0;

            slot = new InFlightFlush
            {
                StartOffset = offsets[0],
                EndOffsetExclusive = offsets[^1] + 1,
                Segments = segments,
                Offsets = offsets,
                Acks = acks,
                // Stamp the initial stage under _stateGate so
                // the watchdog observes a slot in _inFlight that already
                // has Stage=Created and a sensible StageStartedTicks.
                Stage = WalFlushStage.Created,
                StageStartedTicks = Stopwatch.GetTimestamp(),
            };
            node = _inFlight.AddLast(slot);
        }
        // Phase A horizontal-scaling diagnostics: published once per
        // captured flush, outside the gate to keep the lock window
        // tight. Three observations make it possible to distinguish
        // grain-side queueing from provider-side latency:
        //   * batch_entries / batch_bytes - how full is each flush?
        //   * in_flight - how parallel is this shard against the
        //     provider, given WalMaxPendingBatches?
        // The matching provider duration is recorded inside FlushAsync.
        LatticeMetrics.WalAppendBatchEntries.Record(batchEntries, _treeTag, _shardTag, _walPartitionsTag, _walMaxPendingBatchesTag);
        LatticeMetrics.WalAppendBatchBytes.Record(batchBytes, _treeTag, _shardTag, _walPartitionsTag, _walMaxPendingBatchesTag);
        LatticeMetrics.WalAppendInFlight.Record(inFlightBefore, _treeTag, _shardTag, _walPartitionsTag, _walMaxPendingBatchesTag);
        // StartFlush counter + pending-segments histogram. The counter
        // is incremented per StartFlush call so a wedge cohort can
        // distinguish "new flushes are being kicked off but slots never
        // drain" (counter rises while inFlight stays pinned -> slot-leak
        // class) from "the cap-cutover loop has stopped kicking flushes"
        // (counter flat while inFlight stays pinned -> kick-stall class).
        // The histogram observation of _pendingSegments.Count at the
        // moment the flush is captured gives the cohort the matching
        // back-pressure-absorption signal.
        LatticeMetrics.WalShardStartFlushCalls.Add(1, _treeTag, _shardTag);
        LatticeMetrics.WalShardPendingSegments.Record(pendingSegmentsBefore, _treeTag, _shardTag);
        slot.Task = FlushAsync(node);
    }

    private async Task FlushAsync(LinkedListNode<InFlightFlush> node)
    {
        // Preflight deadline. The post-yield body and the
        // synchronous setup that precedes the provider-call deadline
        // (lines below: parallel-array materialisation, stopwatch start,
        // provider-call CTS construction) are normally microseconds, but
        // a paused activation scheduler can leave the Task.Yield()
        // continuation un-scheduled indefinitely, pinning this slot in
        // _inFlight with NO deadline armed yet (the existing
        // WalFlushTimeout only covers the provider call, which has not
        // been issued). Constructing the preflight CTS BEFORE the yield
        // arms its timer before the scheduler-pause window opens, so a
        // paused continuation is caught even if it never runs.
        //
        // The CTS is checked immediately after the yield resumes; if it
        // fired during the pause the flush surfaces as TimeoutException
        // routed through HandleFlushFailureAsync, the slot drains, and
        // WalFlushPreflightTimeouts attributes the trip to (tree, shard).
        // The CTS is disposed in the outer finally below regardless of
        // path, so the timer is always released.
        var preflightTimeout = Options.WalFlushPreflightTimeout;
        using var preflightDeadline = preflightTimeout == Timeout.InfiniteTimeSpan
            ? null
            : new CancellationTokenSource(preflightTimeout);

        // Yield once before any provider call so this task is observably
        // incomplete by the time StartFlush stores the Task on the slot
        // (a synchronously-completing provider would otherwise run the
        // body inline and the slot's Task field would still be the
        // default Task<VoidTaskResult> placeholder when we attempt to
        // remove the node in the finally - the contract is "every slot
        // in _inFlight carries a Task that completes when its provider
        // call settles").
        await Task.Yield();

        // If the preflight CTS fired while the yield's continuation was
        // un-scheduled, surface a TimeoutException through the normal
        // failure handler so the slot drains and the chain recovers
        // rather than wedging at inFlight=WalMaxPendingBatches with no
        // fault and no activation recycle. The throw falls through to
        // the outer catch/finally (HandleFlushFailureAsync + slot
        // removal + list recycling), preserving the existing failure
        // accounting and pool hygiene.
        TimeoutException? preflightFailure = null;
        if (preflightDeadline is not null && preflightDeadline.IsCancellationRequested)
        {
            LatticeMetrics.WalFlushPreflightTimeouts.Add(1, _treeTag, _shardTag);
            preflightFailure = new TimeoutException(
                $"WAL flush preflight for shard {_shardIndex} of tree '{_treeId}' "
                + $"offsets [{node.Value.StartOffset},{node.Value.EndOffsetExclusive}) "
                + $"exceeded the {preflightTimeout} preflight deadline "
                + $"({nameof(LatticeOptions.WalFlushPreflightTimeout)}); the grain "
                + $"scheduler did not resume the post-yield continuation in time, "
                + $"indicating a paused activation (startup reshard / membership change, "
                + $"non-cooperative scheduler work item, or mid-flush deactivation).");
            Trace($"flush.preflight.timeout [{node.Value.StartOffset},{node.Value.EndOffsetExclusive}) timeout={preflightTimeout}");
        }

        var slot = node.Value;
        var segments = slot.Segments;
        var offsets = slot.Offsets;
        // Post-yield lifecycle stamp. If the preflight CTS fired,
        // skip the stamp - the failure path overwrites with FailureHandled
        // below. Otherwise the slot has passed the preflight wedge
        // window and is about to materialise the provider call.
        if (preflightFailure is null)
        {
            slot.Stage = WalFlushStage.Yielded;
            slot.StageStartedTicks = Stopwatch.GetTimestamp();
        }
        Trace($"flush.enter [{slot.StartOffset},{slot.EndOffsetExclusive})");
        try
        {
            if (preflightFailure is not null)
            {
                throw preflightFailure;
            }
            // Hand the encoded segments straight to the provider's
            // zero-copy overload. Providers that natively store binary
            // payloads (Azure Table Storage) skip the second encode
            // entirely; providers that need the WalEntry shape (or
            // third-party ones that have not overridden the new
            // overload) round-trip via the encoder's Decode path on the
            // default interface implementation.
            // Materialise parallel arrays for the provider call.
            // ReadOnlyMemory<T> needs a backing array, so we copy the
            // pooled list contents into two fresh arrays per flush.
            // The two arrays themselves are tiny relative to the
            // payload bytes the segments carry, and rented payload
            // buffers are still pooled; this copy is one O(N) array
            // walk per flush, not per entry.
            var encodedArray = segments.ToArray();
            var offsetsArray = offsets.ToArray();
            var providerStartTicks = Stopwatch.GetTimestamp();
            try
            {
                // Bound the provider call with the configured flush
                // deadline. A provider call that hangs indefinitely (for
                // example against a partition left half-activated by a
                // placement/reshard race) would otherwise never settle,
                // so this slot would never be removed from _inFlight, the
                // chain would saturate at WalMaxPendingBatches, and every
                // subsequent append would back-pressure behind a flush
                // that can never complete - a steady-state stall with no
                // fault and no activation recycle. The deadline turns that
                // hang into a TimeoutException that the catch below routes
                // through the normal failure handler, which resynchronises
                // the tail and drains the chain.
                //
                // The bound is enforced *twice*, deliberately. The
                // deadline token is passed to the provider so a
                // co-operative provider stops its own work promptly. But
                // the grain ALSO bounds its own await with
                // Task.WaitAsync(token): a provider whose hang does not
                // observe the token - a non-cancellable SDK wait, an
                // internal retry loop that swallows cancellation, or a
                // genuinely wedged half-activated partition - would
                // otherwise leave the grain awaiting forever even though
                // the CTS has fired. WaitAsync abandons the un-cancellable
                // provider task (its slot is removed by the finally and
                // its eventual completion is harmlessly unobserved) so the
                // chain drains and recovery runs regardless of whether the
                // provider honours cancellation. This is the difference
                // between bounding the *call* and bounding the *wait*; only
                // the latter is wedge-proof.
                var flushTimeout = Options.WalFlushTimeout;
                // Link the per-flush deadline to the per-activation
                // drain cancellation source so a deactivation entering
                // at any point cancels the in-flight provider call
                // immediately rather than waiting for the natural
                // per-flush WalFlushTimeout deadline to fire. The drain
                // CTS is the same instance constructed at activation time
                // and signalled at OnDeactivateAsync entry, so the link
                // is captured here once and the deactivation cancels
                // every linked token in one shot regardless of when the
                // flush started.
                var drainSnapshot = _drainCts;
                using var deadline = (flushTimeout == Timeout.InfiniteTimeSpan, drainSnapshot) switch
                {
                    (true, null) => null,
                    (true, not null) => CancellationTokenSource.CreateLinkedTokenSource(drainSnapshot.Token),
                    (false, null) => new CancellationTokenSource(flushTimeout),
                    (false, not null) => LinkWithFlushTimeout(drainSnapshot.Token, flushTimeout),
                };
                try
                {
                    var providerCall = _provider.AppendEncodedBatchAsync(
                        _treeId,
                        _shardIndex,
                        encodedArray.AsMemory(),
                        offsetsArray.AsMemory(),
                        encoder,
                        deadline?.Token ?? CancellationToken.None);
                    // Provider Task is in hand and the matching
                    // WalFlushTimeout deadline is armed; the wedge
                    // candidate "stuck inside the provider call" is now
                    // observable as Stage=ProviderCallIssued.
                    slot.Stage = WalFlushStage.ProviderCallIssued;
                    slot.StageStartedTicks = Stopwatch.GetTimestamp();
                    if (deadline is null)
                    {
                        await providerCall.ConfigureAwait(true);
                    }
                    else
                    {
                        await providerCall.WaitAsync(deadline.Token).ConfigureAwait(true);
                    }
                    // Provider await resumed (success path). The wedge
                    // candidate "stuck after the provider returned but
                    // before slot removal" is observable as
                    // Stage=ProviderCallReturned.
                    slot.Stage = WalFlushStage.ProviderCallReturned;
                    slot.StageStartedTicks = Stopwatch.GetTimestamp();
                }
                catch (OperationCanceledException oce)
                    when (deadline is not null && deadline.IsCancellationRequested)
                {
                    // Distinguish the two cancellation sources so the
                    // surfaced TimeoutException attributes the trip to
                    // the actually-firing deadline rather than blaming
                    // WalFlushTimeout when the drain CTS was the
                    // cause. drainSnapshot is captured up-front (same
                    // value the deadline was linked to); a null
                    // drainSnapshot means there's no linked drain and
                    // only the flush-timer could have fired.
                    if (drainSnapshot is not null && drainSnapshot.IsCancellationRequested)
                    {
                        throw new TimeoutException(
                            $"WAL flush for shard {_shardIndex} of tree '{_treeId}' "
                            + $"offsets [{slot.StartOffset},{slot.EndOffsetExclusive}) "
                            + $"was cancelled by the per-activation deactivation drain "
                            + $"({nameof(LatticeOptions.WalDrainBudget)}); the in-flight provider "
                            + $"call observed cancellation at drain entry.", oce);
                    }
                    throw new TimeoutException(
                        $"WAL flush for shard {_shardIndex} of tree '{_treeId}' "
                        + $"offsets [{slot.StartOffset},{slot.EndOffsetExclusive}) "
                        + $"exceeded the {flushTimeout} flush deadline "
                        + $"({nameof(LatticeOptions.WalFlushTimeout)}).", oce);
                }
            }
            finally
            {
                // Record provider duration on both success and fault
                // so the histogram covers the throttled / faulted tail
                // as well as the happy path. The HandleFlushFailureAsync
                // catch below subsumes the fault accounting downstream.
                var providerMs = Stopwatch.GetElapsedTime(providerStartTicks).TotalMilliseconds;
                LatticeMetrics.WalAppendProviderDuration.Record(providerMs, _treeTag, _shardTag, _walPartitionsTag, _walMaxPendingBatchesTag);
            }

            // If a predecessor already failed and faulted us, our acks
            // were faulted in HandleFailureAsync; do not try to satisfy
            // them with a result. Our provider call may have committed
            // against an orphaned offset window - that is the price of
            // multi-batch concurrency and is reconciled by the post-
            // failure resync against GetHighestOffsetAsync.
            if (Volatile.Read(ref _stickyFailure) is null)
            {
                Trace($"flush.ok    [{slot.StartOffset},{slot.EndOffsetExclusive}) -> set {slot.Acks.Count} TCS results");
                for (var i = 0; i < slot.Acks.Count; i++)
                {
                    slot.Acks[i].TrySetResult(offsets[i]);
                }
                // Success path's terminal lifecycle stamp. The slot is
                // about to be removed by the outer finally; a slot
                // observed at AcksApplied has fully discharged its
                // contract and any subsequent wedge is in the outer
                // remove-from-_inFlight finally region.
                slot.Stage = WalFlushStage.AcksApplied;
                slot.StageStartedTicks = Stopwatch.GetTimestamp();
            }
            else
            {
                Trace($"flush.ok    [{slot.StartOffset},{slot.EndOffsetExclusive}) but sticky set - SKIP TCS results");
            }
        }
        catch (Exception ex)
        {
            Trace($"flush.fail  [{slot.StartOffset},{slot.EndOffsetExclusive}) ex={ex.GetType().Name}: {ex.Message}");
            await HandleFlushFailureAsync(node, ex).ConfigureAwait(true);
            // Failure path's terminal lifecycle stamp. A slot observed
            // at FailureHandled has had its sticky-failure published and
            // is about to be removed by the outer finally.
            slot.Stage = WalFlushStage.FailureHandled;
            slot.StageStartedTicks = Stopwatch.GetTimestamp();
        }
        finally
        {
            // Always remove our slot from the chain when our provider
            // call settles, even if a predecessor's failure handler
            // already faulted our acks. The chain is the durable record
            // of "what is still in motion against the provider".
            //
            // Recycle the per-flush lists into the per-grain pool only
            // when the failure latch is clear - in the failure path,
            // HandleFlushFailureAsync (the failed slot's own handler or
            // a predecessor's, depending on which threw) still
            // references slot.Acks via failedAcks / laterSlots until it
            // finishes faulting TCSs, and clearing the list would lose
            // the TCSs before faulting. In the steady state with no
            // failure the success path is exclusive owner of both
            // lists and recycling eliminates the per-flush allocations.
            lock (_stateGate)
            {
                if (node.List is not null)
                {
                    _inFlight.Remove(node);
                }
                if (_stickyFailure is null)
                {
                    // Return rented byte arrays to the ArrayPool before
                    // clearing the segment list so the pool sees them
                    // once and only once per batch.
                    for (var i = 0; i < segments.Count; i++)
                    {
                        ReturnSegment(segments[i]);
                    }
                    ReturnSegmentList(segments);
                    ReturnOffsetList(offsets);
                    ReturnAckList(slot.Acks);
                }
            }
        }

        // Drain a follow-on batch that accumulated while we were in
        // flight. Done outside the try/finally so a fresh flush failure
        // is observed cleanly by its own callers. Snapshot the relevant
        // state under the gate to make the trigger decision; kick a
        // flush whenever the chain has spare capacity below
        // WalMaxPendingBatches and a pending batch is waiting. With
        // cap = 1 `< maxPending` is identical to the previous
        // "chain fully drained" predicate (the slot that just settled
        // is already removed above, so _inFlight.Count == 0 holds at
        // this point under cap = 1).
        bool kickFollowOn;
        lock (_stateGate)
        {
            kickFollowOn = _stickyFailure is null
                && _pendingSegments.Count > 0
                && _inFlight.Count < Options.WalMaxPendingBatches;
        }
        if (kickFollowOn)
        {
            StartFlush();
        }
    }

    /// <summary>
    /// Handles a failure observed by the flush owning <paramref name="failedNode"/>.
    /// Latches the sticky failure, captures every TCS that needs to be
    /// faulted (failed window + later windows + pending), drains every
    /// later in-flight slot, re-synchronises <see cref="_nextOffset"/>
    /// from the provider's authoritative tail, clears the sticky latch,
    /// and only *then* faults the captured TCSs. The fault-after-resync
    /// order means a caller that catches the surfaced exception and
    /// immediately retries observes a fully-resynced grain rather than
    /// a still-latched one, which preserves the sequential-test pattern
    /// "throw, switch backend, retry succeeds at the rolled-back offset"
    /// without the test having to spin on the sticky latch.
    /// </summary>
    private async Task HandleFlushFailureAsync(LinkedListNode<InFlightFlush> failedNode, Exception ex)
    {
        List<TaskCompletionSource<long>> failedAcks;
        List<InFlightFlush> laterSlots;
        List<TaskCompletionSource<long>> stalePending;
        lock (_stateGate)
        {
            // Latch sticky immediately so any concurrent append running on
            // a subsequent grain turn fails fast and does not claim offsets
            // we are about to roll back.
            _stickyFailure ??= ex;

            // Capture the TCS lists to fault later. We deliberately defer
            // faulting until after the resync (see the method summary).
            failedAcks = failedNode.Value.Acks;
            laterSlots = new List<InFlightFlush>();
            for (var n = failedNode.Next; n is not null; n = n.Next)
            {
                laterSlots.Add(n.Value);
            }
            Trace($"failure.handle failed=[{failedNode.Value.StartOffset},{failedNode.Value.EndOffsetExclusive}) laterSlots={laterSlots.Count} pendingAcks={_pendingAcks.Count}");

            // Reset pending state immediately so any append that races in
            // on a future grain turn (and would otherwise be admitted by
            // the cap loop) sees an empty pending and short-circuits on
            // _stickyFailure instead. We hold the stale TCSs locally for
            // post-resync faulting. The pre-existing pending segments
            // are dropped (their backing buffers are returned to the
            // pool below) because the failure handler is the only writer
            // that mutates _pendingSegments after the latch was taken.
            stalePending = _pendingAcks;
            for (var i = 0; i < _pendingSegments.Count; i++)
            {
                ReturnSegment(_pendingSegments[i]);
            }
            ReturnSegmentList(_pendingSegments);
            ReturnOffsetList(_pendingOffsets);
            _pendingSegments = RentSegmentList();
            _pendingOffsets = RentOffsetList();
            _pendingAcks = RentAckList();
            _pendingBatchSizeBytes = 0;
        }

        // Wait for every other in-flight slot to settle (their tasks
        // may still be observing their own provider calls) before we
        // resync, so GetHighestOffsetAsync sees a stable tail.
        foreach (var slot in laterSlots)
        {
            try { await slot.Task.ConfigureAwait(true); } catch { /* surfaced via TCSs below */ }
        }

        // Resync _nextOffset from the provider. Concurrent flushes
        // before this one may have already committed against later
        // offset windows, so the dense-offset invariant is restored
        // against the provider's real tail rather than the failed
        // window's start.
        try
        {
            // Bound the resync with the same flush deadline. If the
            // original flush hung against a wedged partition, the tail
            // read can hang the same way; without a ceiling the failure
            // handler itself would never complete, the later in-flight
            // slots it is draining would stay parked, and the recovery
            // would wedge in place of the flush it was meant to rescue.
            var resyncTimeout = Options.WalFlushTimeout;
            using var deadline = resyncTimeout == Timeout.InfiniteTimeSpan
                ? null
                : new CancellationTokenSource(resyncTimeout);
            var highest = await _provider.GetHighestOffsetAsync(
                _treeId,
                _shardIndex,
                deadline?.Token ?? CancellationToken.None).ConfigureAwait(true);
            lock (_stateGate)
            {
                _nextOffset = highest + 1;
                _stickyFailure = null;
            }
        }
        catch
        {
            // If even the resync fails we keep _stickyFailure latched
            // so callers continue to see the original fault; the next
            // successful activation will resync from scratch.
        }

        // Fault every captured TCS now that the grain is in a
        // consistent post-failure state. Callers that catch the
        // surfaced exception and retry immediately will observe a
        // fully-resynced _nextOffset.
        Trace($"failure.fault  failed.acks={failedAcks.Count} later.slots={laterSlots.Count} stale.pending={stalePending.Count}");
        for (var i = 0; i < failedAcks.Count; i++)
        {
            failedAcks[i].TrySetException(ex);
        }
        foreach (var slot in laterSlots)
        {
            var acks = slot.Acks;
            for (var i = 0; i < acks.Count; i++)
            {
                acks[i].TrySetException(ex);
            }
        }
        for (var i = 0; i < stalePending.Count; i++)
        {
            stalePending[i].TrySetException(ex);
        }
    }

    /// <summary>
    /// One slot in the in-flight flush chain. Carries the offset
    /// window it owns, the encoded payload segments and parallel
    /// offsets handed to the provider, the ack TCSs parked against
    /// the slot, and the task that completes when the provider's
    /// <c>AppendEncodedBatchAsync</c> for this slot has settled.
    /// </summary>
    private sealed class InFlightFlush
    {
        public long StartOffset { get; init; }
        public long EndOffsetExclusive { get; init; }
        public required List<ArraySegment<byte>> Segments { get; init; }
        public required List<long> Offsets { get; init; }
        public required List<TaskCompletionSource<long>> Acks { get; init; }
        public Task Task { get; set; } = System.Threading.Tasks.Task.CompletedTask;

        /// <summary>
        /// Lifecycle stamp. Updated at every milestone in
        /// <see cref="WalShardGrain.FlushAsync"/> so a wedged head slot
        /// can be attributed to the exact code region it is stuck at via
        /// an out-of-process ClrMD walk (e.g. the azure-throughput
        /// silo's <c>StallWatchdog</c>). The default value is
        /// <see cref="WalFlushStage.Created"/>, set when the slot is
        /// constructed in <c>StartFlush</c> before the flush task is
        /// kicked off; subsequent stages overwrite it as the flush
        /// progresses. Read by the watchdog without grabbing
        /// <see cref="_stateGate"/> - a torn read is acceptable because
        /// any value the field carries names a real point in the state
        /// machine the slot has visited, and the watchdog is
        /// observation-only.
        /// </summary>
        public WalFlushStage Stage;

        /// <summary>
        /// Lifecycle stamp companion. <see cref="Stopwatch.GetTimestamp"/>
        /// captured each time <see cref="Stage"/> is updated, so the
        /// watchdog can report how long the slot has been stuck at its
        /// current stage. Two writes per milestone, no allocations.
        /// </summary>
        public long StageStartedTicks;
    }

    /// <summary>
    /// Lifecycle stage for an <see cref="InFlightFlush"/> slot.
    /// Updated at every milestone in <see cref="FlushAsync"/> so an
    /// out-of-process ClrMD walk can attribute a wedged head slot to
    /// the exact code region it is stuck at.
    /// <list type="bullet">
    ///   <item><description><see cref="Created"/> - slot constructed in
    ///   <see cref="StartFlush"/>, added to <c>_inFlight</c>, and
    ///   <c>FlushAsync</c> kicked off but its body has not yet been
    ///   reached.</description></item>
    ///   <item><description><see cref="Yielded"/> - <c>FlushAsync</c>
    ///   resumed after <c>await Task.Yield()</c> and passed the
    ///   preflight-deadline check. Synchronous setup (array materialisation,
    ///   provider-call CTS construction) follows.</description></item>
    ///   <item><description><see cref="ProviderCallIssued"/> - the
    ///   provider's <c>AppendEncodedBatchAsync</c> Task has been
    ///   returned and the <c>await</c> is about to start. The matching
    ///   <c>WalFlushTimeout</c> deadline is armed.</description></item>
    ///   <item><description><see cref="ProviderCallReturned"/> - the
    ///   provider call's await resumed (either successfully or via the
    ///   flush-timeout catch). Ack-application or failure-handling
    ///   follows.</description></item>
    ///   <item><description><see cref="AcksApplied"/> - on the success
    ///   path, every per-entry ack TCS has been satisfied with its
    ///   offset.</description></item>
    ///   <item><description><see cref="FailureHandled"/> - on the
    ///   failure path, <c>HandleFlushFailureAsync</c> has run and the
    ///   sticky failure has been published to every parked TCS.</description></item>
    /// </list>
    /// </summary>
    internal enum WalFlushStage : byte
    {
        Created = 0,
        Yielded = 1,
        ProviderCallIssued = 2,
        ProviderCallReturned = 3,
        AcksApplied = 4,
        FailureHandled = 5,
    }

    /// <summary>
    /// Rents a cleared encoded-segment list from the per-grain pool,
    /// or allocates a fresh one if the pool is empty. Must be called
    /// under <see cref="_stateGate"/>.
    /// </summary>
    private List<ArraySegment<byte>> RentSegmentList()
    {
        if (_segmentListPool.Count > 0)
        {
            return _segmentListPool.Pop();
        }
        return new List<ArraySegment<byte>>();
    }

    /// <summary>
    /// Rents a cleared offset list from the per-grain pool, or
    /// allocates a fresh one if the pool is empty. Must be called
    /// under <see cref="_stateGate"/>.
    /// </summary>
    private List<long> RentOffsetList()
    {
        if (_offsetListPool.Count > 0)
        {
            return _offsetListPool.Pop();
        }
        return new List<long>();
    }

    /// <summary>
    /// Rents a cleared ack-TCS list from the per-grain pool, or
    /// allocates a fresh one if the pool is empty. Must be called
    /// under <see cref="_stateGate"/>.
    /// </summary>
    private List<TaskCompletionSource<long>> RentAckList()
    {
        if (_ackListPool.Count > 0)
        {
            return _ackListPool.Pop();
        }
        return new List<TaskCompletionSource<long>>();
    }

    /// <summary>
    /// Returns a no-longer-needed segment list to the pool. The caller
    /// must guarantee no other reference to the list survives. Lists
    /// that exceed the pool depth are dropped to the GC; lists whose
    /// backing array has grown unusually large are also dropped to keep
    /// the pool's resident footprint bounded. Must be called under
    /// <see cref="_stateGate"/>.
    /// </summary>
    private void ReturnSegmentList(List<ArraySegment<byte>> list)
    {
        if (_segmentListPool.Count >= MaxPoolDepth || list.Capacity > 4096)
        {
            return;
        }
        list.Clear();
        _segmentListPool.Push(list);
    }

    /// <summary>
    /// Returns a no-longer-needed offset list to the pool. Same
    /// contract as <see cref="ReturnSegmentList"/>.
    /// </summary>
    private void ReturnOffsetList(List<long> list)
    {
        if (_offsetListPool.Count >= MaxPoolDepth || list.Capacity > 4096)
        {
            return;
        }
        list.Clear();
        _offsetListPool.Push(list);
    }

    /// <summary>
    /// Returns a rented byte buffer underlying an encoded segment to
    /// the shared <see cref="ArrayPool{T}"/>. The grain hands the
    /// segment to the provider on flush; once the provider's call
    /// settles (success or failure), the buffer is no longer needed
    /// because the storage layer has either captured the bytes
    /// verbatim into its own row or persisted them in its own buffer.
    /// </summary>
    private static void ReturnSegment(ArraySegment<byte> segment)
    {
        if (segment.Array is { Length: > 0 } array)
        {
            ArrayPool<byte>.Shared.Return(array);
        }
    }

    /// <summary>
    /// Returns a no-longer-needed ack list to the pool. Same contract
    /// as <see cref="ReturnSegmentList"/>.
    /// </summary>
    private void ReturnAckList(List<TaskCompletionSource<long>> list)
    {
        if (_ackListPool.Count >= MaxPoolDepth || list.Capacity > 4096)
        {
            return;
        }
        list.Clear();
        _ackListPool.Push(list);
    }

    /// <summary>
    /// Constructs a linked <see cref="CancellationTokenSource"/> that
    /// cancels when either the drain token <paramref name="drainToken"/>
    /// fires or the <paramref name="flushTimeout"/> deadline elapses.
    /// Extracted into a named helper so the
    /// <see cref="FlushAsync"/> deadline-construction switch stays a
    /// pure expression and the (timer-armed, drain-linked) shape lands
    /// in one place. <c>CancellationTokenSource.CancelAfter</c> arms
    /// the timer after construction; the linked source is returned
    /// already linked, then <c>CancelAfter</c> arms the flush deadline
    /// on top so either cause cancels the same token.
    /// </summary>
    private static CancellationTokenSource LinkWithFlushTimeout(
        CancellationToken drainToken,
        TimeSpan flushTimeout)
    {
        var linked = CancellationTokenSource.CreateLinkedTokenSource(drainToken);
        linked.CancelAfter(flushTimeout);
        return linked;
    }

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException(
                $"{nameof(WalShardGrain)} has not been initialized. The grain is normally activated by Orleans, "
                + $"which calls {nameof(OnActivateAsync)}; unit tests may bypass that by calling {nameof(InitializeForTestingAsync)}.");
        }
    }

    /// <summary>
    /// Throws <see cref="LatticeWalQuiescingException"/> when this activation is
    /// fenced for an in-progress placement move. If the fence lease has expired
    /// (the move coordinator failed mid-move), self-heals by requesting
    /// deactivation so the next activation re-resolves placement from the
    /// durable pin, then throws so the caller retries against the fresh
    /// activation.
    /// </summary>
    private void ThrowIfMoveFenced()
    {
        bool fenced;
        long deadlineTicks;
        lock (_stateGate)
        {
            fenced = _moveFenced;
            deadlineTicks = _fenceDeadlineTicks;
        }
        if (!fenced)
        {
            return;
        }

        if (Stopwatch.GetTimestamp() < deadlineTicks)
        {
            throw new LatticeWalQuiescingException(
                $"WAL shard {_treeId}/{_shardIndex} is quiesced for a placement move; retry shortly.");
        }

        // Lease expired without a cutover: keep the fence raised and request
        // deactivation. We deliberately do NOT clear _moveFenced here - because
        // the append methods are [AlwaysInterleave], clearing the flag before
        // the activation is actually gone would open a window in which an
        // interleaved append slips through and writes to the stale cached
        // provider (catastrophic if the move has, by then, flipped the pin).
        // The fence therefore holds until this activation dies; the next
        // activation re-resolves placement from the durable pin (resuming on the
        // old provider if the move aborted, or routing to the new provider if
        // the pin was already flipped) and comes up unfenced.
        Trace($"move.quiesce.lease_expired tree={_treeId} shard={_shardIndex}");
        context.Deactivate(new DeactivationReason(
            DeactivationReasonCode.ApplicationRequested,
            "WAL move quiesce lease expired"));
        throw new LatticeWalQuiescingException(
            $"WAL shard {_treeId}/{_shardIndex} quiesce lease expired; re-resolving placement.");
    }

    /// <inheritdoc />
    public async Task<WalMoveQuiesceResult> QuiesceForMoveAsync(
        long expectedPlacementVersion, TimeSpan lease, CancellationToken cancellationToken)
    {
        EnsureInitialized();

        // Abort (without fencing) when the coordinator's expected placement
        // version does not match the version this activation resolved against:
        // the activation has already re-resolved placement, so quiescing it
        // would fence the wrong provider.
        if (_placementVersion != expectedPlacementVersion)
        {
            return new WalMoveQuiesceResult(
                Quiesced: false,
                HighestOffsetInclusive: -1,
                ObservedPlacementVersion: _placementVersion,
                ProviderKey: _providerKey);
        }

        // Raise the fence so new offset assignments are refused, then flush the
        // pending batch and drain every in-flight flush so the source tail is
        // stable before the coordinator copies it.
        var leaseTicks = lease <= TimeSpan.Zero
            ? long.MaxValue
            : Stopwatch.GetTimestamp() + (long)(lease.TotalSeconds * Stopwatch.Frequency);
        bool hasPending;
        lock (_stateGate)
        {
            _moveFenced = true;
            _fenceDeadlineTicks = leaseTicks;
            hasPending = _pendingSegments.Count > 0;
        }
        if (hasPending)
        {
            StartFlush();
        }
        await DrainInFlightAsync(Options.WalDrainBudget).ConfigureAwait(true);

        var highest = await _provider
            .GetHighestOffsetAsync(_treeId, _shardIndex, cancellationToken)
            .ConfigureAwait(true);
        return new WalMoveQuiesceResult(
            Quiesced: true,
            HighestOffsetInclusive: highest,
            ObservedPlacementVersion: _placementVersion,
            ProviderKey: _providerKey);
    }

    /// <inheritdoc />
    public Task DeactivateForMoveAsync(CancellationToken cancellationToken)
    {
        context.Deactivate(new DeactivationReason(
            DeactivationReasonCode.ApplicationRequested,
            "WAL placement move cutover"));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Test seam that bypasses Orleans activation: configures the grain
    /// for direct instantiation in unit tests without standing up a
    /// silo. Tests pre-load any persisted state into the supplied
    /// <paramref name="provider"/> before calling this method.
    /// <para>
    /// Per-tree <see cref="LatticeOptions"/> overrides are supplied
    /// through the injected <see cref="IOptionsMonitor{TOptions}"/> in
    /// the constructor (configure the substitute monitor to return the
    /// desired <see cref="LatticeOptions"/> for the tree id before
    /// calling this seam) so the grain reads exactly the same surface
    /// it does in production.
    /// </para>
    /// </summary>
    internal async Task InitializeForTestingAsync(
        string treeId,
        int shardIndex,
        IWalStorageProvider provider,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(provider);

        _treeId = treeId;
        _shardIndex = shardIndex;
        _provider = provider;
        // Mirror OnActivateAsync's metric-tag initialisation so the
        // test seam observes the same (tree, shard) tag attribution
        // on every metric record (including the deactivation hook
        // and the preflight-timeout counter) that the production
        // activation contract provides.
        _treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, _treeId);
        _shardTag = new KeyValuePair<string, object?>(LatticeMetrics.TagShard, _shardIndex);
        // Mirror OnActivateAsync's ordering: reconcile any
        // half-committed multi-phase backend state before reading the
        // tail so the test seam exercises the same activation contract
        // production grains use.
        await provider.ReconcileAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(true);
        var highest = await provider.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(true);
        _nextOffset = highest + 1;
        // Mirror OnActivateAsync's drain-CTS construction so unit tests
        // see the same activation contract production grains use; the
        // drain-budget tests rely on the CTS being available so every
        // FlushAsync can link its per-flush deadline.
        _drainCts = new CancellationTokenSource();
        _initialized = true;
    }
}
