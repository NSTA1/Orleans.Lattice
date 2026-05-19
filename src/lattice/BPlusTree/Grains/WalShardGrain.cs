using Microsoft.Extensions.DependencyInjection;
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
/// (<see cref="LatticeOptions.DefaultWalMaxPendingBatches"/> = 1) is
/// bit-identical to the single-in-flight protocol.
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
    IServiceProvider services,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILatticeMergeModeResolver modeResolver,
    ILatticeOriginClusterIdResolver clusterIdResolver) : IWalShardGrain, IGrainBase
{
    /// <summary>Per-entry serialised-size estimate overhead in bytes (envelope + HLC + origin id + slot tags).</summary>
    private const int EntrySizeOverhead = 128;

    private string _treeId = "";
    private int _shardIndex;
    private IWalStorageProvider _provider = null!;
    private long _nextOffset;
    private bool _initialized;

    private List<WalEntry> _pendingBatch = new();
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
    /// Per-grain free-list of recycled <see cref="WalEntry"/> batch
    /// buffers. Eliminates the per-flush
    /// <c>new List&lt;WalEntry&gt;()</c> allocation in the steady-state
    /// hot path. Accessed only under <see cref="_stateGate"/>; the
    /// pre-existing gate makes a separate pool lock unnecessary. Depth
    /// is capped at <see cref="MaxPoolDepth"/> so a transient burst of
    /// concurrent flushes does not pin large buffers indefinitely.
    /// </summary>
    private readonly Stack<List<WalEntry>> _batchListPool = new();

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

        var options = optionsMonitor.Get(_treeId);
        _provider = options.WalStorageProvider?.Invoke(_treeId)
            ?? services.GetRequiredService<IWalStorageProvider>();
        var highest = await _provider.GetHighestOffsetAsync(_treeId, _shardIndex, cancellationToken).ConfigureAwait(true);
        _nextOffset = highest + 1;
        _initialized = true;
    }

    /// <summary>
    /// Drains every in-flight flush and any pending batch before
    /// returning, so a graceful deactivation never leaves callers
    /// observing a hung <see cref="TaskCompletionSource{TResult}"/>.
    /// </summary>
    public async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        await DrainInFlightAsync().ConfigureAwait(true);

        bool hasPending;
        lock (_stateGate)
        {
            hasPending = _pendingBatch.Count > 0;
        }
        if (hasPending)
        {
            StartFlush();
            await DrainInFlightAsync().ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Awaits every in-flight flush in chronological order, swallowing
    /// individual failures because they are already surfaced to their
    /// respective ack TCSs (and to <see cref="_stickyFailure"/>).
    /// </summary>
    private async Task DrainInFlightAsync()
    {
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
            try
            {
                await head.ConfigureAwait(true);
            }
            catch
            {
                // Failures already surfaced to TCSs and _stickyFailure.
            }
        }
    }

    /// <inheritdoc />
    public async Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();

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

        var size = EstimateSize(entry);
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
                needsCutover = _pendingBatch.Count > 0
                    && (_pendingBatch.Count + 1 > maxEntries || _pendingBatchSizeBytes + size > maxBytes);
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
            throw stickyPost;
        }

        TaskCompletionSource<long> tcs;
        bool kickFlush;
        lock (_stateGate)
        {
            var offset = _nextOffset++;
            _pendingBatch.Add(new WalEntry { Offset = offset, Mutation = WalRecordConverter.FromWalRecord(entry) });
            _pendingBatchSizeBytes += size;

            tcs = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingAcks.Add(tcs);

            // Decide whether to start a flush right now. Three triggers:
            //   1. _inFlight.Count == 0: a lone entry must not wait for a
            //      future cutover to make progress (latency floor).
            //   2. pending is full (reached WalMaxBatchEntries): kick a
            //      flush to fan out under multi-batch caps; otherwise the
            //      next entry's cutover would block on the head.
            //   3. pending is at the byte budget: same reasoning for the
            //      byte limit. Compared with the cutover loop's check,
            //      this is the "exact-fit" boundary - the next entry would
            //      definitely cut over.
            // Always honour the cap: never kick when at it.
            kickFlush = _inFlight.Count < maxPending
                && (_inFlight.Count == 0
                    || _pendingBatch.Count >= maxEntries
                    || _pendingBatchSizeBytes >= maxBytes);
        }
        if (kickFlush)
        {
            StartFlush();
        }

        return await tcs.Task.ConfigureAwait(true);
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
    public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_nextOffset);
    }

    /// <inheritdoc />
    public Task<long> GetEntryCountAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureInitialized();
        return Task.FromResult(_nextOffset);
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
        List<WalEntry> batch;
        LinkedListNode<InFlightFlush> node;
        InFlightFlush slot;
        lock (_stateGate)
        {
            if (_pendingBatch.Count == 0)
            {
                return;
            }

            batch = _pendingBatch;
            var acks = _pendingAcks;
            _pendingBatch = RentBatchList();
            _pendingAcks = RentAckList();
            _pendingBatchSizeBytes = 0;

            slot = new InFlightFlush
            {
                StartOffset = batch[0].Offset,
                EndOffsetExclusive = batch[^1].Offset + 1,
                Acks = acks,
            };
            node = _inFlight.AddLast(slot);
        }
        slot.Task = FlushAsync(batch, node);
    }

    private async Task FlushAsync(List<WalEntry> batch, LinkedListNode<InFlightFlush> node)
    {
        // Yield once before any provider call so this task is observably
        // incomplete by the time StartFlush stores the Task on the slot
        // (a synchronously-completing provider would otherwise run the
        // body inline and the slot's Task field would still be the
        // default Task<VoidTaskResult> placeholder when we attempt to
        // remove the node in the finally - the contract is "every slot
        // in _inFlight carries a Task that completes when its provider
        // call settles").
        await Task.Yield();

        var slot = node.Value;
        try
        {
            await _provider.AppendBatchAsync(_treeId, _shardIndex, batch, CancellationToken.None).ConfigureAwait(true);

            // If a predecessor already failed and faulted us, our acks
            // were faulted in HandleFailureAsync; do not try to satisfy
            // them with a result. Our provider call may have committed
            // against an orphaned offset window - that is the price of
            // multi-batch concurrency and is reconciled by the post-
            // failure resync against GetHighestOffsetAsync.
            if (Volatile.Read(ref _stickyFailure) is null)
            {
                for (var i = 0; i < slot.Acks.Count; i++)
                {
                    slot.Acks[i].TrySetResult(batch[i].Offset);
                }
            }
        }
        catch (Exception ex)
        {
            await HandleFlushFailureAsync(node, ex).ConfigureAwait(true);
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
                    ReturnBatchList(batch);
                    ReturnAckList(slot.Acks);
                }
            }
        }

        // Drain a follow-on batch that accumulated while we were in
        // flight. Done outside the try/finally so a fresh flush failure
        // is observed cleanly by its own callers. Snapshot the relevant
        // state under the gate to make the trigger decision; only kick
        // a flush when the chain has fully drained and a pending batch
        // is waiting.
        bool kickFollowOn;
        lock (_stateGate)
        {
            kickFollowOn = _stickyFailure is null && _pendingBatch.Count > 0 && _inFlight.Count == 0;
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

            // Reset pending state immediately so any append that races in
            // on a future grain turn (and would otherwise be admitted by
            // the cap loop) sees an empty pending and short-circuits on
            // _stickyFailure instead. We hold the stale TCSs locally for
            // post-resync faulting.
            stalePending = _pendingAcks;
            _pendingBatch = RentBatchList();
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
            var highest = await _provider.GetHighestOffsetAsync(_treeId, _shardIndex, CancellationToken.None).ConfigureAwait(true);
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
    /// window it owns, the ack TCSs parked against it, and the task
    /// that completes when the provider's <c>AppendBatchAsync</c> for
    /// this slot has settled.
    /// </summary>
    private sealed class InFlightFlush
    {
        public long StartOffset { get; init; }
        public long EndOffsetExclusive { get; init; }
        public required List<TaskCompletionSource<long>> Acks { get; init; }
        public Task Task { get; set; } = System.Threading.Tasks.Task.CompletedTask;
    }

    /// <summary>
    /// Approximates the serialised size of a captured
    /// <see cref="WalRecord"/> for batch-byte-budget accounting. The
    /// estimate covers the key bytes (UTF-16 worst case), the value
    /// bytes, and a constant overhead for the record envelope, HLC,
    /// origin cluster id, and Orleans slot tags. Documented as
    /// approximate in <see cref="LatticeOptions.WalMaxBatchBytes"/>.
    /// </summary>
    private static long EstimateSize(WalRecord entry)
    {
        var keyBytes = entry.Key is { } k ? k.Length * 2 : 0;
        var valueBytes = entry.Value?.Length ?? 0;
        return keyBytes + valueBytes + EntrySizeOverhead;
    }

    /// <summary>
    /// Rents a cleared <see cref="WalEntry"/> batch list from the
    /// per-grain pool, or allocates a fresh one if the pool is empty.
    /// Must be called under <see cref="_stateGate"/>.
    /// </summary>
    private List<WalEntry> RentBatchList()
    {
        if (_batchListPool.Count > 0)
        {
            return _batchListPool.Pop();
        }
        return new List<WalEntry>();
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
    /// Returns a no-longer-needed batch list to the pool. The caller
    /// must guarantee no other reference to the list survives.
    /// Lists that exceed the pool depth are dropped to the GC; lists
    /// whose backing array has grown unusually large are also dropped
    /// to keep the pool's resident footprint bounded. Must be called
    /// under <see cref="_stateGate"/>.
    /// </summary>
    private void ReturnBatchList(List<WalEntry> list)
    {
        if (_batchListPool.Count >= MaxPoolDepth || list.Capacity > 4096)
        {
            return;
        }
        list.Clear();
        _batchListPool.Push(list);
    }

    /// <summary>
    /// Returns a no-longer-needed ack list to the pool. Same contract
    /// as <see cref="ReturnBatchList"/>. Must be called under
    /// <see cref="_stateGate"/>.
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
        var highest = await provider.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(true);
        _nextOffset = highest + 1;
        _initialized = true;
    }
}
