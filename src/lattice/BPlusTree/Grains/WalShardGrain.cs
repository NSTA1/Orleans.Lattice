using System.Buffers;
using System.Diagnostics;
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
    ILatticeOriginClusterIdResolver clusterIdResolver,
    IWalMutationEncoder encoder) : IWalShardGrain, IGrainBase
{
    private string _treeId = "";
    private int _shardIndex;
    private IWalStorageProvider _provider = null!;
    private long _nextOffset;
    private bool _initialized;

    // Pending state shape after the zero-copy provider hand-off (R-076):
    // each pending entry contributes one pre-encoded payload segment
    // (carrying the bytes the canonical IWalMutationEncoder produced at
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

        var options = optionsMonitor.Get(_treeId);
        _provider = options.WalStorageProvider?.Invoke(_treeId)
            ?? services.GetRequiredService<IWalStorageProvider>();
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
            hasPending = _pendingSegments.Count > 0;
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

        // Encode the mutation once at append time. The exact encoded
        // byte count becomes the budget contribution (replacing the
        // historical heuristic) and the same bytes are handed to the
        // provider on flush via AppendEncodedBatchAsync - one encode per
        // append, no second pass inside the provider. The mutation is
        // converted to the WAL-boundary LatticeMutation shape once,
        // here, so the encoded form is exactly what the storage layer
        // sees on read-back.
        var mutation = WalRecordConverter.FromWalRecord(entry);
        var writer = new PooledByteBufferWriter();
        try
        {
            encoder.Encode(in mutation, writer);
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

        TaskCompletionSource<long> tcs;
        bool kickFlush;
        lock (_stateGate)
        {
            var offset = _nextOffset++;
            _pendingSegments.Add(segment);
            _pendingOffsets.Add(offset);
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
                    || _pendingSegments.Count >= maxEntries
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
        lock (_stateGate)
        {
            if (_pendingSegments.Count == 0)
            {
                return;
            }

            segments = _pendingSegments;
            offsets = _pendingOffsets;
            var acks = _pendingAcks;
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
            };
            node = _inFlight.AddLast(slot);
        }
        slot.Task = FlushAsync(node);
    }

    private async Task FlushAsync(LinkedListNode<InFlightFlush> node)
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
        var segments = slot.Segments;
        var offsets = slot.Offsets;
        Trace($"flush.enter [{slot.StartOffset},{slot.EndOffsetExclusive})");
        try
        {
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
            await _provider.AppendEncodedBatchAsync(
                _treeId,
                _shardIndex,
                encodedArray.AsMemory(),
                offsetsArray.AsMemory(),
                encoder,
                CancellationToken.None).ConfigureAwait(true);

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
        // state under the gate to make the trigger decision; only kick
        // a flush when the chain has fully drained and a pending batch
        // is waiting.
        bool kickFollowOn;
        lock (_stateGate)
        {
            kickFollowOn = _stickyFailure is null && _pendingSegments.Count > 0 && _inFlight.Count == 0;
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
        // Mirror OnActivateAsync's ordering: reconcile any
        // half-committed multi-phase backend state before reading the
        // tail so the test seam exercises the same activation contract
        // production grains use.
        await provider.ReconcileAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(true);
        var highest = await provider.GetHighestOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(true);
        _nextOffset = highest + 1;
        _initialized = true;
    }
}
