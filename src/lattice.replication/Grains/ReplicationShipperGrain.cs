using System.Buffers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Default <see cref="IReplicationShipperGrain"/> implementation.
/// Hosts the per-<c>(tree, peer)</c> outbound ship loop using the
/// shared <see cref="CoordinatorGrain{TSelf}"/> reminder + phase-timer
/// scaffold.
/// <para>
/// Steady-state drain is a partition-resume hot path: each pump tick
/// reads one bounded page per WAL partition starting from a durable
/// per-partition sequence cursor (<see cref="ReplicationShipperState.PartitionCursors"/>),
/// merges the pages by <see cref="HybridLogicalClock"/> ascending via
/// a heap-free linear scan-for-min over partition heads (O(P), and
/// O(1) for the canonical single-partition case), and emits up to
/// <see cref="LatticeReplicationOptions.ShipBatchSize"/> entries per
/// outbound batch. <see cref="IChangeFeed"/> is reserved for bootstrap
/// / test / future-materialiser consumers that have no notion of
/// partition routing.
/// </para>
/// </summary>
internal sealed class ReplicationShipperGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    ILogger<ReplicationShipperGrain> logger,
    IOptionsMonitor<LatticeReplicationOptions> optionsMonitor,
    IReplicationTransport transport,
    IReplicationBatchEncoder encoder,
    ILatticeReplicationCursorRegistry cursorRegistry,
    IGrainFactory grainFactory,
    [PersistentState("replication-shipper", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationShipperState> state,
    ReplicationPeerStats peerStats)
    : CoordinatorGrain<ReplicationShipperGrain>(context, reminderRegistry, logger),
      IReplicationShipperGrain
{
    private readonly IOptionsMonitor<LatticeReplicationOptions> _optionsMonitor =
        optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly IReplicationTransport _transport =
        transport ?? throw new ArgumentNullException(nameof(transport));
    private readonly IReplicationBatchEncoder _encoder =
        encoder ?? throw new ArgumentNullException(nameof(encoder));
    private readonly ILatticeReplicationCursorRegistry _cursorRegistry =
        cursorRegistry ?? throw new ArgumentNullException(nameof(cursorRegistry));
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ReplicationPeerStats _peerStats =
        peerStats ?? throw new ArgumentNullException(nameof(peerStats));

    private string _treeName = "";
    private string _peerClusterId = "";
    private bool _keyParsed;

    /// <summary>
    /// Wall-clock instant at or after which the next phase tick is
    /// allowed to attempt a send. Set to a future value on transient
    /// transport failure to apply backoff. <see cref="DateTime.MinValue"/>
    /// (the default) means "no backoff in effect".
    /// </summary>
    private DateTime _nextRetryAtUtc = DateTime.MinValue;

    /// <summary>Re-entrancy guard. Orleans serialises grain turns, so the field is for clarity rather than concurrency.</summary>
    private bool _pumpInFlight;

    /// <summary>
    /// Random source for backoff jitter. Aliased to the process-wide
    /// thread-safe singleton (<see cref="Random.Shared"/>) — shared
    /// across every shipper activation on this silo. Sufficient for
    /// jitter purposes; not cryptographically random.
    /// </summary>
    private readonly Random _jitterRandom = Random.Shared;

    /// <summary>
    /// Activation-scoped drain buffer reused across pump ticks. Cleared
    /// at the start of every <see cref="PumpOnceAsync"/>; the encoder
    /// consumes the list synchronously inside <see cref="IReplicationBatchEncoder.Encode"/>
    /// so reuse is safe (no aliasing past the call). Bounded in size by
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/>.
    /// </summary>
    private readonly List<ReplogEntry> _drainBuffer = new();

    /// <summary>
    /// Activation-scoped framing buffer reused across pump ticks. Reset
    /// via <c>ResetWrittenCount</c> when the previous batch fits within
    /// the soft budget (<see cref="LargeWriteBufferThreshold"/>); a
    /// one-time spike past the budget recreates the writer so a single
    /// outlier batch does not pin a large array on the heap forever.
    /// </summary>
    private ArrayBufferWriter<byte> _writeBuffer = new();

    /// <summary>
    /// 4 MB soft cap above which the framing buffer is recreated rather
    /// than reset. Sized to match the WAL's per-batch byte budget so
    /// the typical steady-state path always reuses the buffer.
    /// </summary>
    private const int LargeWriteBufferThreshold = 4 * 1024 * 1024;

    // ── Activation-scoped scratch arrays for the k-way HLC merge ──
    //
    // Sized lazily on first pump tick (and resized on partition-count
    // change via Array.Resize). Reused across every subsequent tick;
    // steady-state pump allocates nothing beyond the per-page DTOs the
    // shard grain returns.
    //
    // Index range: [0, _partitionCount).
    //
    //   _partitionPages[p]    — current page entries from partition p,
    //                           or null when that partition is "drained
    //                           for this tick" (no more entries past
    //                           the saved cursor right now).
    //   _partitionPageIndex[p]— next entry index inside the page;
    //                           equals _partitionPages[p].Count when
    //                           the page is exhausted and a refill is
    //                           required to advance further.
    //   _partitionNextSeq[p]  — fromSequence to pass on the next
    //                           ReadAsync call; mirrors
    //                           state.PartitionCursors[p] but kept as a
    //                           primitive long to avoid dictionary
    //                           lookups inside the merge loop.
    //   _partitionMaxReadSeq[p] — highest sequence we have *consumed*
    //                           (shipped or filtered) from partition p
    //                           this tick. -1 means "none consumed yet";
    //                           on positive ack the partition cursor
    //                           advances to this value + 1.
    //   _partitionAdvanced[p] — whether the current tick consumed at
    //                           least one entry from partition p (used
    //                           to bound the cursor write to changed
    //                           partitions on ack).
    private IReadOnlyList<ReplogShardEntry>?[] _partitionPages = Array.Empty<IReadOnlyList<ReplogShardEntry>?>();
    private int[] _partitionPageIndex = Array.Empty<int>();
    private long[] _partitionNextSeq = Array.Empty<long>();
    private long[] _partitionMaxReadSeq = Array.Empty<long>();
    private bool[] _partitionAdvanced = Array.Empty<bool>();
    private IReplogShardGrain?[] _partitionGrainCache = Array.Empty<IReplogShardGrain?>();
    private int _partitionCount;

    /// <summary>
    /// Number of successful cursor advances since the last durable
    /// <see cref="IPersistentState{TState}.WriteStateAsync"/>. Reset
    /// to <c>0</c> on every flush. Counter rather than wall-clock
    /// because the relevant cost is per-batch persistence I/O, and
    /// the per-batch rate is what the operator tunes via
    /// <see cref="LatticeReplicationOptions.ShipCursorWriteInterval"/>.
    /// </summary>
    private int _pendingCursorWrites;

    /// <summary>
    /// Highest HLC reported to the registry (i.e. successfully
    /// persisted in a previous flush). Used to suppress redundant
    /// <see cref="ILatticeReplicationCursorRegistry.ReportCursorAsync"/>
    /// calls when a flush did not actually advance the durable cursor
    /// (e.g. only partition cursors changed since the last flush).
    /// </summary>
    private HybridLogicalClock _lastReportedCursor = HybridLogicalClock.Zero;

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "shipper-keepalive";

    /// <inheritdoc />
    protected override TimeSpan KeepaliveReminderPeriod => TimeSpan.FromSeconds(90);

    /// <inheritdoc />
    /// <remarks>
    /// Read once at activation via <see cref="IOptionsMonitor{TOptions}.CurrentValue"/>.
    /// The Orleans timer infrastructure registers the period at
    /// <see cref="CoordinatorGrain{TSelf}.StartPhaseTimer"/> time, so a runtime
    /// option change does not propagate until the activation is recycled —
    /// which is the same scope as "silo restart picks up the new value".
    /// </remarks>
    protected override TimeSpan PhaseTimerPeriod => _optionsMonitor.CurrentValue.ShipPhaseTimerPeriod;

    /// <inheritdoc />
    protected override bool InProgress => true; // The shipper is steady-state — always running.

    /// <inheritdoc />
    protected override string LogContext => $"shipper {_treeName}/{_peerClusterId}";

    /// <inheritdoc />
    public async Task EnsureActiveAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();
        // RegisterOrUpdateReminder is idempotent; StartPhaseTimer's
        // _phaseTimer ??= guard makes the second call a no-op. Safe
        // for repeated invocation.
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Flushes any pending deferred-persist cursor on graceful
    /// deactivation. Crash deactivations bypass this hook by design —
    /// the receiver's HLC dedupe bounds the replay cost in that case
    /// (at most <see cref="LatticeReplicationOptions.ShipCursorWriteInterval"/>
    /// &#xD7; <see cref="LatticeReplicationOptions.ShipBatchSize"/>
    /// entries get re-shipped and no-op'd at the receiver). A storage
    /// failure during the flush must not block deactivation; the
    /// pending advance is recovered on the next activation by
    /// re-shipping from the last durable cursor.
    /// </summary>
    protected override async Task OnDeactivateCoreAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        if (_pendingCursorWrites == 0)
        {
            return;
        }
        try
        {
            await FlushCursorAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Pending cursor flush failed during deactivation of {Context}; "
                + "recovery will re-ship at most {Pending} batches' worth of entries (receiver dedupes).",
                LogContext, _pendingCursorWrites);
        }
        _ = reason;
    }

    /// <inheritdoc />
    public Task OnDoorbellAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParseGrainKey();
        // Drive an immediate pump on the same Orleans turn rather
        // than waiting for the next 200ms timer tick. Best-effort:
        // any thrown exception is logged by the base class's tick
        // handler and does not propagate to the doorbell caller
        // (the producer-side commit path).
        return ProcessNextPhaseAsync();
    }

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        ParseGrainKey();

        // Honour the backoff budget set by the previous failed pump.
        if (_nextRetryAtUtc > DateTime.UtcNow)
        {
            return;
        }

        if (_pumpInFlight)
        {
            return;
        }

        _pumpInFlight = true;
        try
        {
            await PumpOnceAsync(CancellationToken.None);
        }
        finally
        {
            _pumpInFlight = false;
        }
    }

    /// <summary>
    /// Drains one batch from the change feed, applies producer-side
    /// filters, calls the transport, advances the cursor on positive
    /// ack, and applies backoff on transient failure. Schema-shaped
    /// failures during encode park every offending entry on the
    /// per-tree dead-letter queue (reason
    /// <see cref="LatticeReplicationMetrics.ReasonSchema"/>) and then
    /// advance the cursor past the batch so a single poison entry
    /// never stalls the stream forever; operators inspect / replay /
    /// discard via <see cref="ILatticeReplicationDeadLetters"/>.
    /// </summary>
    private async Task PumpOnceAsync(CancellationToken cancellationToken)
    {
        var options = _optionsMonitor.Get(_treeName);

        // Drain a batch up to ShipBatchSize entries past each
        // partition's resume cursor, k-way merging by HLC and applying
        // KeyFilter / KeyPrefixes / cycle-break inline. The drain
        // buffer is activation-scoped and reused across pump ticks;
        // Orleans serialises grain turns and the _pumpInFlight guard
        // prevents re-entry, so clearing in place is safe.
        _drainBuffer.Clear();
        var maxPerBatch = Math.Max(1, options.ShipBatchSize);
        try
        {
            await DrainBatchAsync(options, maxPerBatch, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Drain failure is transient by definition — back off and retry.
            ApplyBackoff(options, ex, "drain");
            return;
        }

        if (_drainBuffer.Count == 0)
        {
            // No work this tick; preserve any accumulated backoff.
            return;
        }

        var sourceHlc = _drainBuffer[^1].Timestamp;

        // Encode the batch into a buffer; the gRPC transport hands
        // the gRPC stream's IBufferWriter through directly so the
        // encoded bytes never round-trip through a managed array.
        // The opaque-bytes-shaped IReplicationTransport seam still
        // requires us to land bytes into a buffer here (the typed-envelope transport widens
        // the seam to remove this round-trip).
        //
        // The framing buffer is activation-scoped and reused across
        // ticks: in the steady state we ResetWrittenCount() (which
        // keeps the underlying array, just rewinds the write index),
        // and a one-time spike at-or-past the soft cap recreates the
        // writer so a single outlier batch does not pin a multi-MB
        // array on the heap forever. The encoder consumes _drainBuffer
        // synchronously inside Encode, so reuse is safe (no aliasing
        // past the call).
        if (_writeBuffer.Capacity >= LargeWriteBufferThreshold)
        {
            _writeBuffer = new ArrayBufferWriter<byte>();
        }
        else
        {
            _writeBuffer.ResetWrittenCount();
        }
        try
        {
            var envelope = new ReplicationBatchEnvelope
            {
                WireVersion = _encoder.CurrentWireVersion,
                TreeName = _treeName,
                OriginClusterId = options.ClusterId,
                Entries = _drainBuffer,
            };
            _encoder.Encode(envelope, _writeBuffer);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            // Schema-shaped failure during encode: the entries can
            // never be shipped in their current form. Park every
            // entry in the offending batch on the per-tree DLQ
            // tagged ReasonSchema and advance the cursor past the
            // batch so the stream makes progress. Operators inspect
            // / replay / discard via ILatticeReplicationDeadLetters.
            Logger.LogWarning(ex,
                "Encode failed for {EntryCount}-entry batch on {Context}; routing to DLQ and advancing cursor to {Hlc}",
                _drainBuffer.Count, LogContext, sourceHlc);
            await RouteBatchToDeadLetterAsync(ex, cancellationToken);
            await AdvanceCursorAsync(sourceHlc, options, cancellationToken);
            return;
        }

        ReplicationAck ack;
        try
        {
            var batch = new ReplicationBatch
            {
                TargetClusterId = _peerClusterId,
                TreeName = _treeName,
                OriginClusterId = options.ClusterId,
                Payload = _writeBuffer.WrittenMemory,
            };
            ack = await _transport.SendAsync(batch, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ApplyBackoff(options, ex, "transport");
            return;
        }

        if (!ack.Accepted)
        {
            // Receiver rejected the batch; treat as transient (the
            // sender's cursor stays put and we retry after backoff).
            ApplyBackoff(options, exception: null, reason: "ack-rejected");
            return;
        }

        // Trust the receiver's ack frontier. A receiver that fully
        // applied the batch returns the highest entry HLC; a receiver
        // that partially applied returns the partial frontier; a
        // receiver that fully deduped returns a frontier at or below
        // ours and we fall back to sourceHlc below to make progress.
        var advancedTo = ack.HighestAppliedHlc;
        if (advancedTo <= state.State.Cursor)
        {
            // Receiver acknowledged a frontier at or below ours
            // (e.g. every entry was deduped). Still resets the
            // backoff because the round-trip succeeded; advance the
            // cursor to the last shipped entry's HLC so we don't
            // re-ship the same batch next tick.
            advancedTo = sourceHlc;
        }

        await AdvanceCursorAsync(advancedTo, options, cancellationToken);
        // Successful round-trip resets the backoff counter.
        state.State.ConsecutiveFailures = 0;
        _nextRetryAtUtc = DateTime.MinValue;

        // Per-peer telemetry. RecordSuccess clears the consecutive-error
        // counter and stamps the last-contact timestamp; RecordBacklog
        // updates the entries_behind / bytes_behind gauges. The backlog
        // reading is a *lower bound* derived from this tick's drain
        // outcome: when the drain hit ShipBatchSize the WAL had at
        // least one batch worth of entries past our cursor, so we
        // report the just-shipped count and bytes as a floor; when
        // the drain returned fewer than the cap we know we caught up
        // and report zero. This avoids a hot-path WAL frontier query
        // (one extra grain call per partition per tick) while still
        // making "is this peer keeping up?" answerable on the dashboard.
        _peerStats.RecordSuccess(_treeName, _peerClusterId);
        var hitBatchCap = _drainBuffer.Count >= maxPerBatch;
        var entriesBehind = hitBatchCap ? (long)_drainBuffer.Count : 0L;
        var bytesBehind = hitBatchCap ? (long)_writeBuffer.WrittenCount : 0L;
        _peerStats.RecordBacklog(_treeName, _peerClusterId, entriesBehind, bytesBehind);
    }

    /// <summary>
    /// Producer-side filter: applies <see cref="LatticeReplicationOptions.KeyFilter"/> /
    /// <see cref="LatticeReplicationOptions.KeyPrefixes"/> and the
    /// durable origin-based cycle-break (skip entries whose
    /// <see cref="ReplogEntry.OriginClusterId"/> matches the peer's
    /// own cluster id).
    /// </summary>
    private bool ShouldShip(ReplogEntry entry, LatticeReplicationOptions options)
    {
        // Cycle-break: don't ship a peer its own writes back.
        if (entry.OriginClusterId is { } origin
            && string.Equals(origin, _peerClusterId, StringComparison.Ordinal))
        {
            return false;
        }

        if (options.KeyFilter is { } filter)
        {
            if (entry.Key is null || !filter(entry.Key))
            {
                return false;
            }
        }

        if (options.KeyPrefixes is { } prefixes && prefixes.Count > 0)
        {
            if (entry.Key is null)
            {
                return false;
            }
            var matched = false;
            foreach (var prefix in prefixes)
            {
                if (entry.Key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    matched = true;
                    break;
                }
            }
            if (!matched)
            {
                return false;
            }
        }

        return true;
    }

    private async Task AdvanceCursorAsync(
        HybridLogicalClock newCursor,
        LatticeReplicationOptions options,
        CancellationToken cancellationToken)
    {
        // Mutate in-memory state up front so the next pump tick within
        // this activation resumes from the latest known-good cursor
        // even if the durable write is deferred.
        var hlcAdvanced = newCursor.CompareTo(state.State.Cursor) > 0;
        var partitionsAdvanced = AdvancePartitionCursorsInState();

        if (!hlcAdvanced && !partitionsAdvanced)
        {
            return;
        }

        if (hlcAdvanced)
        {
            state.State.Cursor = newCursor;
        }

        _pendingCursorWrites++;
        var interval = Math.Max(1, options.ShipCursorWriteInterval);
        if (_pendingCursorWrites < interval)
        {
            // Defer the durable write. Receiver-side apply is
            // HLC-monotonic and dedupes on (originClusterId, originHlc),
            // so a silo crash inside this window replays at most
            // (interval × ShipBatchSize) entries — the receiver no-ops
            // the duplicates. The WAL GC's view of this peer is
            // pinned at the last reported cursor (_lastReportedCursor)
            // until the flush completes, so the trim frontier never
            // exceeds the durably-recoverable point.
            return;
        }

        await FlushCursorAsync(cancellationToken);
        _ = options; // Reserved for future per-tree report flavours.
    }

    /// <summary>
    /// Persists <see cref="state"/> via
    /// <see cref="IPersistentState{TState}.WriteStateAsync"/> and then
    /// (only on success) reports the durable HLC cursor to the
    /// registry. Idempotent — safe to call when no in-memory advance
    /// is pending. The persistence-then-report ordering is
    /// load-bearing: the WAL GC consumes the reported cursor to
    /// compute the trim frontier, so reporting before persistence
    /// would risk trimming entries we cannot recover after a crash.
    /// </summary>
    private async Task FlushCursorAsync(CancellationToken cancellationToken)
    {
        if (_pendingCursorWrites == 0)
        {
            return;
        }

        await state.WriteStateAsync();
        _pendingCursorWrites = 0;

        var durableCursor = state.State.Cursor;
        if (durableCursor.CompareTo(_lastReportedCursor) <= 0)
        {
            // Only partition cursors changed since the last flush —
            // nothing new for the GC.
            return;
        }

        // Best-effort registry report: a registry-side failure does
        // not unwind the durable cursor advance. We still update
        // _lastReportedCursor so a transient registry outage does not
        // wedge the report indefinitely; the next flush re-reports
        // through the same suppression check on the next advance.
        try
        {
            await _cursorRegistry
                .ReportCursorAsync(_treeName, _peerClusterId, durableCursor, cancellationToken)
                ;
            _lastReportedCursor = durableCursor;
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Cursor registry report failed for {Context}; persisted cursor remains {Cursor}",
                LogContext, durableCursor);
            _lastReportedCursor = durableCursor;
        }
    }

    /// <summary>
    /// Folds the per-tick <see cref="_partitionMaxReadSeq"/> /
    /// <see cref="_partitionAdvanced"/> scratch arrays into the
    /// durable <see cref="ReplicationShipperState.PartitionCursors"/>
    /// dictionary. Returns <see langword="true"/> when at least one
    /// partition cursor actually moved forward (so the caller knows
    /// whether a <c>WriteStateAsync</c> is required).
    /// <para>
    /// Resets the scratch arrays' "advanced" flag once consumed —
    /// the next pump tick starts from a clean slate.
    /// </para>
    /// </summary>
    private bool AdvancePartitionCursorsInState()
    {
        var changed = false;
        for (var p = 0; p < _partitionCount; p++)
        {
            if (!_partitionAdvanced[p])
            {
                continue;
            }
            // _partitionMaxReadSeq[p] is the highest sequence we
            // *consumed* this tick (shipped or filtered). Resume on
            // the next tick from the entry just past it.
            var nextSeq = _partitionMaxReadSeq[p] + 1;
            // Idempotent: only advance the durable cursor when the
            // computed next-seq is strictly greater than what's
            // already there. (Guards against a degenerate case where
            // _partitionAdvanced[p] flips true but no entry was
            // actually consumed past the prior cursor — should not
            // happen given the merge-loop semantics, but the guard
            // is cheap and removes a sharp edge.)
            if (state.State.PartitionCursors.TryGetValue(p, out var existing) && existing >= nextSeq)
            {
                continue;
            }
            state.State.PartitionCursors[p] = nextSeq;
            changed = true;
            // Reset the per-tick flag; _partitionMaxReadSeq stays as
            // the last value (fine — it gets overwritten on the next
            // consume from this partition).
            _partitionAdvanced[p] = false;
        }
        return changed;
    }

    /// <summary>
    /// Drains up to <paramref name="maxPerBatch"/> entries past each
    /// partition's saved resume cursor, k-way merging by HLC ascending
    /// via a heap-free linear scan-for-min over partition heads. Applies
    /// <see cref="ShouldShip"/> inline so filtered entries are
    /// consumed-but-not-shipped (their sequence still advances the
    /// partition cursor on ack so the next tick does not re-read them).
    /// <para>
    /// O(P) per emitted entry — collapses to O(1) for the canonical
    /// single-partition case. Allocates nothing on the hot path beyond
    /// the per-page DTOs the shard grain returns; the scratch arrays
    /// and the drain buffer are activation-scoped.
    /// </para>
    /// </summary>
    private async Task DrainBatchAsync(
        LatticeReplicationOptions options,
        int maxPerBatch,
        CancellationToken cancellationToken)
    {
        var partitions = Math.Max(1, options.ReplogPartitions);
        var pageSize = Math.Max(1, options.ShipPartitionPageSize);

        EnsureScratchSized(partitions);

        // Initialise per-partition state for this tick. _partitionPages
        // and _partitionPageIndex always reset (they're tick-scoped);
        // _partitionNextSeq seeds from the durable cursor;
        // _partitionMaxReadSeq is initialised from the cursor minus 1
        // so a partition that contributes nothing this tick reports
        // "no advance" in AdvancePartitionCursorsInState.
        for (var p = 0; p < partitions; p++)
        {
            _partitionPages[p] = null;
            _partitionPageIndex[p] = 0;
            _partitionAdvanced[p] = false;
            var seeded = state.State.PartitionCursors.TryGetValue(p, out var saved) ? saved : 0L;
            _partitionNextSeq[p] = seeded;
            _partitionMaxReadSeq[p] = seeded - 1;
        }

        // Prime each partition's page from its saved cursor. Done up
        // front so the merge loop below is allocation-free apart from
        // page refills triggered when a partition exhausts its page
        // mid-batch.
        for (var p = 0; p < partitions; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await TryRefillPartitionAsync(p, pageSize, cancellationToken);
        }

        // K-way merge: at every step pick the partition whose head
        // entry has the smallest HLC, consume one entry from it, and
        // advance. When a partition's page is exhausted, refill from
        // the saved next-sequence; if the refill returns empty the
        // partition is "drained for this tick" and excluded from the
        // candidate set.
        while (_drainBuffer.Count < maxPerBatch)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var minPartition = -1;
            HybridLogicalClock minHlc = default;

            for (var p = 0; p < partitions; p++)
            {
                var page = _partitionPages[p];
                if (page is null)
                {
                    continue; // drained this tick
                }
                if (_partitionPageIndex[p] >= page.Count)
                {
                    // Page exhausted — try to refill. We already advanced
                    // _partitionNextSeq on the prior consume so the
                    // refill picks up where we left off.
                    await TryRefillPartitionAsync(p, pageSize, cancellationToken);
                    page = _partitionPages[p];
                    if (page is null)
                    {
                        continue;
                    }
                }

                var head = page[_partitionPageIndex[p]].Entry.Timestamp;
                if (minPartition < 0 || head.CompareTo(minHlc) < 0)
                {
                    minPartition = p;
                    minHlc = head;
                }
            }

            if (minPartition < 0)
            {
                // Every partition drained for this tick.
                break;
            }

            var winningPage = _partitionPages[minPartition]!;
            var winningEntry = winningPage[_partitionPageIndex[minPartition]];
            _partitionPageIndex[minPartition]++;
            _partitionMaxReadSeq[minPartition] = winningEntry.Sequence;
            _partitionAdvanced[minPartition] = true;

            // Defensive HLC filter: a legacy state with a non-zero
            // state.Cursor but an empty PartitionCursors dictionary
            // resumes from sequence 0, which would re-ship every
            // entry the peer has already seen on the very first tick
            // after upgrade. The HLC predicate filters them out at
            // negligible cost (one comparison per entry); steady
            // state never matches because the partition cursor moves
            // strictly forward on ack.
            if (winningEntry.Entry.Timestamp.CompareTo(state.State.Cursor) <= 0)
            {
                continue;
            }

            if (!ShouldShip(winningEntry.Entry, options))
            {
                continue;
            }

            _drainBuffer.Add(winningEntry.Entry);
        }
    }

    /// <summary>
    /// Issues a <see cref="IReplogShardGrain.ReadAsync"/> against the
    /// requested partition starting at <see cref="_partitionNextSeq"/>,
    /// stores the page in the scratch arrays, and updates
    /// <see cref="_partitionNextSeq"/> to the page's
    /// <see cref="ReplogShardPage.NextSequence"/>. An empty result
    /// leaves <see cref="_partitionPages"/> at <see langword="null"/>
    /// for that partition — the caller treats that as "drained this
    /// tick" and stops considering the partition for the rest of the
    /// merge loop.
    /// </summary>
    private async Task TryRefillPartitionAsync(int partition, int pageSize, CancellationToken cancellationToken)
    {
        var grain = _partitionGrainCache[partition] ??=
            _grainFactory.GetGrain<IReplogShardGrain>($"{_treeName}/{partition}");
        var page = await grain
            .ReadAsync(_partitionNextSeq[partition], pageSize, cancellationToken)
            ;
        if (page.Entries.Count == 0)
        {
            _partitionPages[partition] = null;
            return;
        }
        _partitionPages[partition] = page.Entries;
        _partitionPageIndex[partition] = 0;
        _partitionNextSeq[partition] = page.NextSequence;
    }

    /// <summary>
    /// Grows the activation-scoped scratch arrays in lockstep when the
    /// configured <see cref="LatticeReplicationOptions.ReplogPartitions"/>
    /// changes (or on first activation). Idempotent — a no-op when the
    /// arrays are already at the requested size.
    /// </summary>
    private void EnsureScratchSized(int partitions)
    {
        _partitionCount = partitions;
        if (_partitionPages.Length >= partitions)
        {
            return;
        }
        Array.Resize(ref _partitionPages, partitions);
        Array.Resize(ref _partitionPageIndex, partitions);
        Array.Resize(ref _partitionNextSeq, partitions);
        Array.Resize(ref _partitionMaxReadSeq, partitions);
        Array.Resize(ref _partitionAdvanced, partitions);
        Array.Resize(ref _partitionGrainCache, partitions);
    }

    private void ApplyBackoff(LatticeReplicationOptions options, Exception? exception, string reason)
    {
        state.State.ConsecutiveFailures = checked(state.State.ConsecutiveFailures + 1);
        var failures = state.State.ConsecutiveFailures;
        var initialMs = options.ShipBackoffInitial.TotalMilliseconds;
        var maxMs = options.ShipBackoffMax.TotalMilliseconds;
        var multiplier = Math.Pow(2, Math.Max(0, failures - 1));
        var delayMs = Math.Min(maxMs, initialMs * multiplier);

        var jitter = options.ShipBackoffJitter;
        if (jitter > 0.0)
        {
            // Symmetric: [1 - jitter, 1 + jitter]
            var spread = (_jitterRandom.NextDouble() * 2.0 - 1.0) * jitter;
            delayMs = Math.Max(0.0, delayMs * (1.0 + spread));
        }

        var delay = TimeSpan.FromMilliseconds(delayMs);
        _nextRetryAtUtc = DateTime.UtcNow.Add(delay);

        // Per-peer error tally: only count failures that are attributable
        // to the peer round-trip (transport throw, receiver ack rejection).
        // "drain" failures are local WAL read errors — the peer is fine,
        // so they must not bump the consecutive_errors gauge for that peer.
        if (string.Equals(reason, "transport", StringComparison.Ordinal)
            || string.Equals(reason, "ack-rejected", StringComparison.Ordinal))
        {
            _peerStats.RecordError(_treeName, _peerClusterId);
        }

        if (exception is not null)
        {
            Logger.LogWarning(exception,
                "Shipper {Context} {Reason} failed (consecutive={Failures}); backing off {Delay}",
                LogContext, reason, failures, delay);
        }
        else
        {
            Logger.LogDebug(
                "Shipper {Context} {Reason} (consecutive={Failures}); backing off {Delay}",
                LogContext, reason, failures, delay);
        }
    }

    /// <summary>
    /// Routes every entry in the current drain buffer to the per-tree
    /// dead-letter queue, tagged with
    /// <see cref="LatticeReplicationMetrics.ReasonSchema"/>. A
    /// best-effort enqueue failure is logged and swallowed — the
    /// cursor still advances past the batch so a deterministically-
    /// failing DLQ does not pin the ship loop forever; the WAL
    /// retains the originals until the GC pass trims them, so an
    /// operator can still recover off the WAL even when the DLQ is
    /// unavailable.
    /// </summary>
    private async Task RouteBatchToDeadLetterAsync(Exception encodeFailure, CancellationToken cancellationToken)
    {
        var failureReason = encodeFailure.Message ?? "<no message>";
        var dlq = _grainFactory.GetGrain<IReplicationDeadLetterGrain>(_treeName);
        foreach (var entry in _drainBuffer)
        {
            try
            {
                await dlq.EnqueueAsync(
                    entry,
                    failureReason,
                    retryCount: 0,
                    LatticeReplicationMetrics.ReasonSchema,
                    cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogWarning(ex,
                    "Failed to park entry on DLQ for {Context} (key={Key}, hlc={Hlc}); proceeding with cursor advance",
                    LogContext, entry.Key, entry.Timestamp);
            }
        }
    }

    private void ParseGrainKey()
    {
        if (_keyParsed)
        {
            return;
        }

        var key = Context.GrainId.Key.ToString() ?? "";
        if (string.IsNullOrEmpty(key))
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationShipperGrain)} activation key is empty; expected '{{treeName}}/{{peerClusterId}}'.");
        }

        var slash = key.LastIndexOf('/');
        if (slash <= 0 || slash >= key.Length - 1)
        {
            throw new InvalidOperationException(
                $"{nameof(ReplicationShipperGrain)} activation key '{key}' is not in the expected '{{treeName}}/{{peerClusterId}}' format.");
        }

        _treeName = key[..slash];
        _peerClusterId = key[(slash + 1)..];
        _keyParsed = true;
    }

    /// <summary>
    /// Test seam: bypasses Orleans activation and key parsing for
    /// direct unit tests against a fake Orleans context.
    /// </summary>
    internal void InitializeForTesting(string treeName, string peerClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        _treeName = treeName;
        _peerClusterId = peerClusterId;
        _keyParsed = true;
    }
}
