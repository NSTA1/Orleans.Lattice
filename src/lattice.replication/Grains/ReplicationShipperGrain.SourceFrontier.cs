using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The applied low watermark this shipper vouches for at its peer (issue #4586
/// part 2b): every write this cluster authored to the tree and stamped strictly
/// below it was acknowledged by the peer, in the peer's current lineage of the
/// tree. Shipped beside every batch (<see cref="ReplicationBatch.SourceFrontier"/>)
/// and on an idle link's liveness probe.
/// <list type="bullet">
/// <item><b>Per partition.</b> A shipping read returns the partition's clock floor
/// <c>F</c> paired with the offset <c>O</c> it was in force at: every fresh local
/// write at an offset at or past <c>O</c> carries a stamp at or past <c>F</c>. So
/// once the durable cursor - which only acknowledgements, capped at held
/// terminals, move - has reached <c>O</c>, every local write stamped below
/// <c>F</c> has been acknowledged.</item>
/// <item><b>Per tree:</b> the minimum over partitions, every partition being
/// required to have such a floor.</item>
/// <item><b>Clamps</b> it never passes: the earliest acknowledged prepare of a saga
/// whose terminals are not all acknowledged (a prepared write is invisible on the
/// peer until then); a record the cursor passed without delivering; and a prepare
/// it could not track.</item>
/// <item><b>Frozen</b> (no watermark at all) while the peer is off the log, while a
/// replay filter is set, while the latest acknowledgement did not report the
/// peer's lineage, while the peer tracks none, while the cluster's clock floor
/// gate is closed, and for a tree whose replication is key-filtered.</item>
/// <item><b>Lineage.</b> A move to a new non-empty receiver lineage means the
/// peer's contents may lack writes already shipped, so it is a forced gap: the
/// shipper re-seeds the peer and vouches again only after the rewind's replay
/// filter clears. Such re-seeds are paced per peer.</item>
/// </list>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    /// <summary>The most unterminated shipped sagas the shipper tracks before it stops vouching.</summary>
    internal const int MaxFrontierPrepares = 4096;

    private const int MaxFloorPairsPerPartition = 16;

    /// <summary>How often a shipper reports its tree's watermark to its peer's aggregate, at the least.</summary>
    internal static TimeSpan SourceFrontierReportInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The least spacing of liveness probes sent only to carry an advanced watermark.</summary>
    internal static TimeSpan SourceFrontierHeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Test seam: overrides whether the cluster's WAL clock floor gate is open.</summary>
    internal Func<bool>? ClockFloorGateOpenForTesting { get; set; }

    // Per partition, the (floor, offset) pairs shipping reads published, offset ascending.
    private List<(HybridLogicalClock Floor, long Offset)>?[] _floorPairs = [];

    // The receiver lineage of the latest acknowledgement this tick, when one was reported.
    private Guid? _tickAckLineage;
    private bool _tickAckLineageReported;

    // Whether the latest acknowledgement reported no lineage; the watermark waits for one that does.
    private bool _latestAckLineageUnreported;

    // Whether anything was acknowledged before the first acknowledgement this
    // tick that reported a lineage other than the one last seen: only then can
    // the peer's new contents lack writes already shipped.
    private bool _acknowledgedBeforeLineageChange;

    // The frontier the next batch carries, and the watermark the peer last received.
    private ReplicationSourceFrontier? _currentFrontier;
    private HybridLogicalClock _deliveredTreeLowWatermark;
    private long _deliveredOriginGeneration;
    private HybridLogicalClock _reportedTreeLowWatermark = HybridLogicalClock.Zero;
    private DateTimeOffset _nextFrontierReportUtc = DateTimeOffset.MinValue;
    private HybridLogicalClock _originLowWatermark;
    private long _originGeneration;

    // The reap-only watermark (issue #4615), and the key filter it was vouched under.
    private HybridLogicalClock _reapLowWatermark = HybridLogicalClock.Zero;
    private (Func<string, bool>? Filter, string[]? Prefixes)? _reapFilterScope;

    /// <summary>The frontier the next batch to the peer carries; test seam.</summary>
    internal ReplicationSourceFrontier? CurrentSourceFrontierForTesting => _currentFrontier;

    /// <inheritdoc />
    public Task<HybridLogicalClock> GetReapLowWatermarkAsync() => Task.FromResult(_reapLowWatermark);

    /// <summary>
    /// The saga records of one shipped batch, captured when it is drained so its
    /// acknowledgement can update <see cref="SourceFrontierShipperState.Prepares"/>.
    /// </summary>
    private sealed record SagaFrontierDelta(
        List<(Guid TransactionId, HybridLogicalClock Stamp)>? Prepares,
        List<(Guid TransactionId, int ShardIndex, int ShardCount)>? Terminals);

    /// <summary>Records the floor a shipping read of <paramref name="partition"/> published.</summary>
    private void NoteClockFloor(int partition, HybridLogicalClock floor, long offset)
    {
        if (floor == HybridLogicalClock.Zero)
        {
            return;
        }

        if (_floorPairs.Length < _partitionCount)
        {
            Array.Resize(ref _floorPairs, _partitionCount);
        }

        var pairs = _floorPairs[partition] ??= new List<(HybridLogicalClock, long)>(4);
        if (pairs.Count > 0)
        {
            var last = pairs[^1];
            if (offset < last.Offset || floor.CompareTo(last.Floor) <= 0)
            {
                // Out of order, or no newer floor: the pair adds nothing sound.
                return;
            }

            if (pairs.Count >= MaxFloorPairsPerPartition)
            {
                // Replacing the newest pair keeps every older one, so the
                // watermark only loses precision, never soundness.
                pairs[^1] = (floor, offset);
                return;
            }
        }

        pairs.Add((floor, offset));
    }

    /// <summary>Discards every recorded floor: they describe a log the shipper no longer reads.</summary>
    private void DiscardClockFloors()
    {
        Array.Clear(_floorPairs);
        _currentFrontier = null;
    }

    /// <summary>Records the receiver lineage an acknowledgement reports.</summary>
    private void NoteReceiverLineage(ReplicationAck ack)
    {
        if (ack.ReceiverLineage is not { } lineage)
        {
            // Not reported this time: no change, and nothing it covers counts.
            _latestAckLineageUnreported = true;
            return;
        }

        _latestAckLineageUnreported = false;
        var frontier = state.State.Frontier;
        var differs = !frontier.LineageObserved || frontier.Lineage != lineage;
        if (differs && (!_tickAckLineageReported || _tickAckLineage != lineage))
        {
            // Called before this acknowledgement's batch folds its cursors.
            foreach (var cursor in state.State.PartitionCursors.Values)
            {
                if (cursor > 0)
                {
                    _acknowledgedBeforeLineageChange = true;
                    break;
                }
            }
        }

        _tickAckLineage = lineage;
        _tickAckLineageReported = true;
    }

    /// <summary>Captures the saga records of the batch about to ship.</summary>
    private SagaFrontierDelta? CaptureSagaFrontierDelta()
    {
        List<(Guid, HybridLogicalClock)>? prepares = null;
        List<(Guid, int, int)>? terminals = null;
        foreach (var record in _drainBuffer)
        {
            if (record.TransactionId == Guid.Empty)
            {
                continue;
            }

            if (record.IsPrepared)
            {
                (prepares ??= new List<(Guid, HybridLogicalClock)>()).Add((record.TransactionId, record.Timestamp));
            }
            else if (record.Op is MutationKind.TxCommit or MutationKind.TxAbort)
            {
                (terminals ??= new List<(Guid, int, int)>()).Add((record.TransactionId, record.ShardIndex, record.AtomicShardCount));
            }
        }

        return prepares is null && terminals is null ? null : new SagaFrontierDelta(prepares, terminals);
    }

    /// <summary>
    /// Folds an acknowledged batch's saga records into the unterminated-prepare
    /// clamp. Runs before the batch's cursor fold, so a cursor written past a
    /// prepare is never persisted without it.
    /// </summary>
    private void ApplySagaFrontierDelta(SagaFrontierDelta? delta)
    {
        if (delta is null)
        {
            return;
        }

        var frontier = state.State.Frontier;
        foreach (var (txid, stamp) in delta.Prepares ?? [])
        {
            if (frontier.Prepares.TryGetValue(txid, out var tracked))
            {
                if (stamp.CompareTo(tracked.MinPrepare) < 0)
                {
                    tracked.MinPrepare = stamp;
                }
            }
            else if (frontier.Prepares.Count < MaxFrontierPrepares)
            {
                frontier.Prepares[txid] = new SourceFrontierPrepare { MinPrepare = stamp };
            }
            else if (frontier.OverflowClamp is not { } overflow || stamp.CompareTo(overflow) < 0)
            {
                frontier.OverflowClamp = stamp;
                Logger.LogWarning(
                    "{Context}: more than {Max} shipped sagas await their terminals; the applied low watermark stops "
                    + "advancing past {Stamp}.",
                    LogContext, MaxFrontierPrepares, stamp);
            }
        }

        foreach (var (txid, shard, count) in delta.Terminals ?? [])
        {
            if (frontier.Prepares.TryGetValue(txid, out var tracked))
            {
                tracked.AckedTerminalShards.Add(shard);
                if (tracked.AckedTerminalShards.Count >= Math.Max(1, count))
                {
                    frontier.Prepares.Remove(txid);
                }
            }
        }
    }

    /// <summary>
    /// Lowers the skip clamp to the earliest local record of the batch the cursor
    /// is about to pass without delivering it.
    /// </summary>
    private async Task NoteSkippedBatchAsync()
    {
        HybridLogicalClock? earliest = null;
        foreach (var record in _drainBuffer)
        {
            if (earliest is not { } e || record.Timestamp.CompareTo(e) < 0)
            {
                earliest = record.Timestamp;
            }
        }

        if (earliest is not { } stamp)
        {
            return;
        }

        var frontier = state.State.Frontier;
        if (frontier.SkipClamp is not { } clamp || stamp.CompareTo(clamp) < 0)
        {
            frontier.SkipClamp = stamp;
        }

        // Only a re-seed from an export after this point carries the record.
        frontier.SkipClampEpoch = await _grainFactory.GetGrain<IReplicationExportEpochGrain>(_treeName).GetAsync();
    }

    /// <summary>
    /// After the rewind a re-seed allows: a skipped record the echoed export
    /// carried no longer clamps, and every tracked saga the origin has decided or
    /// forgotten is settled on the peer - by the export's decision row or
    /// committed rows, or by its terminal the replay re-ships before the filter
    /// clears - so it no longer clamps either. A saga still undecided keeps its
    /// entry until its terminal is acknowledged; a failed read keeps it too.
    /// </summary>
    private async Task OnReseedRewoundForFrontierAsync(long echoedEpoch)
    {
        var frontier = state.State.Frontier;
        if (frontier.SkipClamp is not null && echoedEpoch > frontier.SkipClampEpoch)
        {
            frontier.SkipClamp = null;
        }

        if (frontier.LineageReseedLeased)
        {
            frontier.LineageReseedLeased = false;
            try
            {
                await _grainFactory.GetGrain<IReplicationSourceFrontierAggregateGrain>(_peerClusterId)
                    .ReleaseLineageReseedAsync(_treeName);
            }
            catch (Exception ex)
            {
                // The slot lapses on its own.
                Logger.LogDebug(ex, "{Context}: releasing the lineage re-seed slot failed.", LogContext);
            }
        }

        if (frontier.Prepares.Count == 0)
        {
            return;
        }

        foreach (var txid in frontier.Prepares.Keys.ToList())
        {
            try
            {
                var registry = TxRegistryRouting.GetRegistry(_grainFactory, _treeName, txid);
                var decided = await registry.GetRecordedStatusAsync(txid) is TxStatus.Committed or TxStatus.Aborted;
                if (decided || (await registry.GetParticipantsAsync(txid)).Count == 0)
                {
                    frontier.Prepares.Remove(txid);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogDebug(ex, "{Context}: could not read saga {TransactionId}; it keeps clamping the watermark.", LogContext, txid);
            }
        }
    }

    /// <summary>
    /// The durable read positions shipped beside a vouched watermark (issue
    /// #4684): the ones the WAL GC reads, capped at held terminals and at a
    /// re-seed's retained floor, on the bound log. Never shipped without a
    /// watermark, so never while the peer is off the log or a replay filter is
    /// set - the states in which a record can be passed without delivery.
    /// </summary>
    private ReplicationAckedPositions? AckedPositionsForFrontier()
    {
        var log = _walTreeId;
        if (string.IsNullOrEmpty(log) || state.State.DetachedFromLog)
        {
            return null;
        }

        return new ReplicationAckedPositions { PhysicalTreeId = log, Positions = [.. CurrentPartitionPositions()] };
    }

    /// <summary>A saga the replay filter withholds whole never sends a terminal; it stops clamping.</summary>
    private void ForgetFrontierPrepare(Guid transactionId) =>
        state.State.Frontier.Prepares.Remove(transactionId);

    /// <summary>
    /// End-of-tick step: acts on a receiver lineage change, then recomputes the
    /// frontier the next batch carries and reports it to the peer's aggregate.
    /// Never throws: a failure leaves the shipper vouching for nothing.
    /// </summary>
    private async Task RefreshSourceFrontierAsync(LatticeReplicationOptions options)
    {
        try
        {
            await ApplyReceiverLineageAsync();
            NoteReapFilterScope(options);
            var tree = ComputeTreeLowWatermark(options);
            _reapLowWatermark = ComputeTreeLowWatermark(options, forReap: true);
            await ReportSourceFrontierAsync(tree);
            _currentFrontier = tree == HybridLogicalClock.Zero || state.State.Frontier.Lineage is not { } lineage || lineage == Guid.Empty
                ? null
                : new ReplicationSourceFrontier
                {
                    ReceiverLineage = lineage,
                    TreeLowWatermark = tree,
                    OriginLowWatermark = _originLowWatermark.CompareTo(tree) <= 0 ? _originLowWatermark : tree,
                    OriginGeneration = _originGeneration,
                    AckedPositions = AckedPositionsForFrontier(),
                };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _currentFrontier = null;
            _reapLowWatermark = HybridLogicalClock.Zero;
            Logger.LogWarning(ex, "{Context}: refreshing the applied low watermark failed; none is shipped this tick.", LogContext);
        }
    }

    /// <summary>
    /// A move to a new non-empty receiver lineage - from a different one, from an
    /// empty one, or from none seen - is a forced gap once anything was
    /// acknowledged: the peer's contents may lack writes already shipped. The
    /// re-seed waits for the peer's pacing slot.
    /// </summary>
    private async Task ApplyReceiverLineageAsync()
    {
        var frontier = state.State.Frontier;
        if (_tickAckLineageReported)
        {
            _tickAckLineageReported = false;
            var seen = _tickAckLineage!.Value;
            var acknowledgedBefore = _acknowledgedBeforeLineageChange;
            _acknowledgedBeforeLineageChange = false;
            if (!frontier.LineageObserved || frontier.Lineage != seen)
            {
                var forced = seen != Guid.Empty && acknowledgedBefore;
                frontier.LineageObserved = true;
                frontier.Lineage = seen;
                if (forced)
                {
                    frontier.LineageReseedPending = true;
                }

                await state.WriteStateAsync();
                if (forced)
                {
                    Logger.LogWarning(
                        "{Context}: the peer's lineage of the tree changed to {Lineage}; its contents may lack writes already "
                        + "shipped, so it will be re-seeded.",
                        LogContext, seen);
                }
            }
        }

        if (frontier.LineageReseedPending && !frontier.LineageReseedLeased)
        {
            var granted = await _grainFactory.GetGrain<IReplicationSourceFrontierAggregateGrain>(_peerClusterId)
                .TryAcquireLineageReseedAsync(_treeName);
            if (!granted)
            {
                return;
            }

            frontier.LineageReseedLeased = true;
            frontier.LineageReseedPending = false;
            await RemarkReseedRequiredAsync();
        }
    }

    /// <summary>
    /// Takes the peer off the log, or - when it already is - raises the marker to
    /// the tree's current export epoch, so an export taken before this point
    /// cannot satisfy the re-seed.
    /// </summary>
    private async Task RemarkReseedRequiredAsync()
    {
        if (!ReseedRequired)
        {
            var marked = await TakePeerOffLogStateAsync();
            _terminalHolds.Clear();
            _prepareTallies.Clear();
            _prepareTallyOrder.Clear();
            await state.WriteStateAsync();
            ReportReseedState();
            Logger.LogWarning(
                "{Context}: saga records are withheld from the peer until it is re-seeded from a snapshot export after "
                + "epoch {Epoch}, because its lineage of the tree changed.",
                LogContext, marked);
            return;
        }

        var epoch = await _grainFactory.GetGrain<IReplicationExportEpochGrain>(_treeName).GetAsync();
        if (state.State.ReseedRequiredEpoch is { } marker && epoch > marker)
        {
            state.State.ReseedRequiredEpoch = epoch;
        }

        await state.WriteStateAsync();
    }

    /// <summary>The tree's watermark toward the peer, or zero when it vouches for nothing.</summary>
    private HybridLogicalClock ComputeTreeLowWatermark(LatticeReplicationOptions options, bool forReap = false)
    {
        var frontier = state.State.Frontier;
        if (_latestAckLineageUnreported
            || frontier.Lineage is not { } lineage
            || lineage == Guid.Empty
            || frontier.LineageReseedPending
            || ReseedRequired
            || state.State.ReplayFilterHorizon is not null
            || (!forReap && (options.KeyFilter is not null || options.KeyPrefixes is { Count: > 0 }))
            || !IsClockFloorGateOpen()
            || _partitionCount == 0
            || _floorPairs.Length < _partitionCount)
        {
            return HybridLogicalClock.Zero;
        }

        HybridLogicalClock? watermark = null;
        for (var p = 0; p < _partitionCount; p++)
        {
            var cursor = state.State.PartitionCursors.TryGetValue(p, out var c) ? c : 0L;
            var pairs = _floorPairs[p];
            if (pairs is null)
            {
                return HybridLogicalClock.Zero;
            }

            HybridLogicalClock? best = null;
            var covered = 0;
            for (var i = 0; i < pairs.Count; i++)
            {
                if (pairs[i].Offset <= cursor)
                {
                    best = pairs[i].Floor;
                    covered = i;
                }
            }

            if (best is not { } floor)
            {
                return HybridLogicalClock.Zero;
            }

            // The covered pair supersedes every older one.
            if (covered > 0)
            {
                pairs.RemoveRange(0, covered);
            }

            if (watermark is not { } w || floor.CompareTo(w) < 0)
            {
                watermark = floor;
            }
        }

        var result = watermark ?? HybridLogicalClock.Zero;
        foreach (var tracked in frontier.Prepares.Values)
        {
            if (tracked.MinPrepare.CompareTo(result) < 0)
            {
                result = tracked.MinPrepare;
            }
        }

        if (frontier.OverflowClamp is { } overflow && overflow.CompareTo(result) < 0)
        {
            result = overflow;
        }

        if (frontier.SkipClamp is { } skip && skip.CompareTo(result) < 0)
        {
            result = skip;
        }

        return result;
    }

    private bool IsClockFloorGateOpen() =>
        ClockFloorGateOpenForTesting is { } overridden
            ? overridden()
            : Context.ActivationServices?.GetService<IWalClockFloorGate>()?.IsOpen ?? false;

    /// <summary>
    /// The reap-only watermark ignores the key filter: it claims only that every
    /// in-scope write this cluster stamped below it was acknowledged by the
    /// peer, which is all the tombstone reap gate needs, since the peer never
    /// holds an out-of-scope key. It is never shipped. A change to the filter -
    /// a widening above all - would let a key that was out of scope ride on a
    /// watermark vouched under the old scope, so any change discards the floors
    /// and the watermark is zero until the cursor re-covers one read under the
    /// new scope.
    /// </summary>
    private void NoteReapFilterScope(LatticeReplicationOptions options)
    {
        var prefixes = options.KeyPrefixes is { Count: > 0 } configured
            ? configured.Order(StringComparer.Ordinal).ToArray()
            : null;
        if (_reapFilterScope is { } scope
            && ReferenceEquals(scope.Filter, options.KeyFilter)
            && (scope.Prefixes is null ? prefixes is null : prefixes is not null && scope.Prefixes.AsSpan().SequenceEqual(prefixes)))
        {
            return;
        }

        if (_reapFilterScope is not null)
        {
            DiscardClockFloors();
            Logger.LogInformation(
                "{Context}: the key filter changed; the tombstone reap watermark restarts under the new scope.", LogContext);
        }

        _reapFilterScope = (options.KeyFilter, prefixes);
    }

    /// <summary>
    /// Reports the tree's watermark and lineage to the peer's aggregate when it
    /// changed or the report interval elapsed, and keeps the aggregate it answers.
    /// </summary>
    private async Task ReportSourceFrontierAsync(HybridLogicalClock tree)
    {
        var now = _cursorFlushClock.GetUtcNow();
        if (tree == _reportedTreeLowWatermark && now < _nextFrontierReportUtc)
        {
            return;
        }

        var frontier = state.State.Frontier;
        var (origin, generation) = await _grainFactory.GetGrain<IReplicationSourceFrontierAggregateGrain>(_peerClusterId)
            .ReportAsync(_treeName, frontier.LineageObserved ? frontier.Lineage ?? Guid.Empty : Guid.Empty, tree, frontier.LineageReseedLeased);
        _originLowWatermark = origin;
        _originGeneration = generation;
        _reportedTreeLowWatermark = tree;
        _nextFrontierReportUtc = now + SourceFrontierReportInterval;
    }

    /// <summary>
    /// Whether an idle link should send a probe only to carry a watermark the peer
    /// has not received yet.
    /// </summary>
    private bool SourceFrontierHeartbeatDue()
    {
        if (_currentFrontier is not { } frontier
            || (frontier.TreeLowWatermark.CompareTo(_deliveredTreeLowWatermark) <= 0
                && frontier.OriginGeneration == _deliveredOriginGeneration))
        {
            return false;
        }

        return DateTime.UtcNow - _lastSuccessfulContactUtc >= SourceFrontierHeartbeatInterval;
    }

    /// <summary>Records the frontier an accepted batch or probe carried as delivered.</summary>
    private void NoteSourceFrontierDelivered(ReplicationSourceFrontier? frontier)
    {
        if (frontier is { } delivered)
        {
            if (delivered.TreeLowWatermark.CompareTo(_deliveredTreeLowWatermark) > 0)
            {
                _deliveredTreeLowWatermark = delivered.TreeLowWatermark;
            }

            _deliveredOriginGeneration = delivered.OriginGeneration;
        }
    }
}
