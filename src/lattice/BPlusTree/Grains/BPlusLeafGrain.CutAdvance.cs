using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The consistent-cut advance (issue #6166), and the guard and stamp it shares with the
/// split's cut uplift (#5837, D4) and the live advance (#5572).
/// <para>
/// A leaf that applies no writes of its own - a leaf a sorted bulk append left behind its
/// right edge, say - never live-advances, so its checkpoint moved only by replay. Each
/// stall drive then re-read every record appended since, by every author, and a leaf
/// reactivated after a long seed re-read the whole seed behind its birth before serving a
/// read: per leaf, a span that grows with the log, so the seed grew superlinearly and
/// reads collapsed behind the replay permits.
/// </para>
/// <para>
/// The split already knows how to pass such a span without reading it: the consistent
/// cut (<see cref="IWalShardGrain.GetSplitCutAsync"/>) reads each partition's readable
/// head with every bound the leaf must have read past, in one locked step, and a leaf
/// whose read position is at or above those bounds and every close head, read in a view
/// taken after the cut, has read every record below the head it could own. The split
/// raises its sibling's hint, and its own read position, to that head. The stall drive
/// now does the same before it replays: the same cut read, the same view, the same guard
/// (<see cref="CutAdvanceTarget"/>), so where the cut lets the leaf pass the span it
/// replays nothing. Where it does not, the drive runs as before.
/// </para>
/// <para>
/// Cost: the cut is one call per partition, made only where a drive was about to make
/// more, and the scratch it reads into is one pair of arrays per activation, allocated on
/// the first cut advance.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>The cut the latest cut advance read; per activation, reused.</summary>
    private WalSplitCut[]? _cutAdvanceScratch;

    /// <summary>The cut reads in flight for <see cref="_cutAdvanceScratch"/>; cleared after each read.</summary>
    private Task<WalSplitCut>[]? _cutAdvanceReads;

    /// <summary>
    /// The highest close head bearing on the leaf at <paramref name="partition"/>: its own
    /// marker's, and its shard marker's. Every advance that passes other authors' records
    /// must have read to one below it.
    /// </summary>
    private static long LiveAdvanceCloseHead(LiveAdvanceHandOff handOff, string? low, string? high, int partition) =>
        Math.Max(
            HandOffMarkerCore.RequiredHead(handOff.LeafMarker, low, high, partition),
            HandOffMarkerCore.HeadAt(handOff.ShardCloseHeads, partition));

    /// <summary>
    /// Stamps <paramref name="target"/> as partition <paramref name="partition"/>'s
    /// checkpoint. A <paramref name="live"/> stamp records the live advance's origin and
    /// is not counted as replay progress by the stall check; any other stamp is a read
    /// position the leaf has earned, as a replay's is.
    /// </summary>
    private async Task StampAdvanceAsync(int partition, long target, bool live)
    {
        var stampedFrom = GetCurrentCheckpointForPartition(partition);
        if (live)
        {
            _liveAdvanceStamp = (partition, target);
        }

        await StampCheckpointHintAsync(partition, target);
        _liveAdvanceStamp = null;
        if (live)
        {
            _checkpointLiveAdvancedSinceStallCheck += Math.Max(0, GetCurrentCheckpointForPartition(partition) - stampedFrom);
        }
    }

    /// <summary>
    /// Whether a cut read by <paramref name="reader"/> may raise a read position under
    /// <paramref name="view"/>, read after it: no hand-off is open on the leaf, and the
    /// reader carried every donor close the view shows pending.
    /// </summary>
    private bool IsCutUsable(LiveAdvanceHandOff? view, in WalAppendStamp reader, out LiveAdvanceHandOff handOff)
    {
        if (view is { Clear: true } clear
            && _appendFence is { } fence
            && fence.CarriesEveryPendingClose(in reader))
        {
            handOff = clear;
            return true;
        }

        handOff = default;
        return false;
    }

    /// <summary>
    /// The read position partition <paramref name="partition"/> may take from
    /// <paramref name="cut"/> - its head minus one - when <paramref name="current"/> is at or
    /// above every close head and every bound the cut reported (#5837, D4); otherwise -1,
    /// noting the catch-up read owed where the cut's bounds or concrete close heads alone
    /// hold it (#5851, W1-W5), which the coverage-lag tick dispatches.
    /// </summary>
    /// <param name="catchUp">The catch-up read's target when one is owed; otherwise -1.</param>
    private long CutAdvanceTarget(
        int partition,
        int partitions,
        long current,
        in WalSplitCut cut,
        LiveAdvanceHandOff handOff,
        string? low,
        string? high,
        out long catchUp)
    {
        var closeHead = LiveAdvanceCloseHead(handOff, low, high, partition);
        if (current >= closeHead - 1 && current >= Math.Max(cut.ForeignHigh, cut.UnstampedWatermark))
        {
            catchUp = -1;
            return cut.Head - 1;
        }

        catchUp = LeafLiveAdvanceCore.SplitCatchUpTarget(
            current: current,
            closeHead: closeHead,
            closesConcrete: AreCloseHeadsConcrete(handOff, low, high),
            boundPending: HandOffMarkerCore.HasPendingDonorBound(handOff.LeafMarker, low, high),
            foreignHigh: cut.ForeignHigh,
            watermark: cut.UnstampedWatermark);
        NoteReportedBoundCatchUp(partition, partitions, catchUp);
        return -1;
    }

    /// <summary>
    /// Reads every partition's consistent cut for the range the leaf owns now into
    /// <paramref name="into"/>, the calls issued together through <paramref name="reads"/>,
    /// which is cleared after. K2, K4 (#5851): the range is read in the same turn as the cut.
    /// </summary>
    private async Task ReadConsistentCutAsync(string treeId, WalAppendStamp reader, WalSplitCut[] into, Task<WalSplitCut>[] reads)
    {
        var low = state.State.LowKeyInclusive;
        var high = state.State.HighKeyExclusive;
        try
        {
            for (var p = 0; p < into.Length; p++)
            {
                reads[p] = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{p}").GetSplitCutAsync(reader, low, high, CancellationToken.None);
            }

            for (var p = 0; p < into.Length; p++)
            {
                into[p] = await reads[p];
            }
        }
        finally
        {
            Array.Clear(reads);
        }
    }

    /// <summary>What a stall drive does after <see cref="TryAdvanceCheckpointFromCutAsync"/> (#6166).</summary>
    internal enum CutAdvanceOutcome
    {
        /// <summary>The cut could not be used: the drive replays to the head, as before.</summary>
        NotTaken,

        /// <summary>Every checkpointed partition stands at or above its cut head minus one: nothing is left to replay.</summary>
        Reached,

        /// <summary>
        /// A hand-off open on the shard's marker overlaps the leaf, or the hand-off view could
        /// not be read for a reason that clears on its own (an undecided descent while a bulk
        /// graft moves the shard's routing, a failed read). A replay now would be read again
        /// past what holds it, so the drive waits for the next stall check.
        /// </summary>
        Deferred,

        /// <summary>
        /// Every partition not at its cut head is held only by a read to a known target
        /// (<see cref="CutCatchUpBoundsForReplay"/>): the drive replays to those targets, never
        /// to the head, and the next stall check takes the cut.
        /// </summary>
        CatchUp,
    }

    /// <summary>The per-partition catch-up targets the latest cut advance found; per activation, reused; -1 where none.</summary>
    private long[]? _cutCatchUpBounds;

    /// <summary>
    /// Inclusive per-partition caps a drive's replay reads no further than (#6166), set for
    /// the duration of one capped catch-up; <see langword="null"/> otherwise. A cap only
    /// shortens the read, and the checkpoint moves only to what it read, so it never passes
    /// a record it did not read.
    /// </summary>
    private long[]? _replayReadCap;

    /// <summary>The catch-up targets <see cref="CutAdvanceOutcome.CatchUp"/> reported.</summary>
    internal long[]? CutCatchUpBoundsForReplay => _cutCatchUpBounds;

    /// <summary>
    /// The exclusive head a replay of <paramref name="partition"/> reads to: <paramref name="head"/>,
    /// or one past the capped catch-up target while <see cref="_replayReadCap"/> is set.
    /// </summary>
    private long CapReplayHead(int partition, long head) =>
        _replayReadCap is { } cap && partition < cap.Length && cap[partition] >= 0 && cap[partition] + 1 < head
            ? cap[partition] + 1
            : head;

    /// <summary>
    /// Advances each checkpointed partition to its consistent cut's head where the cut and
    /// a view read after it prove the leaf has read every record below the head it could
    /// own (issue #6166); the guard is the split's (<see cref="CutAdvanceTarget"/>). Never on a
    /// write; a never-checkpointed partition is left to the starvation drive.
    /// </summary>
    /// <param name="partitions">The tree's WAL partition count.</param>
    /// <returns>What the drive that asked should do next.</returns>
    internal async Task<CutAdvanceOutcome> TryAdvanceCheckpointFromCutAsync(int partitions)
    {
        if (_liveAdvanceInFlight
            || !IsCutAdvanceQuiescent()
            || _appendFence is not { } fence
            || !IsLiveAdvanceOverlapExcluded()
            || !IsPreArmGateOpen()
            || state.State.TreeId is not { Length: > 0 } treeId)
        {
            return CutAdvanceOutcome.NotTaken;
        }

        var anyCheckpointed = false;
        for (var p = 0; p < partitions; p++)
        {
            if (GetCurrentCheckpointForPartition(p) >= 0 && !IsPartitionStaleLatched(p))
            {
                anyCheckpointed = true;
                break;
            }
        }

        if (!anyCheckpointed)
        {
            return CutAdvanceOutcome.NotTaken;
        }

        _liveAdvanceInFlight = true;
        try
        {
            if (!_carriedClosesRead)
            {
                // #5851, LA4, as the split: a reader carrying none of the closes the marker
                // carries would only mismatch the view read after the cut.
                await ReadLiveAdvanceHandOffAsync();
            }

            if (!fence.TryGetCutReader(out var reader))
            {
                return CutAdvanceOutcome.NotTaken;
            }

            if (_cutAdvanceScratch is not { } cut || cut.Length != partitions)
            {
                cut = _cutAdvanceScratch = new WalSplitCut[partitions];
                _cutAdvanceReads = new Task<WalSplitCut>[partitions];
                _cutCatchUpBounds = new long[partitions];
            }

            try
            {
                await ReadConsistentCutAsync(treeId, reader, cut, _cutAdvanceReads!);
            }
            catch (Exception ex)
            {
                ResolveLogger()?.LogDebug(
                    ex,
                    "Leaf {LeafId}: could not read a consistent cut; its checkpoint advances by replay (#6166).",
                    context.GrainId);
                return CutAdvanceOutcome.NotTaken;
            }

            // Read after the cut, so every record the cut's heads pass predates the view.
            var view = await ReadLiveAdvanceHandOffAsync();
            if (!IsCutUsable(view, in reader, out var handOff))
            {
                return view is { ShardHeldOpen: true } || (view is null && _handOffViewTransient)
                    ? CutAdvanceOutcome.Deferred
                    : CutAdvanceOutcome.NotTaken;
            }

            var low = state.State.LowKeyInclusive;
            var high = state.State.HighKeyExclusive;
            var bounds = _cutCatchUpBounds!;
            var reached = true;
            var boundedOnly = true;
            for (var p = 0; p < partitions; p++)
            {
                bounds[p] = -1;

                // Re-checked per partition: a stamp may await a persist, and a write or a
                // prepare can interleave with it; an own append below the head must be
                // applied before the head is passed.
                if (!IsCutAdvanceQuiescent())
                {
                    return CutAdvanceOutcome.NotTaken;
                }

                var current = GetCurrentCheckpointForPartition(p);
                if (current < 0 || IsPartitionStaleLatched(p))
                {
                    reached = false;
                    boundedOnly = false;
                    continue;
                }

                var target = CutAdvanceTarget(p, partitions, current, in cut[p], handOff, low, high, out var catchUp);
                if (target < 0)
                {
                    reached = false;
                    bounds[p] = catchUp;
                    boundedOnly &= catchUp > current;
                    continue;
                }

                bounds[p] = target > current ? target : current;
                if (target > current)
                {
                    await StampAdvanceAsync(p, target, live: false);
                }
            }

            return reached ? CutAdvanceOutcome.Reached
                : boundedOnly ? CutAdvanceOutcome.CatchUp
                : CutAdvanceOutcome.NotTaken;
        }
        finally
        {
            _liveAdvanceInFlight = false;
        }
    }
}
