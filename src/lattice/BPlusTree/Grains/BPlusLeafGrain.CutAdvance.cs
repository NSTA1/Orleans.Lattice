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
    private long CutAdvanceTarget(
        int partition,
        int partitions,
        long current,
        in WalSplitCut cut,
        LiveAdvanceHandOff handOff,
        string? low,
        string? high)
    {
        var closeHead = LiveAdvanceCloseHead(handOff, low, high, partition);
        if (current >= closeHead - 1 && current >= Math.Max(cut.ForeignHigh, cut.UnstampedWatermark))
        {
            return cut.Head - 1;
        }

        NoteReportedBoundCatchUp(partition, partitions, LeafLiveAdvanceCore.SplitCatchUpTarget(
            current: current,
            closeHead: closeHead,
            closesConcrete: AreCloseHeadsConcrete(handOff, low, high),
            boundPending: HandOffMarkerCore.HasPendingDonorBound(handOff.LeafMarker, low, high),
            foreignHigh: cut.ForeignHigh,
            watermark: cut.UnstampedWatermark));
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

    /// <summary>
    /// Advances each checkpointed partition to its consistent cut's head where the cut and
    /// a view read after it prove the leaf has read every record below the head it could
    /// own (issue #6166); the guard is the split's (<see cref="CutAdvanceTarget"/>). Returns
    /// whether every checkpointed partition now stands at or above its cut head minus one,
    /// so a drive would find nothing the cut did not already pass. Never on a write; a
    /// never-checkpointed partition is left to the starvation drive.
    /// </summary>
    /// <param name="partitions">The tree's WAL partition count.</param>
    internal async Task<bool> TryAdvanceCheckpointFromCutAsync(int partitions)
    {
        if (_liveAdvanceInFlight
            || !IsCutAdvanceQuiescent()
            || _appendFence is not { } fence
            || !IsLiveAdvanceOverlapExcluded()
            || !IsPreArmGateOpen()
            || state.State.TreeId is not { Length: > 0 } treeId)
        {
            return false;
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
            return false;
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
                return false;
            }

            if (_cutAdvanceScratch is not { } cut || cut.Length != partitions)
            {
                cut = _cutAdvanceScratch = new WalSplitCut[partitions];
                _cutAdvanceReads = new Task<WalSplitCut>[partitions];
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
                return false;
            }

            // Read after the cut, so every record the cut's heads pass predates the view.
            if (!IsCutUsable(await ReadLiveAdvanceHandOffAsync(), in reader, out var handOff))
            {
                return false;
            }

            var low = state.State.LowKeyInclusive;
            var high = state.State.HighKeyExclusive;
            var reached = true;
            for (var p = 0; p < partitions; p++)
            {
                // Re-checked per partition: a stamp may await a persist, and a write or a
                // prepare can interleave with it; an own append below the head must be
                // applied before the head is passed.
                if (!IsCutAdvanceQuiescent())
                {
                    return false;
                }

                var current = GetCurrentCheckpointForPartition(p);
                if (current < 0 || IsPartitionStaleLatched(p))
                {
                    reached = false;
                    continue;
                }

                var target = CutAdvanceTarget(p, partitions, current, in cut[p], handOff, low, high);
                if (target < 0)
                {
                    reached = false;
                    continue;
                }

                if (target > current)
                {
                    await StampAdvanceAsync(p, target, live: false);
                }

            }

            return reached;
        }
        finally
        {
            _liveAdvanceInFlight = false;
        }
    }
}
