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
/// replays nothing. Where it does not, the drive runs as before. A warm activation does
/// the same before its replay (<see cref="TryAdvanceActivationFromCutAsync"/>), since a
/// leaf deactivated during the seed runs no drive.
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
    /// For the benchmark census (#6166), which close holds a cut at <paramref name="partition"/>:
    /// the leaf's own marker or the shard's bulk-switch closes, with the leaf's birth-close
    /// state and whether its range is still open at the tail - the two cases in which a leaf
    /// reads the shard's closes, which a later fence of the run may have raised to near the
    /// head. Called only when the diagnostic is enabled.
    /// </summary>
    private static string CloseHoldSource(LiveAdvanceHandOff handOff, string? low, string? high, int partition)
    {
        var fromShard = HandOffMarkerCore.HeadAt(handOff.ShardCloseHeads, partition)
            >= HandOffMarkerCore.RequiredHead(handOff.LeafMarker, low, high, partition);
        var birth = handOff.LeafMarker.BulkBirthClose switch
        {
            LeafHandOffState.BulkBirthCloseRecorded => "birth-recorded",
            LeafHandOffState.BulkBirthCloseRevoked => "birth-revoked",
            _ => "birth-none",
        };
        return string.Concat(fromShard ? ".shard." : ".leaf.", birth, high is null ? ".open-high" : ".bounded");
    }

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
    /// <param name="atActivation">
    /// The caller is the activation's replay (<see cref="TryAdvanceActivationFromCutAsync"/>):
    /// the cache holds no record an earlier activation appended above the checkpoint, so the
    /// cut is read for no activation, and every activation's records bound it.
    /// </param>
    /// <returns>What the drive that asked should do next.</returns>
    internal async Task<CutAdvanceOutcome> TryAdvanceCheckpointFromCutAsync(int partitions, bool atActivation = false)
    {
        if (_liveAdvanceInFlight
            || !IsCutAdvanceQuiescent(atActivation)
            || _appendFence is not { } fence
            || !IsLiveAdvanceOverlapExcluded()
            || !IsPreArmGateOpen()
            || state.State.TreeId is not { Length: > 0 } treeId)
        {
            _cutDecline = _liveAdvanceInFlight ? "in-flight"
                : !IsCutAdvanceQuiescent(atActivation) ? "not-quiescent"
                : _appendFence is null ? "no-fence"
                : !IsLiveAdvanceOverlapExcluded() ? "overlap-not-excluded"
                : !IsPreArmGateOpen() ? "pre-arm-gate-closed"
                : "no-tree";
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
            _cutDecline = "never-checkpointed";
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
                _cutDecline = "no-reader";
                return CutAdvanceOutcome.NotTaken;
            }

            if (atActivation)
            {
                // No activation token: every activation's records are foreign to it, as a
                // resumed close reads them (#5837, rule R).
                reader = reader with { Activation = Guid.Empty };
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
                _cutDecline = "cut-read-failed";
                return CutAdvanceOutcome.NotTaken;
            }

            // Read after the cut, so every record the cut's heads pass predates the view.
            var view = await ReadLiveAdvanceHandOffAsync();
            if (!IsCutUsable(view, in reader, out var handOff))
            {
                _cutDecline = view is null ? (_handOffViewTransient ? "view-transient" : "view-unreachable")
                    : view.Value.ShardHeldOpen ? "shard-held-open"
                    : !view.Value.Clear ? "view-not-clear"
                    : "closes-not-carried";
                return view is { ShardHeldOpen: true } || (view is null && _handOffViewTransient)
                    ? CutAdvanceOutcome.Deferred
                    : CutAdvanceOutcome.NotTaken;
            }

            var low = state.State.LowKeyInclusive;
            var high = state.State.HighKeyExclusive;
            var bounds = _cutCatchUpBounds!;
            var reached = true;
            var boundedOnly = true;
            string? heldBy = null;
            for (var p = 0; p < partitions; p++)
            {
                bounds[p] = -1;

                // Re-checked per partition: a stamp may await a persist, and a write or a
                // prepare can interleave with it; an own append below the head must be
                // applied before the head is passed.
                if (!IsCutAdvanceQuiescent(atActivation))
                {
                    _cutDecline = "not-quiescent-mid-cut";
                    return CutAdvanceOutcome.NotTaken;
                }

                var current = GetCurrentCheckpointForPartition(p);
                if (current < 0 || IsPartitionStaleLatched(p))
                {
                    reached = false;
                    boundedOnly = false;
                    heldBy ??= "partition-uncheckpointed";
                    continue;
                }

                var target = CutAdvanceTarget(p, partitions, current, in cut[p], handOff, low, high, out var catchUp);
                if (target < 0)
                {
                    reached = false;
                    bounds[p] = catchUp;
                    boundedOnly &= catchUp > current;
                    if (heldBy is null && AwaitDiagnostics.Enabled)
                    {
                        var closeHead = LiveAdvanceCloseHead(handOff, low, high, p);
                        heldBy = (catchUp > current ? "catch-up-" : "unbounded-")
                            + (current < closeHead - 1 ? "close" + CloseHoldSource(handOff, low, high, p) : current < cut[p].ForeignHigh ? "foreign" : "watermark");
                    }

                    continue;
                }

                bounds[p] = target > current ? target : current;
                if (target > current)
                {
                    await StampAdvanceAsync(p, target, live: false);
                }
            }

            _cutDecline = heldBy ?? "reached";
            return reached ? CutAdvanceOutcome.Reached
                : boundedOnly ? CutAdvanceOutcome.CatchUp
                : CutAdvanceOutcome.NotTaken;
        }
        finally
        {
            _liveAdvanceInFlight = false;
        }
    }

    /// <summary>
    /// At a warm activation, advances each checkpointed partition the consistent cut proves
    /// past (#6166) before the replay reads it. A leaf that applies no writes of its own and
    /// was deactivated during a long bulk seed otherwise re-read the whole seed behind its
    /// checkpoint before serving its first read, holding a replay permit throughout; the
    /// stall drive's cut never reached it, since a deactivated leaf runs no drive.
    /// <para>
    /// Runs only where it saves calls: the backlog the sweep probed, read in slices of
    /// <paramref name="sliceBudget"/>, must take more reads than the cut makes (one per
    /// partition and the hand-off view's two). A single-partition tree probes no head, so it
    /// never runs. Allocates nothing; the replay resumes from each partition's current checkpoint, which the cut's stamps raise.
    /// </para>
    /// </summary>
    /// <param name="sweep">The replay's sweep order, with the heads it probed.</param>
    /// <param name="partitions">The tree's WAL partition count.</param>
    /// <param name="sliceBudget">The replay's slice width.</param>
    /// <param name="allowCatchUp">
    /// Whether a catch-up the cut owes may cap this replay at its targets
    /// (<see cref="_replayReadCap"/>), as a stall drive's does; the caller then takes the cut
    /// again once the capped read is done.
    /// </param>
    /// <param name="census">The activation's census scratch; <see langword="null"/> unless the diagnostic is enabled.</param>
    /// <returns>Whether the cut stamped any partition.</returns>
    private async Task<bool> TryAdvanceActivationFromCutAsync(
        List<(int Partition, long? ProbedHead)> sweep,
        int partitions,
        int sliceBudget,
        bool allowCatchUp,
        ActivationReplayCensus? census)
    {
        if (partitions <= 1)
        {
            CountCut(allowCatchUp, "single-partition", census);
            return false;
        }

        long backlog = 0;
        foreach (var (partition, probedHead) in sweep)
        {
            var checkpoint = GetCurrentCheckpointForPartition(partition);
            if (probedHead is { } head && checkpoint >= 0 && head - 1 > checkpoint)
            {
                backlog += head - 1 - checkpoint;
            }
        }

        if (backlog <= (long)Math.Max(1, sliceBudget) * (partitions + 2))
        {
            CountCut(allowCatchUp, "backlog-under-gate", census);
            return false;
        }

        // The fence exists once the writer is resolved; the cut is read for its closes.
        ResolveCommitLogWriter();
        var before = SumCurrentCheckpoints(partitions);
        using (AwaitDiagnostics.Begin("leaf.replay.cut", context.GrainId))
        {
            var outcome = await TryAdvanceCheckpointFromCutAsync(partitions, atActivation: true);
            CountCut(allowCatchUp, outcome, _cutDecline, census);
            if (outcome == CutAdvanceOutcome.CatchUp && allowCatchUp)
            {
                _replayReadCap = _cutCatchUpBounds;
            }
        }

        // A checkpoint only moves forward, so the sum rises exactly where one was stamped.
        return SumCurrentCheckpoints(partitions) > before;
    }

    /// <summary>The sum of every partition's current checkpoint.</summary>
    private long SumCurrentCheckpoints(int partitions)
    {
        long sum = 0;
        for (var p = 0; p < partitions; p++)
        {
            sum += GetCurrentCheckpointForPartition(p);
        }

        return sum;
    }

    /// <summary>
    /// Why the latest <see cref="TryAdvanceCheckpointFromCutAsync"/> ended as it did, for the
    /// benchmark census (<see cref="AwaitDiagnostics.Count"/>); a constant, never allocated.
    /// </summary>
    private string _cutDecline = string.Empty;

    /// <summary>
    /// Counts an activation cut declined before it ran, on the benchmark census; a first
    /// pass also names the activation's class on <paramref name="census"/>.
    /// </summary>
    private static void CountCut(bool firstPass, string reason, ActivationReplayCensus? census = null)
    {
        if (AwaitDiagnostics.Enabled)
        {
            AwaitDiagnostics.Count(string.Concat(firstPass ? "leaf.replay.cut.first." : "leaf.replay.cut.final.", reason));
            if (firstPass && census is not null)
            {
                census.Class = reason;
            }
        }
    }

    /// <summary>
    /// Counts an activation cut's outcome and its reason on the benchmark census; a first
    /// pass also names the activation's class on <paramref name="census"/>.
    /// </summary>
    private static void CountCut(bool firstPass, CutAdvanceOutcome outcome, string reason, ActivationReplayCensus? census = null)
    {
        if (AwaitDiagnostics.Enabled)
        {
            var cutClass = string.Concat(outcome.ToString(), ".", reason);
            AwaitDiagnostics.Count(string.Concat(firstPass ? "leaf.replay.cut.first." : "leaf.replay.cut.final.", cutClass));
            if (firstPass && census is not null)
            {
                census.Class = cutClass;
            }
        }
    }

    /// <summary>
    /// Counts, on the benchmark census (#6166), where one partition of an activation's replay
    /// starts: the offsets it owes to the probed head (summed, and bucketed by power of two
    /// so the tail is visible), whether the partition was never persisted - a held birth hint
    /// (#5877, H2) or a leaf no capture covered - and whether a pending advance put the start
    /// past the persisted checkpoint. Called only when the diagnostic is enabled.
    /// </summary>
    private static void CountActivationReplayStart(ActivationCut pass, int partition, long persisted, long start, long? probedHead, ActivationReplayCensus? census)
    {
        var prefix = pass == ActivationCut.Final ? "leaf.replay.activation.final." : "leaf.replay.activation.first.";
        AwaitDiagnostics.Count(string.Concat(prefix, "partitions"));
        if (persisted < 0)
        {
            AwaitDiagnostics.Count(string.Concat(prefix, "never-persisted"));
        }

        if (start > persisted)
        {
            AwaitDiagnostics.Count(string.Concat(prefix, "pending-ahead"));
        }

        if (probedHead is { } head)
        {
            var owed = Math.Max(0, head - 1 - start);
            AwaitDiagnostics.Add(string.Concat(prefix, "owed-offsets"), owed);
            AwaitDiagnostics.Count(string.Concat(prefix, "owed.log2-", (owed == 0 ? 0 : 64 - System.Numerics.BitOperations.LeadingZeroCount((ulong)owed)).ToString(System.Globalization.CultureInfo.InvariantCulture)));
            if (owed > 4096)
            {
                // Which partition a heavy span owes: one shard whose cut head lags shows as one name.
                AwaitDiagnostics.Count(string.Concat(prefix, "owed-heavy.partition-", partition.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                if (pass != ActivationCut.Final && census is not null)
                {
                    census.OwedHeavyPartitions++;
                }
            }
        }
    }

    /// <summary>
    /// One activation replay's benchmark census (#6166), attributed to its first cut's class:
    /// the offsets its passes actually read - a catch-up reads only to its close, where the
    /// owed counts measure to the head - the partitions that owed a heavy span, and the
    /// replay's elapsed time. Allocated once per activation replay, only when the diagnostic is
    /// enabled; otherwise the replay passes <see langword="null"/> and allocates nothing.
    /// </summary>
    private sealed class ActivationReplayCensus
    {
        /// <summary>A replay read past this many offsets, over every partition and pass, counts as heavy.</summary>
        internal const long HeavyReadOffsets = 65_536;

        private readonly long _started = System.Diagnostics.Stopwatch.GetTimestamp();

        /// <summary>The first cut's outcome and reason, as its <c>leaf.replay.cut.first.*</c> name carries them.</summary>
        internal string? Class;

        /// <summary>The offsets read, summed over every partition and pass.</summary>
        internal long ReadOffsets;

        /// <summary>The partitions whose first pass owed more than 4096 offsets to the head.</summary>
        internal int OwedHeavyPartitions;

        /// <summary>Adds one partition's read span; a pass that read nothing adds nothing.</summary>
        internal void AddRead(long offsets)
        {
            if (offsets > 0)
            {
                ReadOffsets += offsets;
            }
        }

        /// <summary>Writes the replay's totals under <c>leaf.replay.activation.by-cut.{class}</c>.</summary>
        internal void Report()
        {
            var prefix = string.Concat("leaf.replay.activation.by-cut.", Class ?? "unclassified");
            AwaitDiagnostics.Count(prefix);
            AwaitDiagnostics.Add(string.Concat(prefix, ".read-offsets"), ReadOffsets);
            AwaitDiagnostics.Add(
                string.Concat(prefix, ".ms"),
                (long)System.Diagnostics.Stopwatch.GetElapsedTime(_started).TotalMilliseconds);
            if (ReadOffsets > HeavyReadOffsets)
            {
                AwaitDiagnostics.Count(string.Concat(prefix, ".read-heavy"));
            }

            if (OwedHeavyPartitions > 0)
            {
                AwaitDiagnostics.Add(string.Concat(prefix, ".owed-heavy-partitions"), OwedHeavyPartitions);
            }
        }
    }

    /// <summary>Counts a stall drive's cut outcome and its reason on the benchmark census.</summary>
    private void CountDriveCut(CutAdvanceOutcome outcome)
    {
        if (AwaitDiagnostics.Enabled)
        {
            AwaitDiagnostics.Count(string.Concat("leaf.drive.cut.", outcome.ToString(), ".", _cutDecline));
        }
    }
}
