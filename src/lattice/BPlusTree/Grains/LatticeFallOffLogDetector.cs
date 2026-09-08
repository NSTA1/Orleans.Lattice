using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILatticeFallOffLogDetector"/> registered as a
/// singleton when <c>AddLattice</c> wires the leaf-projection replay
/// machinery. Consults <see cref="ICommitLogReader"/> for head and
/// tail offsets and the resolved options for the configured triggers.
/// <para>
/// Three triggers fire here, but only ONE of them routes to a
/// recovery path, and the distinction is load-bearing (issue #2275).
/// </para>
/// <para>
/// (1) GENUINE LOSS: the WAL has been trimmed past the leaf's
/// persisted checkpoint (<c>tail &gt; checkpoint + 1</c>). This is the
/// only trigger that consults
/// <see cref="LatticeOptions.ProjectionRebuildPolicy"/>, which selects
/// between snapshot-then-WAL, full WAL rebuild, or surfacing
/// <see cref="LeafProjectionStaleException"/>.
/// </para>
/// <para>
/// (2) the partition-wide offset gap exceeds
/// <see cref="LatticeOptions.MaxLeafReplayEntries"/>, and (3) the
/// persisted checkpoint is older than
/// <see cref="LatticeOptions.LeafProjectionRetention"/>. These are
/// COST signals against an INTACT WAL. They do not elect a recovery
/// path and they never consult the rebuild policy: both return
/// <see cref="FallOffLogDecision.TailReplayOverBudget"/> and the leaf
/// tail-replays regardless, because a slow activation is recoverable
/// and a bricked tree is not (issue #1738).
/// </para>
/// <para>
/// WHAT CONSUMES <see cref="FallOffLogDecision.TailReplayOverBudget"/>:
/// in production, nothing. <c>BPlusLeafGrain</c>'s activation switch
/// matches the value and falls straight through to the replay that
/// would have run anyway (issue #2291 removed the stall-check gate it
/// used to feed). The over-budget WARNING and the
/// <c>activation_replays_over_budget</c> counter are raised downstream
/// in <c>ReplayPartitionAsync</c> off <c>appliedEntries</c>, this
/// leaf's exact post-filter count, on every replay path and NOT off
/// this decision (issue #2149). Deleting trigger 2 would not change
/// which activations warn. Do not describe the gap comparison as a
/// candidate the leaf later "confirms": no such handshake exists.
/// </para>
/// <para>
/// The ONE surviving side effect, which is incidental rather than
/// designed: an over-budget or over-age leaf returns before the
/// <see cref="LatticeOptions.LeafSnapshotMargin"/> proximity check
/// below, so it never yields the
/// <see cref="FallOffLogDecision.SnapshotPending"/> advisory. A leaf
/// whose partition gap sits over budget therefore does not signal the
/// maintenance grain to snapshot, which is the opposite of what its
/// depth suggests it should do. Recorded, not fixed, because it is a
/// behavioural change and this pass is documentation only.
/// </para>
/// <para>
/// <see cref="ICommitLogReader"/> is resolved lazily via
/// <see cref="IServiceProvider"/>; <c>AddLattice</c> registers
/// <see cref="WalCommitLogReader"/> as the in-core default, so the reader
/// is always available at runtime.
/// </para>
/// </summary>
internal sealed class LatticeFallOffLogDetector(IServiceProvider services) : ILatticeFallOffLogDetector
{
    private ICommitLogReader? _reader;

    private ICommitLogReader Reader => _reader ??= services.GetRequiredService<ICommitLogReader>();

    /// <inheritdoc />
    public async Task<FallOffLogDecision> ClassifyAsync(
        string treeId,
        int shardIndex,
        long checkpointOffset,
        TimeSpan checkpointAge,
        ResolvedLatticeOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(options);
        if (shardIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardIndex), shardIndex, "Shard index must be non-negative.");
        }
        if (checkpointOffset < -1)
        {
            throw new ArgumentOutOfRangeException(nameof(checkpointOffset), checkpointOffset,
                "Checkpoint offset must be >= -1 (the 'nothing applied' sentinel) or a real WAL offset.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        var reader = Reader;

        var head = await reader.GetHeadOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
        var tail = await reader.GetTailOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);

        // Trigger 1: WAL trimmed past the leaf's persisted checkpoint.
        //
        // The loss boundary is tail > checkpoint + 1, NOT the looser
        // tail > checkpoint. checkpoint is the last-APPLIED offset, so the
        // first offset this leaf still needs is checkpoint + 1; the entry AT
        // the checkpoint is already applied and harmless to lose. When
        // tail == checkpoint + 1 only that already-applied entry was trimmed
        // and the entire needed (checkpoint, head] window survives - a clean
        // tail replay, never a rebuild. This is exactly the boundary the
        // activation-time #945 loss guard uses
        // (BPlusLeafGrain.Activation.cs: "tail > persistedCheckpoint + 1");
        // the two must agree or the guard passes a shape the detector then
        // rejects. The coverage-gated WAL GC makes this boundary reachable in
        // steady state: the offset floor deliberately permits trimming the
        // already-applied checkpoint entry once a snapshot covers the prefix
        // (see LatticeWalGc.TrimShardAsync: only offsets strictly ABOVE the
        // floor are protected), so a snapshot-covered leaf routinely settles at
        // tail == checkpoint + 1 and must cold-restart via a clean tail replay,
        // not throw LeafProjectionStaleException. Genuine loss (the first
        // needed offset itself fell off) is still tail > checkpoint + 1.
        var walTrimmedPastCheckpoint = checkpointOffset > 0 && tail > checkpointOffset + 1;

        // Trigger 2: replay budget COST SIGNAL.
        //
        // (Named a CANDIDATE until issue #2275. Nothing confirms it; see the
        // class remarks and the note below.)
        //
        // Units (issue #2149). `head` is per (treeId, shardIndex) - per WAL
        // PARTITION, shared by every leaf pinned to it - and the gap is
        // therefore a partition-wide, PRE-filter extent. MaxLeafReplayEntries
        // is a PER-LEAF, POST-filter budget: "the number of entries a leaf
        // grain expects to replay through its projection rebuild seam
        // (ILeafProjection.Apply)". Those are different units, so
        // `gap > MaxLeafReplayEntries` is NOT a measurement of this leaf's
        // work and must never be reported as one. Doing so produced 19,639
        // warnings in 6.26 hours on a deployed box whose real per-leaf work
        // was one to two orders of magnitude BELOW budget, at a measured
        // fan-out of ~1,350 leaves per partition.
        //
        // What the comparison IS, and why it is kept: every entry this leaf
        // applies is one of the entries in (checkpoint, head], so
        //
        //     appliedByThisLeaf <= gap
        //
        // always holds. The gap is therefore a SOUND UPPER BOUND on the
        // per-leaf work. That arithmetic is unchanged and still true.
        //
        // WHAT IS NO LONGER TRUE is that anything acts on it (issue #2275,
        // after #2291). This trigger used to elect a CANDIDATE that the leaf
        // confirmed downstream, and that phrasing survived here after the
        // mechanism was removed. Today BOTH outcomes of the comparison lead
        // to the same place: gap <= budget yields TailReplay and gap > budget
        // yields TailReplayOverBudget, and the leaf's activation switch does
        // nothing with either - it tail-replays. The over-budget verdict is
        // computed independently in ReplayPartitionAsync, which counts the
        // entries that actually pass ShouldApplyDuringReplay during the
        // replay it is performing anyway (one increment per applied entry, no
        // extra read) and warns when that exact count crosses the budget. It
        // does that on every replay path, whether or not this trigger fired,
        // so the set of activations that warn is identical with this trigger
        // and without it.
        //
        // The comparison is kept for the reason below (a pre-check that
        // scanned the window would cost what the replay costs) plus its one
        // incidental effect, documented on the class: firing it skips the
        // SnapshotPending advisory. If you are tempted to restore a
        // confirm-the-candidate handshake, note that the downstream count is
        // already exact and already unconditional; the candidate would add a
        // second quantity to keep in step, which is what #2149 and #2291 each
        // had to unpick.
        //
        // Confirming here instead would mean scanning (checkpoint, head]
        // before the replay - the same read the replay then repeats, and the
        // read whose 30 s overrun is the livelock of issue #2165. A pre-check
        // that costs as much as the work it is checking is not a pre-check.
        //
        // The "nothing applied" sentinel (-1) skips this trigger: a
        // freshly-created leaf has no in-memory projection state to
        // recover, so the apparent gap (head - -1) overstates the
        // actual work by the full WAL contribution of every sibling
        // leaf in the same shard partition. The per-leaf range filter
        // inside the materialiser (see ShouldApplyDuringReplay) drops
        // every WAL entry that does not fall in this leaf's
        // [LowKeyInclusive, HighKeyExclusive) range on iteration, so
        // the cost of a tail-replay against a populated WAL is
        // bounded by the leaf's own range, not by the WAL head.
        //
        // That reasoning is fully general and is exactly what the units
        // correction above generalises it to; the sentinel guard remains
        // because it is also load-bearing for a second reason. Without it, a
        // sibling created mid-run by a split (whose donor's
        // SetCheckpointOffsetHintAsync may race the sibling's own
        // OnActivateAsync) reads checkpoint = -1, computes gap = head + 1
        // against a sibling-populated WAL, trips the budget, and throws
        // LeafProjectionStaleException even though there is nothing to
        // recover. This is the c2-vi production scenario (silo log
        // 20260526-201857Z).
        var gap = head - checkpointOffset;
        var budgetExceeded = checkpointOffset >= 0 && gap > options.MaxLeafReplayEntries;

        // Trigger 3: projection age exceeds retention.
        var ageExceeded = options.LeafProjectionRetention != Timeout.InfiniteTimeSpan
            && checkpointAge > options.LeafProjectionRetention;

        if (!walTrimmedPastCheckpoint && !budgetExceeded && !ageExceeded)
        {
            // No trigger fired. Evaluate the SnapshotPending
            // advisory: when the leaf's checkpoint sits within the
            // trailing LeafSnapshotMargin fraction of the readable WAL
            // window, signal the maintenance grain to capture a
            // snapshot before the WAL trims past the checkpoint.
            // Activation-time behaviour is identical to TailReplay -
            // the advisory only steers the maintenance probe.
            if (options.LeafSnapshotMargin > 0.0
                && checkpointOffset >= 0
                && head > 0
                && head > tail
                && checkpointOffset >= tail
                && checkpointOffset <= head)
            {
                var window = (double)(head - tail);
                var proximity = (checkpointOffset - tail) / window;
                if (proximity <= options.LeafSnapshotMargin)
                {
                    return FallOffLogDecision.SnapshotPending;
                }
            }

            return FallOffLogDecision.TailReplay;
        }

        // Only GENUINE LOSS routes to the configured rebuild policy (every
        // branch of which is currently fatal). The WAL no longer holds an
        // offset this leaf needs, so replaying the surviving suffix would
        // rebuild the leaf over the lost prefix and advance the materialiser
        // pin past unrecoverable data - the #945 laundering hazard the
        // activation-time guard also defends.
        if (walTrimmedPastCheckpoint)
        {
            return options.ProjectionRebuildPolicy switch
            {
                ProjectionRebuildPolicy.SnapshotThenWal => FallOffLogDecision.SnapshotThenWal,
                ProjectionRebuildPolicy.FullRebuildFromWal => FallOffLogDecision.FullRebuildFromWal,
                ProjectionRebuildPolicy.Fail => FallOffLogDecision.Fail,
                _ => FallOffLogDecision.SnapshotThenWal,
            };
        }

        // A COST trigger fired (replay budget or projection age) but the WAL
        // still covers the entire (checkpoint, head] window, so a plain tail
        // replay reconstructs exactly the same projection - it is merely
        // longer than the configured budget wanted. Returning a fatal
        // decision here is what bricked a tree holding fully intact data in
        // issue #1738 (gap 10,648 against a 10,000 budget, nothing trimmed).
        //
        // Cost is bounded on the read side instead of by refusing the work:
        // ReplayPartitionAsync honours WalReplayMaxRecordsPerTurn (yielding
        // between turns so a long replay never monopolises the scheduler) and
        // the activation replay permit (#1030) caps concurrent replays. A slow
        // activation is recoverable; a bricked tree is not.
        return FallOffLogDecision.TailReplayOverBudget;
    }
}

