using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILatticeFallOffLogDetector"/> registered as a
/// singleton when <c>AddLattice</c> wires the leaf-projection replay
/// machinery. Consults <see cref="ICommitLogReader"/> for head and
/// tail offsets and the resolved options for the configured triggers.
/// <para>
/// Three triggers can elect a recovery path: (1) the WAL has been
/// trimmed past the leaf''s persisted checkpoint
/// (<c>tail &gt; checkpoint</c>); (2) the offset gap exceeds
/// <see cref="LatticeOptions.MaxLeafReplayEntries"/>; or
/// (3) the persisted checkpoint is older than
/// <see cref="LatticeOptions.LeafProjectionRetention"/>. When a
/// trigger fires the configured
/// <see cref="LatticeOptions.ProjectionRebuildPolicy"/> selects
/// between snapshot-then-WAL, full WAL rebuild, or surfacing
/// <see cref="LeafProjectionStaleException"/>.
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

        // Trigger 1: WAL trimmed past the leaf''s persisted checkpoint.
        var walTrimmedPastCheckpoint = checkpointOffset > 0 && tail > checkpointOffset;

        // Trigger 2: replay budget exceeded.
        //
        // The "nothing applied" sentinel (-1) skips this trigger: a
        // freshly-created leaf has no in-memory projection state to
        // recover, so the apparent gap (head - -1) overstates the
        // actual work by the full WAL contribution of every sibling
        // leaf in the same shard partition. The per-leaf range filter
        // inside the materialiser (see ShouldApplyDuringReplay) drops
        // every WAL entry that does not fall in this leaf''s
        // [LowKeyInclusive, HighKeyExclusive) range on iteration, so
        // the cost of a tail-replay against a populated WAL is
        // bounded by the leaf''s own range, not by the WAL head.
        //
        // Without this guard, a sibling created mid-run by a split
        // (whose donor''s SetCheckpointOffsetHintAsync may race the
        // sibling''s own OnActivateAsync) reads checkpoint = -1,
        // computes gap = head + 1 against a sibling-populated WAL,
        // trips the budget, and throws LeafProjectionStaleException
        // even though there is nothing to recover. This is the c2-vi
        // production scenario (silo log 20260526-201857Z).
        var gap = head - checkpointOffset;
        var budgetExceeded = checkpointOffset >= 0 && gap > options.MaxLeafReplayEntries;

        // Trigger 3: projection age exceeds retention.
        var ageExceeded = options.LeafProjectionRetention != Timeout.InfiniteTimeSpan
            && checkpointAge > options.LeafProjectionRetention;

        if (!walTrimmedPastCheckpoint && !budgetExceeded && !ageExceeded)
        {
            // No hard trigger fired. Evaluate the SnapshotPending
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

        return options.ProjectionRebuildPolicy switch
        {
            ProjectionRebuildPolicy.SnapshotThenWal => FallOffLogDecision.SnapshotThenWal,
            ProjectionRebuildPolicy.FullRebuildFromWal => FallOffLogDecision.FullRebuildFromWal,
            ProjectionRebuildPolicy.Fail => FallOffLogDecision.Fail,
            _ => FallOffLogDecision.SnapshotThenWal,
        };
    }
}

