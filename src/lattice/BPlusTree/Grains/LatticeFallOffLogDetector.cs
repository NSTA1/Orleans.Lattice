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
/// <see cref="IServiceProvider"/> because the replication package
/// (which registers the concrete adapter) is optional. When no
/// reader is registered the detector returns
/// <see cref="FallOffLogDecision.TailReplay"/> unconditionally — without
/// a commit log there is no possibility of fall-off-log and the
/// activation-time replay coordinator returns an empty slice anyway.
/// </para>
/// </summary>
internal sealed class LatticeFallOffLogDetector(IServiceProvider services) : ILatticeFallOffLogDetector
{
    private ICommitLogReader? _reader;

    private ICommitLogReader? Reader => _reader ??= services.GetService<ICommitLogReader>();

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
        if (checkpointOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(checkpointOffset), checkpointOffset, "Checkpoint offset must be non-negative.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        var reader = Reader;
        if (reader is null)
        {
            // No replication package registered — no commit log, no possibility
            // of fall-off-log. The replay coordinator returns EmptySlice in
            // this state, so a tail-replay decision is a vacuous no-op.
            return FallOffLogDecision.TailReplay;
        }

        var head = await reader.GetHeadOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);
        var tail = await reader.GetTailOffsetAsync(treeId, shardIndex, cancellationToken).ConfigureAwait(false);

        // Trigger 1: WAL trimmed past the leaf''s persisted checkpoint.
        var walTrimmedPastCheckpoint = checkpointOffset > 0 && tail > checkpointOffset;

        // Trigger 2: replay budget exceeded.
        var gap = head - checkpointOffset;
        var budgetExceeded = gap > options.MaxLeafReplayEntries;

        // Trigger 3: projection age exceeds retention.
        var ageExceeded = options.LeafProjectionRetention != Timeout.InfiniteTimeSpan
            && checkpointAge > options.LeafProjectionRetention;

        if (!walTrimmedPastCheckpoint && !budgetExceeded && !ageExceeded)
        {
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

