using System.Runtime.CompilerServices;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Adapters;

/// <summary>
/// Default <see cref="ICommitLogReader"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions})"/>.
/// Wraps <see cref="IReplogShardGrain.ReadAsync"/> with paginated
/// streaming and translates each <see cref="ReplogEntry"/> back to the
/// public <see cref="LatticeMutation"/> shape via
/// <see cref="ReplogEntryConverter"/>.
/// <para>
/// <b>Dormancy.</b> the dormant seam registers this adapter but no foreground
/// site invokes <see cref="ReadAsync"/>. the future replay coordinator''s per-shard replay
/// coordinator drives it when a leaf grain activates with a stale
/// projection-checkpoint offset.
/// </para>
/// </summary>
internal sealed class ReplicationCommitLogReader(IGrainFactory grainFactory) : ICommitLogReader
{
    /// <summary>Maximum entries requested per <see cref="IReplogShardGrain.ReadAsync"/> call.</summary>
    private const int PageSize = 256;

    /// <inheritdoc />
    public async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (shardIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardIndex), shardIndex, "Shard index must be non-negative.");
        }

        var grain = grainFactory.GetGrain<IReplogShardGrain>($"{treeId}/{shardIndex}");

        // fromOffsetExclusive == -1 means "start at offset 0 inclusive".
        // The WAL grain's ReadAsync takes an inclusive sequence cursor,
        // so the inclusive cursor is fromOffsetExclusive + 1.
        var nextSequence = fromOffsetExclusive + 1;
        if (nextSequence < 0)
        {
            nextSequence = 0;
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var page = await grain.ReadAsync(nextSequence, PageSize, cancellationToken).ConfigureAwait(false);
            if (page.Entries.Count == 0)
            {
                yield break;
            }

            foreach (var sequencedEntry in page.Entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return (sequencedEntry.Sequence, ReplogEntryConverter.FromReplogEntry(sequencedEntry.Entry));
            }

            nextSequence = page.NextSequence;
        }
    }

    /// <inheritdoc />
    public Task<long> GetHeadOffsetAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (shardIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardIndex), shardIndex, "Shard index must be non-negative.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        var grain = grainFactory.GetGrain<IReplogShardGrain>($"{treeId}/{shardIndex}");
        return grain.GetNextSequenceAsync(cancellationToken);
    }
}
