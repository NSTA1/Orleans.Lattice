using System.Runtime.CompilerServices;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ICommitLogReader"/> registered by
/// <see cref="Orleans.Lattice.LatticeServiceCollectionExtensions.AddLattice"/>.
/// Wraps <see cref="IWalShardGrain.ReadAsync"/> with paginated
/// streaming and translates each <see cref="WalRecord"/> back to the
/// public <see cref="LatticeMutation"/> shape via
/// <see cref="WalRecordConverter"/>.
/// <para>
/// <b>Dormancy.</b> the dormant seam registers this adapter but no foreground
/// site invokes <see cref="ReadAsync"/>. the future replay coordinator's per-shard replay
/// coordinator drives it when a leaf grain activates with a stale
/// projection-checkpoint offset.
/// </para>
/// </summary>
internal sealed class WalCommitLogReader(IGrainFactory grainFactory) : ICommitLogReader
{
    /// <summary>Maximum entries requested per <see cref="IWalShardGrain.ReadAsync"/> call.</summary>
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

        var grain = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{shardIndex}");

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
                yield return (sequencedEntry.Sequence, WalRecordConverter.FromWalRecord(sequencedEntry.Entry));
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
        var grain = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{shardIndex}");
        return grain.GetNextSequenceAsync(cancellationToken).AsTask();
    }

    /// <inheritdoc />
    public async Task<long> GetTailOffsetAsync(
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
        var grain = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{shardIndex}");

        // Probe for the oldest readable entry by asking for a single
        // entry from sequence 0. If the WAL has been trimmed, the page
        // will yield the first surviving entry whose sequence is > 0.
        var page = await grain.ReadAsync(0, 1, cancellationToken).ConfigureAwait(false);
        if (page.Entries.Count == 0)
        {
            // Empty (or fully trimmed) WAL - tail collapses to head so
            // a checkpoint at head is not flagged as fallen-off.
            return await grain.GetNextSequenceAsync(cancellationToken).ConfigureAwait(false);
        }

        return page.Entries[0].Sequence;
    }
}
