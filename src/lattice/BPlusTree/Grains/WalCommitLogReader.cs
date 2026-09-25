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

        // No entry can have an offset greater than long.MaxValue, so an
        // exclusive lower bound there selects nothing. Computing
        // fromOffsetExclusive + 1 would overflow to long.MinValue and,
        // once clamped to 0, wrongly replay the whole log from the head.
        if (fromOffsetExclusive == long.MaxValue)
        {
            yield break;
        }

        // fromOffsetExclusive == -1 means "start at offset 0 inclusive".
        // The WAL grain's ReadAsync takes an inclusive sequence cursor,
        // so the inclusive cursor is fromOffsetExclusive + 1.
        var nextSequence = fromOffsetExclusive + 1;

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
    /// <remarks>
    /// Pushes the filter down to the WAL grain page by page through
    /// <see cref="IWalShardGrain.ReadFilteredAsync"/>, so an excluded payload
    /// is dropped before it crosses the grain boundary - and, with a provider
    /// that implements the filtered read, before it is ever decoded.
    /// <para>
    /// Each page is asked to examine at most the budget that remains, and is
    /// charged for the offset span it covers. The span is an upper bound on what
    /// the page examined, since offsets only ever skip forward, so the whole read
    /// examines no more than <paramref name="maxExamined"/> entries - never
    /// fewer than one while the window holds any.
    /// </para>
    /// <para>
    /// A page ends with a routing-only entry when its last examined entry was
    /// excluded. That entry is held back until the next page is known to be
    /// empty, so only the window's final such entry is delivered and a caller
    /// is not handed the markers of pages that turned out not to be the last.
    /// </para>
    /// </remarks>
    public async IAsyncEnumerable<(long Offset, LatticeMutation Mutation)> ReadFilteredAsync(
        string treeId,
        int shardIndex,
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int maxExamined,
        WalKeyFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (shardIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardIndex), shardIndex, "Shard index must be non-negative.");
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(maxExamined, 1);

        // Same overflow guard as ReadAsync: an exclusive lower bound of
        // long.MaxValue selects nothing, and + 1 would wrap to long.MinValue.
        if (fromOffsetExclusive == long.MaxValue || toOffsetInclusive <= fromOffsetExclusive)
        {
            yield break;
        }

        var grain = grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{shardIndex}");
        var nextSequence = fromOffsetExclusive + 1;
        long remaining = maxExamined;
        var markerPending = false;
        var markerOffset = 0L;
        var marker = default(LatticeMutation);

        while (remaining > 0 && nextSequence <= toOffsetInclusive)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // A plain await, deliberately: this reader runs inside the replay
            // coordinator's grain turn and issues a grain call per page, so each
            // page must resume on the activation's scheduler with its
            // RequestContext intact (see BoundedFanOut and the writer's audit in
            // WalCommitLogWriterConfigureAwaitAuditTests).
            var page = await grain
                .ReadFilteredAsync(nextSequence, toOffsetInclusive, (int)Math.Min(PageSize, remaining), filter, cancellationToken);
            var entries = page.Entries;
            if (entries.Count == 0)
            {
                break;
            }

            // A non-empty page moves past the previous page's marker, so that
            // marker is no longer the window's last examined entry.
            markerPending = false;
            var last = entries.Count - 1;
            for (var i = 0; i <= last; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var sequenced = entries[i];
                var mutation = WalRecordConverter.FromWalRecord(sequenced.Entry);
                if (i == last && filter.Excludes(mutation.Kind, mutation.Key))
                {
                    markerPending = true;
                    markerOffset = sequenced.Sequence;
                    marker = mutation;
                    break;
                }

                yield return (sequenced.Sequence, mutation);
            }

            // A page that fails to advance would repeat itself forever. The WAL
            // grain derives NextSequence from the last entry it returned, so a
            // non-empty page always advances; this guard makes that local.
            if (page.NextSequence <= nextSequence)
            {
                break;
            }

            remaining -= page.NextSequence - nextSequence;
            nextSequence = page.NextSequence;
        }

        if (markerPending)
        {
            yield return (markerOffset, marker);
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
