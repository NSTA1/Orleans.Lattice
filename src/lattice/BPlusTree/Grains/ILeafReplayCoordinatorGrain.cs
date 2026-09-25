using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A per-shard WAL-read coordinator that amortises commit-log reads across
/// every leaf grain that activates within a single shard. Keyed
/// <c>{treeId}/{shardIndex}</c>. Internal - the leaf grain is the sole
/// caller; the V1 surface does not expose this on any public API.
/// </summary>
[Alias(TypeAliases.ILeafReplayCoordinatorGrain)]
internal interface ILeafReplayCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Reads a slice of the per-shard write-ahead log between
    /// <paramref name="fromOffsetExclusive"/> and <paramref name="toOffsetInclusive"/>,
    /// returning at most <paramref name="budget"/> entries. The coordinator
    /// caches the most recently served slice in-memory so multiple leaves
    /// activating concurrently against the same shard share one underlying
    /// commit-log read.
    /// </summary>
    Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Filtered counterpart of
    /// <see cref="ReadSliceAsync(long, long, int, CancellationToken)"/> for a
    /// leaf that owns <paramref name="filter"/> and rejects every key-scoped
    /// record outside it (issue #3565). The filter is pushed down to storage:
    /// the slice examines at most <paramref name="budget"/> entries of the window
    /// and holds the ones the filter does not exclude, in full, plus the last
    /// examined entry routing-only when it is excluded, as
    /// <see cref="IWalStorageProvider.ReadFilteredAsync"/> documents. The slice
    /// is therefore not offset-dense, but its last entry is still how far the
    /// read got: a caller advancing by it passes every record the filter
    /// dropped, and an empty slice still means the window holds nothing.
    /// <para>
    /// The caller must judge the slice by the same ownership it passed here:
    /// that is what makes every dropped record one it would have rejected
    /// anyway.
    /// </para>
    /// </summary>
    Task<IReadOnlyList<CommitLogSliceEntry>> ReadSliceAsync(
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int budget,
        WalKeyFilter filter,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current head offset of the per-shard write-ahead log -
    /// i.e. the next sequence number that will be assigned to a future append.
    /// </summary>
    Task<long> GetHeadOffsetAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current tail offset of the per-shard write-ahead log -
    /// i.e. the offset of the oldest entry still readable. Equals the head
    /// when the log is empty or has never been trimmed.
    /// </summary>
    Task<long> GetTailOffsetAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// One entry in a commit-log slice returned by
/// <see cref="ILeafReplayCoordinatorGrain.ReadSliceAsync"/>. Named record
/// rather than tuple because Orleans serialisation prefers a stable wire
/// shape.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CommitLogSliceEntry)]
[Immutable]
internal readonly record struct CommitLogSliceEntry(
    [property: Id(0)] long Offset,
    [property: Id(1)] LatticeMutation Mutation);
