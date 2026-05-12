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
