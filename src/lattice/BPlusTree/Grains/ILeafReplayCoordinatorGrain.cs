using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A per-shard WAL-read coordinator that amortises commit-log reads across
/// every leaf grain that activates within a single shard. Keyed
/// <c>{treeId}/{shardIndex}</c>. Internal - the leaf grain is the sole
/// caller; the V1 surface does not expose this on any public API.
/// <para>
/// <b>Every method interleaves (issue #5877).</b> One activation serves every
/// leaf replaying a WAL partition, and a slice read spans many storage round
/// trips, so a non-reentrant coordinator let a silo serve at most one slice read
/// per partition at a time whatever its replay permit count: raising the permits
/// from 4 to 64 on a cold restart bought 4.7 times the storage reads and made
/// every replay about 2.5 times slower. Interleaving is safe because the
/// activation's only state is the lazily parsed key binding and the last-served
/// slice cache. The binding is parsed and assigned in one synchronous segment and
/// is the same for every call. The cache is read and returned with no await
/// between the check and the return, and written - all five fields together - in
/// the synchronous segment after a read's await, so no call reads or writes it
/// across an await. Two interleaved reads therefore cannot tear an entry; the
/// later writer wins, which is the cache's behaviour under serial calls too. The
/// map of slice reads in flight, which lets an identical request join a read in
/// storage rather than start a second one, is looked up and inserted in one
/// synchronous segment and cleared when the read ends.
/// </para>
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
    [AlwaysInterleave]
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
    [AlwaysInterleave]
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
    [AlwaysInterleave]
    Task<long> GetHeadOffsetAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current tail offset of the per-shard write-ahead log -
    /// i.e. the offset of the oldest entry still readable. Equals the head
    /// when the log is empty or has never been trimmed.
    /// </summary>
    [AlwaysInterleave]
    Task<long> GetTailOffsetAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Declares that the leaf <paramref name="reader"/> is queued to replay this
    /// partition after <paramref name="nextExclusive"/> (issue #5877), so the
    /// replay page cache keeps the pages it will read. Advisory and leased: it
    /// orders eviction only, and lapses unless the reader reads or declares again.
    /// </summary>
    [AlwaysInterleave]
    [OneWay]
    Task DeclareReplayDemandAsync(string reader, long nextExclusive);

    /// <summary>
    /// Withdraws <paramref name="reader"/>'s declared demand on this partition:
    /// its replay ended without reading it to the head.
    /// </summary>
    [AlwaysInterleave]
    [OneWay]
    Task ClearReplayDemandAsync(string reader);
}

/// <summary>
/// One entry in a commit-log slice returned by
/// <see cref="Orleans.Lattice.BPlusTree.Grains.ILeafReplayCoordinatorGrain.ReadSliceAsync(long, long, int, System.Threading.CancellationToken)"/>. Named record
/// rather than tuple because Orleans serialisation prefers a stable wire
/// shape.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.CommitLogSliceEntry)]
[Immutable]
internal readonly record struct CommitLogSliceEntry(
    [property: Id(0)] long Offset,
    [property: Id(1)] LatticeMutation Mutation);
