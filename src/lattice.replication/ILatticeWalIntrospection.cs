using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Sender-side introspection seam over the per-tree write-ahead log.
/// Exposes the oldest still-available WAL entry HLC across every
/// shard, which the receiver-side
/// <see cref="ILatticeFallOffLogDetector"/> consumes to detect the
/// fall-off-the-log condition.
/// <para>
/// Lives on the sender silo and walks each
/// <c>IReplogShardGrain</c> activation backing the named tree;
/// returns the minimum timestamp across the per-shard heads, or
/// <see langword="null"/> when every shard is empty (no WAL entries
/// have been captured yet). The cost is one grain RPC per
/// configured WAL partition (default <c>1</c>); operators can cache
/// the result with bounded staleness without affecting
/// fall-off-detection correctness because the WAL is append-only at
/// the head and trim-only at the tail — a stale "oldest" reading is
/// always older than or equal to the current oldest, never newer.
/// </para>
/// </summary>
public interface ILatticeWalIntrospection
{
    /// <summary>
    /// Returns the oldest still-available WAL entry HLC for
    /// <paramref name="treeName"/>, or <see langword="null"/> when no
    /// WAL entries have been captured yet for any shard. Reads
    /// <see cref="LatticeReplicationOptions.ReplogPartitions"/> from
    /// the per-tree options to determine the shard count.
    /// </summary>
    /// <param name="treeName">
    /// Logical tree id. Must be non-null and non-empty.
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed at every grain hop.</param>
    Task<HybridLogicalClock?> GetOldestAvailableHlcAsync(
        string treeName,
        CancellationToken cancellationToken = default);
}