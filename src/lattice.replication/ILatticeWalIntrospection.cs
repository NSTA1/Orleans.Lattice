using Orleans.Lattice.BPlusTree.Grains;
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
/// <c>IWalShardGrain</c> activation backing the named tree;
/// returns the minimum timestamp across the per-shard heads, or
/// <see langword="null"/> when every shard is empty (no WAL entries
/// have been captured yet). The cost is one grain RPC per
/// configured WAL partition (default <c>1</c>); operators can cache
/// the result with bounded staleness without affecting
/// fall-off-detection correctness because the WAL is append-only at
/// the head and trim-only at the tail - a stale "oldest" reading is
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

    /// <summary>
    /// Returns the oldest still-available WAL entry HLC for
    /// <paramref name="treeName"/> grouped by the authoring origin
    /// cluster id, or an empty map when no WAL entries have been
    /// captured yet. The fall-off-the-log condition is inherently
    /// per data origin - the receiver can only have fallen off the
    /// log of a peer for the entries that peer authored - so a single
    /// origin-agnostic oldest (see
    /// <see cref="GetOldestAvailableHlcAsync"/>) is insufficient: it
    /// conflates origins and produces a false positive whenever the
    /// global-oldest entry's origin differs from the peer being
    /// probed. This variant lets the caller compare each peer's
    /// apply frontier against the oldest retained entry of that same
    /// origin.
    /// <para>
    /// Because applied remote entries are appended to the local WAL
    /// with their authoring origin preserved, the local WAL mirrors
    /// every origin's retained log; a purely local, per-origin
    /// reading is therefore sufficient and needs no remote peer
    /// introspection. The reading scans a bounded head window of
    /// each shard, which under contiguous-prefix trim captures the
    /// oldest retained entry of every origin clustered at the trim
    /// frontier.
    /// </para>
    /// </summary>
    /// <param name="treeName">
    /// Logical tree id. Must be non-null and non-empty.
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed at every grain hop.</param>
    Task<IReadOnlyDictionary<string, HybridLogicalClock>> GetOldestAvailableHlcByOriginAsync(
        string treeName,
        CancellationToken cancellationToken = default);
}