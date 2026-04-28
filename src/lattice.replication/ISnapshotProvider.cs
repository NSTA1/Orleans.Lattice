using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Streaming as-of-HLC export of a tree's primary state. The
/// snapshot/bootstrap protocol uses this seam to seed a newly-joining
/// peer (or a long-offline peer that has fallen off the WAL) before
/// switching it to incremental replication.
/// <para>
/// The interface is deliberately neutral about the consumer: today
/// the cross-cluster bootstrap state machine drains it; the same shape
/// satisfies in-cluster recovery flows that need a streaming "as-of"
/// view of the primary tree (for example, disaster-recovery
/// re-population of a damaged shard, or re-seeding a downstream
/// projection). Hosts that need a more efficient export against a
/// specific storage backend can register their own
/// <see cref="ISnapshotProvider"/> via DI before calling
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// </para>
/// </summary>
public interface ISnapshotProvider
{
    /// <summary>
    /// Produces a streaming export of <paramref name="treeName"/>'s
    /// primary state as of <paramref name="asOfHlc"/>. The returned
    /// <see cref="SnapshotStream"/> carries the as-of HLC, the
    /// producer's causal-stable frontier (the pointwise minimum
    /// <see cref="Primitives.VersionVector"/> across every consumer
    /// that has reported a vector through the
    /// <see cref="ILatticeReplicationCursorRegistry"/>, with a
    /// fallback to the producer's per-tree local vector clock when no
    /// consumer has reported a vector yet), and an async stream of
    /// every live entry whose
    /// <see cref="SnapshotEntry.Timestamp"/> is less than or equal to
    /// <paramref name="asOfHlc"/>.
    /// <para>
    /// Pass <see cref="HybridLogicalClock.Zero"/> as
    /// <paramref name="asOfHlc"/> to include every live entry
    /// regardless of timestamp - the common case when seeding a fresh
    /// peer that has no incremental cursor yet.
    /// </para>
    /// </summary>
    /// <param name="treeName">
    /// The logical tree id to export. Must be non-null and non-empty.
    /// </param>
    /// <param name="asOfHlc">
    /// Strict upper-bound timestamp. Entries with
    /// <see cref="SnapshotEntry.Timestamp"/> &gt; <paramref name="asOfHlc"/>
    /// are excluded. <see cref="HybridLogicalClock.Zero"/> disables
    /// the upper-bound filter.
    /// </param>
    /// <param name="cancellationToken">
    /// Observed during the up-front frontier read and on every yielded
    /// entry from the returned stream.
    /// </param>
    Task<SnapshotStream> ExportAsync(
        string treeName,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default);
}
