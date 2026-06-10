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
    /// <see cref="Orleans.Lattice.VersionVector"/> across every consumer
    /// that has reported a vector through the
    /// <see cref="IWalCursorRegistry"/>, with a
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
    /// <para>
    /// <b>Entry ordering and resume semantics.</b> Implementations are
    /// not required to emit entries in <see cref="SnapshotEntry.Timestamp"/>
    /// order - the default <c>LatticeSnapshotProvider</c> emits in
    /// leaf-chain enumeration order. Receivers that resume a partial
    /// drain by passing a non-<see cref="HybridLogicalClock.Zero"/>
    /// <paramref name="asOfHlc"/> as a resume hint must therefore
    /// treat the call as <i>"return every live entry whose timestamp is
    /// ≤ asOfHlc, in any order"</i> - overlap with previously-applied
    /// entries is the receiver's job to dedupe (the per-origin
    /// high-water-mark dedupe in
    /// <c>IReplicationApplier</c> makes any re-applied entry a no-op).
    /// Implementations that can stream in HLC order are encouraged to
    /// do so, since it tightens the resume cursor's effectiveness.
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

    /// <summary>
    /// Cross-cluster overload of <see cref="ExportAsync(string, HybridLogicalClock, CancellationToken)"/>
    /// that carries the sender-cluster identifier the export is captured
    /// on. Receiver-side adapters (notably the cross-cluster
    /// <c>RemoteSnapshotProvider</c>) require <paramref name="sourceClusterId"/>
    /// to address the correct sender peer; intra-cluster providers
    /// (e.g. the default <c>LatticeSnapshotProvider</c>) ignore the
    /// argument and delegate to the two-arg overload.
    /// <para>
    /// The default interface implementation preserves backward
    /// compatibility: existing <see cref="ISnapshotProvider"/>
    /// implementations that only override the two-arg overload continue
    /// to work, and callers that have <paramref name="sourceClusterId"/>
    /// in hand (e.g. the bootstrap coordinator reading
    /// <c>BootstrapCoordinatorState.SourceClusterId</c>) call the
    /// three-arg overload directly so that
    /// <c>RemoteSnapshotProvider</c> never has to recover the sender id
    /// out of band.
    /// </para>
    /// </summary>
    /// <param name="treeName">
    /// The logical tree id to export. Must be non-null and non-empty.
    /// </param>
    /// <param name="sourceClusterId">
    /// The sender-cluster identifier the snapshot is captured on. Must
    /// be non-null and non-empty. Intra-cluster implementations may
    /// ignore this value; cross-cluster implementations use it to
    /// address the correct sender peer.
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
        string sourceClusterId,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceClusterId);
        return ExportAsync(treeName, asOfHlc, cancellationToken);
    }
}
