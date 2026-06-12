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

    /// <summary>
    /// Range-scoped overload of
    /// <see cref="ExportAsync(string, HybridLogicalClock, CancellationToken)"/>
    /// that yields only entries whose key falls inside at least one of the
    /// supplied half-open <paramref name="ranges"/>. The scope is the union
    /// of the ranges, compared ordinally with the same half-open
    /// <c>[StartKey, EndKey)</c> semantics as
    /// <see cref="LeafReReplayRange.Contains(string?)"/>, so the export
    /// localises on byte-identical boundaries to the targeted leaf re-replay
    /// repair pass. An empty <paramref name="ranges"/> list scopes to the
    /// union of zero ranges and therefore yields no entries.
    /// <para>
    /// This is the snapshot seam the bootstrap-snapshot fallback uses when a
    /// localised divergence cannot be repaired from the write-ahead log
    /// because the log has been trimmed past the divergence point: it
    /// re-derives the missing committed state of just the divergent leaf
    /// range from the live tree (which is immune to WAL trimming), keeping
    /// the repair cost proportional to the drift rather than the whole tree.
    /// </para>
    /// <para>
    /// The default interface implementation preserves backward compatibility:
    /// it calls the whole-tree
    /// <see cref="ExportAsync(string, HybridLogicalClock, CancellationToken)"/>
    /// overload and filters the returned stream by range on the client side,
    /// so existing <see cref="ISnapshotProvider"/> implementations need no
    /// change. A provider that can push the range bound down into its storage
    /// backend (and so avoid streaming out-of-range entries it then discards)
    /// may override this overload; the <see cref="SnapshotStream.AsOfHlc"/>
    /// and <see cref="SnapshotStream.CausalStableFrontier"/> it returns must
    /// match what the whole-tree export at the same
    /// <paramref name="asOfHlc"/> would report so receivers pin the same
    /// resume cut.
    /// </para>
    /// </summary>
    /// <param name="treeName">
    /// The logical tree id to export. Must be non-null and non-empty.
    /// </param>
    /// <param name="ranges">
    /// The half-open <c>[StartKey, EndKey)</c> covering ranges to scope the
    /// export to. Must be non-null; an empty list yields no entries.
    /// </param>
    /// <param name="asOfHlc">
    /// Strict upper-bound timestamp. Entries with
    /// <see cref="SnapshotEntry.Timestamp"/> &gt; <paramref name="asOfHlc"/>
    /// are excluded. <see cref="HybridLogicalClock.Zero"/> disables the
    /// upper-bound filter.
    /// </param>
    /// <param name="cancellationToken">
    /// Observed during the up-front frontier read and on every yielded entry
    /// from the returned stream.
    /// </param>
    Task<SnapshotStream> ExportAsync(
        string treeName,
        IReadOnlyList<LeafReReplayRange> ranges,
        HybridLogicalClock asOfHlc,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(treeName);
        ArgumentNullException.ThrowIfNull(ranges);
        return ScopedSnapshotStream.CreateAsync(this, treeName, ranges, asOfHlc, cancellationToken);
    }
}
