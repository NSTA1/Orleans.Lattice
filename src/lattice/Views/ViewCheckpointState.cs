using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Durable per-view checkpoint persisted by the view maintainer grain. Records,
/// for each source WAL partition, the offset of the last entry applied to the
/// view, plus the <see cref="ILatticeViewProjection.ProjectionVersion"/> the view
/// was built with and the highest source <see cref="HybridLogicalClock"/> applied
/// so far (reported to the WAL cursor registry to pin garbage collection).
/// <para>
/// On activation the maintainer compares the persisted
/// <see cref="ProjectionVersion"/> against the live projection's version; a
/// mismatch means the projection logic changed and the view is rebuilt from the
/// current source state.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewCheckpointState)]
internal sealed class ViewCheckpointState
{
    /// <summary>
    /// Last applied WAL offset per source partition index. An absent partition
    /// (or value <c>-1</c>) means nothing has been applied from that partition;
    /// the next read uses the value as the exclusive lower bound.
    /// </summary>
    [Id(0)]
    public Dictionary<int, long> AppliedOffsets { get; set; } = new();

    /// <summary>
    /// The projection version the view was built with. Empty until the first
    /// successful activation. A mismatch with the live projection triggers a
    /// rebuild.
    /// </summary>
    [Id(1)]
    public string ProjectionVersion { get; set; } = string.Empty;

    /// <summary>
    /// The highest source HLC the maintainer has applied to the view, reported
    /// to the WAL cursor registry so the source WAL is not trimmed past it.
    /// </summary>
    [Id(2)]
    public HybridLogicalClock HighestAppliedTimestamp { get; set; }

    /// <summary>
    /// Monotonic rebuild generation, bumped once per in-place rebuild. It seeds
    /// the deterministic idempotency key of an aggregation view's atomic
    /// membership + accumulator flip, so a crash-replay of a normal drain reuses
    /// the same key (and dedups), while a rebuild - which clears the view tree
    /// but not the completed sagas (they are retained for up to
    /// <see cref="LatticeOptions.AtomicWriteRetention"/>) - mints fresh keys and
    /// therefore re-applies from scratch rather than re-attaching to the
    /// pre-rebuild sagas of the now-deleted rows. Unused by the filter /
    /// re-project view kind.
    /// </summary>
    [Id(3)]
    public long RebuildGeneration { get; set; }

    /// <summary>
    /// The durable, monotonically-increasing <em>active generation</em> of the
    /// view tree. The live view tree id is generation-addressed: generation
    /// <c>0</c> maps to the legacy <c>view-{name}</c> id (so an already-materialised
    /// view keeps its tree across an upgrade), and every generation greater than
    /// <c>0</c> is suffixed <c>view-{name}#g{N}</c>. A shadow-swap rebuild builds
    /// into generation <c>N+1</c> and, on completion, flips this field to
    /// <c>N+1</c> in the same durable write that advances the checkpoint - the
    /// atomic swap. Readers resolve the view tree through this field, so they flip
    /// from the old fully-built tree to the new one with no empty window. A crash
    /// before the swap leaves the prior generation active and the orphaned shadow
    /// is overwritten by the next rebuild attempt.
    /// </summary>
    [Id(4)]
    public long ActiveGeneration { get; set; }

    /// <summary>
    /// <see langword="true"/> when an old generation's tree is awaiting reclamation
    /// after a swap. The reclamation is deferred (rather than performed inline with
    /// the swap) so a reader still holding the prior generation's cached tree id
    /// during the brief post-swap staleness window reads a fully-built - if
    /// slightly stale - tree, never a deleted one.
    /// </summary>
    [Id(5)]
    public bool HasPendingReclaim { get; set; }

    /// <summary>
    /// The generation whose tree is awaiting reclamation when
    /// <see cref="HasPendingReclaim"/> is set. Resolved to a tree id through the
    /// same generation-addressing scheme as <see cref="ActiveGeneration"/>.
    /// </summary>
    [Id(6)]
    public long PendingReclaimGeneration { get; set; }

    /// <summary>
    /// Absolute UTC tick at or after which the <see cref="PendingReclaimGeneration"/>
    /// tree may be deleted. Set on swap to <c>now + reclaim-grace</c>, where the
    /// grace comfortably exceeds the read handle's active-tree cache lifetime so no
    /// reader can still be resolving the reclaimed generation.
    /// </summary>
    [Id(7)]
    public long ReclaimEligibleAtTicks { get; set; }

    /// <summary>
    /// The physical tree id the view is currently bound to and tailing. A source
    /// tree's logical id (the projection's configured source) is resolved to a
    /// physical id through the registry alias; a shadow-cutover restore, a tree
    /// resize, or a reshard can repoint that alias at a new physical tree whose
    /// write-ahead log is addressed under the new physical id. The maintainer
    /// records the physical identity it last bound to here and, on each drain,
    /// compares it against the freshly-resolved physical id: a mismatch means the
    /// source identity was swapped underneath the alias, so the view resets its
    /// per-partition offsets, rebuilds from the new physical source, re-pins the
    /// WAL cursor under the new id, and updates this field - all in one heal.
    /// Empty until the first activation binds it.
    /// </summary>
    [Id(8)]
    public string BoundPhysicalTreeId { get; set; } = string.Empty;

    /// <summary>
    /// The highest generation whose view tree is addressed through the legacy
    /// generation separator, or <see langword="null"/> when this has never been
    /// determined.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The generation suffix moved to a storage-safe separator because the view
    /// tree id is an Orleans grain primary key and is carried into
    /// <c>ShardRootGrain</c>'s composite key - a persistent grain - where keyed
    /// storage backends reject the old character. Rewriting the separator
    /// outright would have orphaned the data of every view already past
    /// generation 0, whose live tree sits under the old id.
    /// </para>
    /// <para>
    /// Instead the ceiling is pinned once, on the first activation after the
    /// upgrade, to the generation then active: every generation at or below it
    /// keeps resolving through the legacy separator, so no existing tree is
    /// stranded, and the next rebuild allocates a higher generation that uses the
    /// storage-safe one. The view therefore heals itself with no operator action
    /// and no forced rebuild, and the old generation is still reclaimed on the
    /// normal grace cadence.
    /// </para>
    /// <para>
    /// A view created after the upgrade pins <c>0</c>, and generation 0 carries no
    /// suffix at all, so every suffixed generation it ever allocates is
    /// storage-safe. Pinning is idempotent and safe to interrupt: it is derived
    /// from the active generation and recomputed identically if the write is lost.
    /// </para>
    /// </remarks>
    [Id(9)]
    public long? LegacyGenerationCeiling { get; set; }
}
