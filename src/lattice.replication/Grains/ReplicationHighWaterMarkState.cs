using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="ReplicationHighWaterMarkGrain"/>.
/// Holds the receiver's <em>local vector clock</em> for the tree this
/// grain represents: a sparse <c>{originClusterId &#8594; HybridLogicalClock}</c>
/// map whose diagonal entry per origin is the highest HLC the receiver
/// has applied (or pinned via snapshot handoff) for that
/// <c>(treeId, originClusterId)</c> pair.
/// <para>
/// The vector generalises the per-origin high-water-mark table without
/// breaking the wire: existing receiver paths consult
/// <see cref="VersionVector.GetClock(string)"/> for the diagonal entry
/// (semantically identical to the old per-origin HWM read), while the
/// causal-plus dependency check
/// (<see cref="IReplicationHighWaterMarkGrain.GetVectorAsync"/>) reads
/// the full clock in a single grain call.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationHighWaterMarkState)]
internal sealed class ReplicationHighWaterMarkState
{
    /// <summary>
    /// The receiver's local vector clock for this tree. Initialised to
    /// an empty vector on first activation; the per-origin diagonal
    /// entries are advanced monotonically by
    /// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/> and
    /// replaced unconditionally by the restore re-seed's
    /// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>, and raised
    /// pointwise (never lowered) by the bootstrap handoff's
    /// <see cref="IReplicationHighWaterMarkGrain.MergeBootstrapFrontierAsync"/>.
    /// </summary>
    [Id(0)] public VersionVector Vector { get; set; } = new();

    /// <summary>
    /// The legacy <em>snapshot-pinned floor</em>. Earlier builds wrote the
    /// bootstrap frontier here and dropped every point write at or below it
    /// as already contained in the snapshot. That invariant does not hold
    /// (#4463): the source coordinate is sealed at the maximum HLC anywhere in
    /// the snapshot and a third origin's coordinate is the source's maximum
    /// applied HLC, and neither is downward-closed over what the snapshot
    /// holds, so the floor silently discarded writes the snapshot never
    /// contained - the snapshot-handoff form of #1060.
    /// <para>
    /// This build never reads the floor as a drop threshold, and
    /// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/> clears
    /// it. The slot is kept because it is persisted state (its
    /// <c>[Id]</c> is part of the stored shape) and so a silo still on an
    /// earlier build reads an empty floor, and drops nothing, once a pin by
    /// this build has landed. Duplicate deliveries are absorbed by the
    /// shadow-forward identity cache and the leaf-level per-key merge.
    /// </para>
    /// </summary>
    [Id(1)] public VersionVector PinnedFloor { get; set; } = new();

    /// <summary>
    /// Writes of each origin that this tree acknowledged without applying and
    /// then lost for good (#4603): an operator discarded the dead-lettered
    /// entry. A write in this set can never become visible here, so an entry
    /// that depends on one is never released - it is dead-lettered with
    /// reason <see cref="LatticeReplicationMetrics.ReasonDependencyLost"/>.
    /// Bounded by operator discards; never pruned.
    /// </summary>
    [Id(2)] public Dictionary<string, HashSet<HybridLogicalClock>> Lost { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// The bootstrap drop floor the tree holds (issue #4549), or
    /// <see langword="null"/> when it holds none. Installed from a full
    /// bootstrap's export, cleared with the tree's applied identities on every
    /// replacement of its contents. Legacy state decodes to
    /// <see langword="null"/>, which drops nothing.
    /// </summary>
    [Id(3)] public ReplicationBootstrapFloor? BootstrapFloor { get; set; }

    /// <summary>
    /// The highest bootstrap drop-floor epoch installed on this tree (issue
    /// #4549). Bumped by every install and never lowered, so a write admitted
    /// before an install carries an older epoch than every shard enforces after it.
    /// </summary>
    [Id(4)] public long FloorEpoch { get; set; }
}
