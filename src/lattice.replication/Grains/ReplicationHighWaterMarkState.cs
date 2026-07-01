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
    /// replaced unconditionally by
    /// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>.
    /// </summary>
    [Id(0)] public VersionVector Vector { get; set; } = new();

    /// <summary>
    /// The receiver's <em>snapshot-pinned causal floor</em> for this
    /// tree: the per-origin frontier established by the most recent
    /// <see cref="IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
    /// (bootstrap-snapshot handoff or operator rollback re-pin).
    /// <para>
    /// Unlike <see cref="Vector"/> - which is also advanced
    /// incrementally by
    /// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/> as
    /// steady-state entries apply - this floor is written
    /// <em>only</em> by a snapshot pin and therefore represents a true
    /// causal cut: every entry from an origin whose source HLC is at or
    /// below this floor is provably contained in the pinned snapshot.
    /// The receiver uses it (and only it) as the drop criterion for
    /// point writes. The incremental diagonal in <see cref="Vector"/>
    /// must not be used as a drop criterion because the per-origin HLC
    /// is non-monotonic in WAL-append order (per-leaf clocks
    /// interleaved by key-hash partition), so a below-diagonal entry to
    /// a distinct key is routinely a genuinely-new write rather than a
    /// duplicate - dropping it silently strands data (#1060).
    /// </para>
    /// <para>
    /// Empty on first activation (no snapshot pinned): the floor is
    /// <see cref="HybridLogicalClock.Zero"/> for every origin, so
    /// nothing is dropped and at-most-once is upheld by the leaf-level
    /// per-key LWW guard plus the shadow-forward identity cache.
    /// </para>
    /// </summary>
    [Id(1)] public VersionVector PinnedFloor { get; set; } = new();
}
