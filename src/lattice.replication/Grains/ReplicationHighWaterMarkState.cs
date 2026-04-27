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
}
