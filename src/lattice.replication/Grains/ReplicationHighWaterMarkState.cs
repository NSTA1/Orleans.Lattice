using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="ReplicationHighWaterMarkGrain"/>.
/// Carries the highest <see cref="HybridLogicalClock"/> the receiver
/// has applied (or pinned via snapshot handoff) for the
/// <c>(treeId, originClusterId)</c> pair this grain represents.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationHighWaterMarkState)]
internal sealed class ReplicationHighWaterMarkState
{
    /// <summary>
    /// The highest applied (or pinned) HLC. Defaults to
    /// <see cref="HybridLogicalClock.Zero"/> on first activation;
    /// monotonically non-decreasing afterwards.
    /// </summary>
    [Id(0)] public HybridLogicalClock HighWaterMark { get; set; } = HybridLogicalClock.Zero;
}
