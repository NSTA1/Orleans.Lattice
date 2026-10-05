namespace Orleans.Lattice.Replication.Grains;

/// <summary>One origin's entry in <see cref="ReplicationTreeFrontierState"/> (issue #4586 part 2b).</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationTreeOriginFrontier)]
internal sealed class ReplicationTreeOriginFrontier
{
    /// <summary>
    /// Every write of the origin to this tree stamped strictly below it is
    /// applied here in the current epoch, unless held or lost. Persisted lazily.
    /// </summary>
    [Id(0)] public HybridLogicalClock LowWatermark { get; set; }

    /// <summary>
    /// The tree's contents were replaced and no full bootstrap has re-seeded it
    /// since: a shipped watermark is ignored until one does.
    /// </summary>
    [Id(1)] public bool AwaitingPin { get; set; }

    /// <summary>
    /// The tree caps the origin's effective aggregate on its frontier until the
    /// origin ships a watermark for the tree in the current epoch.
    /// </summary>
    [Id(2)] public bool Capped { get; set; }
}