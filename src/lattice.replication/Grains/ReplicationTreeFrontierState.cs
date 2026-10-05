namespace Orleans.Lattice.Replication.Grains;

/// <summary>Durable state of <see cref="IReplicationTreeFrontierGrain"/> (issue #4586 part 2b).</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationTreeFrontierState)]
internal sealed class ReplicationTreeFrontierState
{
    /// <summary>
    /// The receiver's frontier epoch for the tree, echoed to senders as
    /// <see cref="ReplicationAck.ReceiverLineage"/>. Re-minted on every possible
    /// replacement of the tree's contents; <see cref="Guid.Empty"/> in degraded
    /// mode.
    /// </summary>
    [Id(0)] public Guid Epoch { get; set; }

    /// <summary>The registry lineage observed when <see cref="Epoch"/> was settled.</summary>
    [Id(1)] public Guid? ObservedRegistryLineage { get; set; }

    /// <summary>
    /// A replacement was announced and the registry lineage it produces has not
    /// been observed yet.
    /// </summary>
    [Id(2)] public bool Unsettled { get; set; }

    /// <summary>Per origin that has pushed to the tree.</summary>
    [Id(3)] public Dictionary<string, ReplicationTreeOriginFrontier> Origins { get; set; } = new(StringComparer.Ordinal);
}