namespace Orleans.Lattice.Replication.Grains;

/// <summary>A read of <see cref="ICrossTreeDecisionSequenceGrain"/>.</summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.CrossTreeDecisionSequenceSnapshot)]
internal sealed record CrossTreeDecisionSequenceSnapshot(
    [property: Id(0)] long Counter,
    [property: Id(1)] long? MinPending);
