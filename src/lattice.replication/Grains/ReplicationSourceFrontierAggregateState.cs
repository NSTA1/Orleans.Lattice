namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persistent state of <see cref="ReplicationSourceFrontierAggregateGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationSourceFrontierAggregateState)]
internal sealed class ReplicationSourceFrontierAggregateState
{
    /// <summary>The highest aggregate generation handed out; never lowered.</summary>
    [Id(0)]
    public long Generation { get; set; }
}
