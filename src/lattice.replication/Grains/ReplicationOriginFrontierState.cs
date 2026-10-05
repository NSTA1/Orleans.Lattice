namespace Orleans.Lattice.Replication.Grains;

/// <summary>Durable state of <see cref="IReplicationOriginFrontierGrain"/> (issue #4586).</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationOriginFrontierState)]
internal sealed class ReplicationOriginFrontierState
{
    /// <summary>
    /// The origin's writes each source holds without having applied them,
    /// keyed by source (<c>b|{tree}</c> for a causal-apply buffer,
    /// <c>d|{tree}</c> for a dead-letter queue). Bounded by those queues' caps.
    /// </summary>
    [Id(0)] public Dictionary<string, HashSet<HybridLogicalClock>> HeldBySource { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The origin's writes marked lost: acknowledged and never to be applied. Bounded by operator discards and by writes of trees this receiver does not replicate.</summary>
    [Id(1)] public HashSet<HybridLogicalClock> Lost { get; set; } = new();
}
