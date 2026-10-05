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

    /// <summary>
    /// The origin's shipped low watermark over every tree it replicates here,
    /// as of <see cref="AggregateGeneration"/>. Persisted lazily: a stored value
    /// that lags the live one only delays dependents.
    /// </summary>
    [Id(2)] public HybridLogicalClock AggregateLowWatermark { get; set; }

    /// <summary>The origin's aggregate generation <see cref="AggregateLowWatermark"/> was shipped under.</summary>
    [Id(3)] public long AggregateGeneration { get; set; }

    /// <summary>
    /// The oldest aggregate generation still accepted: raised when a tree's cap
    /// lifts to the generation that tree was re-covered in, so an aggregate the
    /// origin computed over that tree's pre-re-stamp coverage, arriving late,
    /// is ignored. Persisted with the lift.
    /// </summary>
    [Id(4)] public long MinGeneration { get; set; }

    /// <summary>
    /// Per tree whose contents changed lineage and that the origin has not yet
    /// re-covered: the ceiling on the effective low watermark. Persisted before
    /// it takes effect.
    /// </summary>
    [Id(5)] public Dictionary<string, HybridLogicalClock> TreeCaps { get; set; } = new(StringComparer.Ordinal);
}
