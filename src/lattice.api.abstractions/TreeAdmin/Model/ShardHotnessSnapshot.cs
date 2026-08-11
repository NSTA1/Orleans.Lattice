namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// A single physical shard's read/write hotness sample, carried as one row of a
/// <see cref="TreeHotnessReport"/>. An aggregate per shard, never a per-key
/// metric. The counters are volatile: they reset when the shard grain
/// deactivates, so a low sample can mean either a cold shard or a recently
/// re-activated one.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.ShardHotnessSnapshot)]
[Immutable]
public sealed record ShardHotnessSnapshot
{
    /// <summary>Zero-based physical shard index.</summary>
    [Id(0)] public int ShardIndex { get; init; }

    /// <summary>Read operations processed since the shard grain activated.</summary>
    [Id(1)] public long Reads { get; init; }

    /// <summary>Write operations processed since the shard grain activated.</summary>
    [Id(2)] public long Writes { get; init; }

    /// <summary>
    /// Observed operations-per-second for the shard, computed as
    /// <c>(Reads + Writes) / WindowSeconds</c>.
    /// </summary>
    [Id(3)] public double OpsPerSecond { get; init; }

    /// <summary>
    /// Wall-clock seconds over which <see cref="Reads"/> and <see cref="Writes"/>
    /// accumulated (the elapsed time since the shard grain activated).
    /// </summary>
    [Id(4)] public double WindowSeconds { get; init; }
}
