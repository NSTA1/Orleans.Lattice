namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A read of a receiver tree's applied low watermarks (issue #4586 part 2b), for
/// the snapshot export and the tombstone reap gate.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationTreeFrontierSnapshot)]
internal sealed record ReplicationTreeFrontierSnapshot
{
    /// <summary>The frontier epoch, <see cref="Guid.Empty"/> in degraded mode.</summary>
    [Id(0)] public Guid Epoch { get; init; }

    /// <summary>The registry lineage the watermarks hold under, <see langword="null"/> when none is tracked.</summary>
    [Id(1)] public Guid? RegistryLineage { get; init; }

    /// <summary>
    /// Per origin, the low watermark: every write of the origin to the tree
    /// stamped strictly below it is applied here, unless held or lost. Only
    /// origins with a valid watermark are listed; none in degraded mode.
    /// </summary>
    [Id(2)] public IReadOnlyDictionary<string, HybridLogicalClock> LowWatermarks { get; init; } =
        new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
}