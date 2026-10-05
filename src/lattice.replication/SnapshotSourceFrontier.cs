using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The source tree's applied frontier carried by a full snapshot export
/// (issue #4586 part 2b): per origin, the applied low watermark and the writes
/// below it the source held without applying (lost marks included), read under
/// <see cref="Lineage"/>.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.SnapshotSourceFrontier)]
internal sealed record SnapshotSourceFrontier
{
    /// <summary>The source tree lineage the frontier was read under, or <see langword="null"/> when unknown.</summary>
    [Id(0)] public Guid? Lineage { get; init; }

    /// <summary>Per origin, the source's applied low watermark.</summary>
    [Id(1)] public IReadOnlyDictionary<string, HybridLogicalClock> LowWatermarks { get; init; } =
        new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);

    /// <summary>Per origin, the writes below its low watermark the source held without applying.</summary>
    [Id(2)] public IReadOnlyDictionary<string, HybridLogicalClock[]> Held { get; init; } =
        new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal);
}
