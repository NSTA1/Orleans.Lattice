namespace Orleans.Lattice.Replication;

/// <summary>
/// The source's applied low watermarks and held writes for the exported tree
/// (issue #4586 part 2b), carried on the end-of-stream trailer of an unbounded
/// export. For each origin <c>o</c>, every write of <c>o</c> to the tree stamped
/// strictly below <see cref="LowWatermarks"/>[o] is reflected in the export,
/// except those listed in <see cref="Held"/>[o]: writes the source acknowledged
/// without applying (parked, dead-lettered or lost). The values hold only under
/// <see cref="Lineage"/>; a receiver uses them only when the export's source
/// generation was stable from open to close under that lineage.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.SnapshotSourceFrontier)]
internal sealed record SnapshotSourceFrontier
{
    /// <summary>The source registry lineage the values were read under.</summary>
    [Id(0)] public Guid? Lineage { get; init; }

    /// <summary>Per origin, the applied low watermark over the exported tree. Origins without one are omitted.</summary>
    [Id(1)] public IReadOnlyDictionary<string, HybridLogicalClock> LowWatermarks { get; init; } =
        new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);

    /// <summary>Per origin, the writes the source held unapplied when the export opened.</summary>
    [Id(2)] public IReadOnlyDictionary<string, HybridLogicalClock[]> Held { get; init; } =
        new Dictionary<string, HybridLogicalClock[]>(StringComparer.Ordinal);
}