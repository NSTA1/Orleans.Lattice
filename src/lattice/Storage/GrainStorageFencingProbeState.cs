namespace Orleans.Lattice;

/// <summary>
/// The row the grain-storage fencing probe writes under its reserved grain type
/// and state name. Its content is irrelevant; only the ETags the provider issues
/// for it matter.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GrainStorageFencingProbeState)]
internal sealed class GrainStorageFencingProbeState
{
    /// <summary>A counter bumped by every probe write.</summary>
    [Id(0)] public long Sequence { get; set; }
}
