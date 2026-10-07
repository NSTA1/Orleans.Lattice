namespace Orleans.Lattice.BPlusTree.State;

/// <summary>Persisted state of <see cref="ICrossTreePurgeFrontierGrain"/>.</summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreePurgeFrontierState)]
internal sealed class CrossTreePurgeFrontierState
{
    /// <summary>Per origin tree, the highest purge frontier advertised.</summary>
    [Id(0)]
    public Dictionary<string, long> Frontiers { get; set; } = new(StringComparer.Ordinal);
}
