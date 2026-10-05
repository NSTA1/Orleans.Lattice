namespace Orleans.Lattice.BPlusTree.State;

/// <summary>Persisted state of <see cref="ICrossTreeBarrierIndexGrain"/>.</summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeBarrierIndexState)]
internal sealed class CrossTreeBarrierIndexState
{
    /// <summary>The registered barrier keys.</summary>
    [Id(0)]
    public HashSet<string> Barriers { get; set; } = new(StringComparer.Ordinal);
}
