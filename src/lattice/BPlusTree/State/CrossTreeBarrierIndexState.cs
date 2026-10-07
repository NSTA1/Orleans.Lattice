namespace Orleans.Lattice.BPlusTree.State;

/// <summary>Persisted state of <see cref="ICrossTreeBarrierIndexGrain"/>.</summary>
[GenerateSerializer]
[Alias(TypeAliases.CrossTreeBarrierIndexState)]
internal sealed class CrossTreeBarrierIndexState
{
    /// <summary>The registered barrier keys.</summary>
    [Id(0)]
    public HashSet<string> Barriers { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The tree's latest snapshot import per origin cluster.</summary>
    [Id(1)]
    public Dictionary<string, CrossTreeImportRecord> Imports { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The decided barrier tombstones the tree took part in (issue #4733).</summary>
    [Id(2)]
    public HashSet<string> Tombstones { get; set; } = new(StringComparer.Ordinal);
}
