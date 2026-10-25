namespace Orleans.Lattice.BPlusTree;

/// <summary>A range count and the leaf topology read after it in the same turn.</summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafCountPage)]
[Immutable]
internal readonly record struct LeafCountPage
{
    /// <summary>The live key count, with the normal pending-transaction visibility rules.</summary>
    [Id(0)] public int Count { get; init; }

    /// <summary>The bounds and successor for continuing the bounded walk.</summary>
    [Id(1)] public LeafChainLink Link { get; init; }
}
