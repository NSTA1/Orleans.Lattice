namespace Orleans.Lattice.BPlusTree;

/// <summary>Leaf statistics and the topology read after them in the same turn.</summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafStatsPage)]
[Immutable]
internal readonly record struct LeafStatsPage
{
    /// <summary>The normal live, tombstone and state-byte statistics.</summary>
    [Id(0)] public LeafStats Stats { get; init; }

    /// <summary>The bounds and successor for continuing the bounded walk.</summary>
    [Id(1)] public LeafChainLink Link { get; init; }
}
