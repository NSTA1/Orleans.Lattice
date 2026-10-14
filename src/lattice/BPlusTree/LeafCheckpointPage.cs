namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A leaf's replay checkpoint and chain link, sampled without waiting for
/// replay so materialiser-lag walks can observe progress in flight.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafCheckpointPage)]
[Immutable]
internal readonly record struct LeafCheckpointPage
{
    /// <summary>The currently published projection checkpoint.</summary>
    [Id(0)] public long CheckpointOffset { get; init; }

    /// <summary>The leaf's right-sibling pointer and bounds.</summary>
    [Id(1)] public LeafChainLink Link { get; init; }
}
