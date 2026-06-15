namespace Orleans.Lattice;

/// <summary>
/// A (tree, key) pair yielded by a multi-tree tag query
/// (<see cref="ILatticeMultiTreeTagQuery"/>), identifying which subject tree a
/// matching key belongs to.
/// </summary>
/// <param name="TreeId">The subject tree the key lives in.</param>
/// <param name="Key">The matching key.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.TaggedKey)]
public readonly record struct TaggedKey(
    [property: Id(0)] string TreeId,
    [property: Id(1)] string Key);
