namespace Orleans.Lattice;

/// <summary>
/// Result of a leaf-level single-key delete (see
/// <see cref="BPlusTree.IBPlusLeafGrain.DeleteTrackedAsync"/>).
/// <para>
/// <see cref="Deleted"/> is <c>true</c> when the key was present and live.
/// <see cref="Split"/> carries every leaf split the delete caused, for the
/// shard root to link (issue #3523). A split that interleaves with the
/// delete's WAL append can move the key to a new sibling before its tombstone
/// is applied; the tombstone is then forwarded to the leaf that declares the
/// key, and that forwarded merge can divide the receiving leaf.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafDeleteResult)]
[Immutable]
internal readonly record struct LeafDeleteResult
{
    /// <summary><c>true</c> when the key was present and live.</summary>
    [Id(0)] public bool Deleted { get; init; }

    /// <summary>
    /// Every leaf split the delete caused, or <see langword="null"/> when none did.
    /// </summary>
    [Id(1)] public BPlusTree.SplitResult? Split { get; init; }
}
