namespace Orleans.Lattice;

/// <summary>
/// Thrown by a B+ leaf grain when a mutation reaches a leaf that empty-leaf
/// chain reclaim has already retired from the tree.
/// <para>
/// A retired leaf has been removed from its parent's routing table and
/// unlinked from the sibling chain, and the key range it used to own now
/// belongs to its predecessor. A write arriving after that point was routed on
/// a topology snapshot that no longer holds, so it is misdirected: refusing it
/// tells the caller to re-route, where accepting it would apply the row to a
/// leaf no scan can reach and no projection rebuild would restore.
/// </para>
/// <para>
/// The window this closes is narrow by construction - reclaim only retires a
/// leaf that is still empty at the moment of the decision, and a mutation
/// merely racing the decision causes the fold to be abandoned rather than the
/// write to be refused - so this exception marks a genuinely stale route, not
/// ordinary contention with background maintenance.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafRetired)]
internal sealed class LeafRetiredException : Exception
{
    /// <summary>The identity of the leaf grain that has been retired.</summary>
    [Id(0)] public string LeafId { get; set; } = string.Empty;

    /// <summary>Creates a new <see cref="LeafRetiredException"/>.</summary>
    public LeafRetiredException(string leafId)
        : base($"Leaf '{leafId}' has been reclaimed from the leaf chain and no longer owns any key range. Re-route the operation and retry.")
    {
        LeafId = leafId;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public LeafRetiredException() { }
}
