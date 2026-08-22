namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free split-boundary ownership rule: whether a key falls in
/// a leaf's half-open <c>[lowInclusive, highExclusive)</c> key range. Extracted so
/// the split-boundary sealing that a donor mid-split relies on - a donor whose
/// <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.HighKeyExclusive"/> has
/// been narrowed to the split key must not serve or replay a key the destination
/// sibling now owns - is one shared rule the production leaf grain
/// (<c>BPlusLeafGrain.ShouldApplyDuringReplay</c>) and the Coyote reshard model
/// both execute, with no possibility of drift.
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no wall-clock, and no Orleans types;
/// it is a total function of a key and two nullable bounds and allocates nothing.
/// A <see langword="null"/> bound means "no constraint on that side", used for the
/// chain's leftmost and rightmost leaves and for legacy state shapes that pre-date
/// the persisted range. Comparison is ordinal, matching the B+ tree's key
/// ordering.
/// </para>
/// </summary>
internal static class SplitBoundary
{
    /// <summary>
    /// Reports whether <paramref name="key"/> is owned by a leaf whose range is
    /// <c>[<paramref name="lowInclusive"/>, <paramref name="highExclusive"/>)</c>.
    /// The low bound is inclusive and the high bound is exclusive, so a key equal
    /// to <paramref name="highExclusive"/> (the split key) belongs to the
    /// destination sibling, not to a donor sealed at that boundary. A
    /// <see langword="null"/> bound is treated as unbounded on that side.
    /// </summary>
    /// <param name="key">The key to test.</param>
    /// <param name="lowInclusive">
    /// The inclusive low bound, or <see langword="null"/> for no lower constraint.
    /// </param>
    /// <param name="highExclusive">
    /// The exclusive high bound (the split key when the leaf is a sealed donor), or
    /// <see langword="null"/> for no upper constraint.
    /// </param>
    public static bool Owns(string key, string? lowInclusive, string? highExclusive)
    {
        ArgumentNullException.ThrowIfNull(key);

        return (lowInclusive is null || string.CompareOrdinal(key, lowInclusive) >= 0)
            && (highExclusive is null || string.CompareOrdinal(key, highExclusive) < 0);
    }
}
