using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Decides whether a shard split or fold may still apply its moved-slot diff to
/// a logical tree's routing map (issue #4264).
/// <para>
/// A split or fold drains and swaps the shards of the physical tree the logical
/// id resolved to when it started, so the shard indices in its diff are indices
/// of that tree. An alias cutover in the meantime carries the destination
/// copy's own map onto the logical entry; applying the diff there would route
/// the moved slots to a shard the copy never populated, and every key in them
/// would read as absent. The diff is admitted only while the logical entry
/// still resolves to the bound physical tree and no cutover has carried another
/// tree's map onto it ahead of its alias swap.
/// </para>
/// </summary>
internal static class ShardMapCommitFence
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="entry"/>, the logical
    /// registry entry of <paramref name="treeId"/>, still describes the shards of
    /// <paramref name="boundPhysicalTreeId"/>.
    /// </summary>
    /// <param name="entry">The logical tree's registry entry, or <see langword="null"/> when it has none.</param>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="boundPhysicalTreeId">The physical tree the coordinator drained and swapped.</param>
    public static bool Admits(TreeRegistryEntry? entry, string treeId, string boundPhysicalTreeId)
    {
        var resolved = entry?.PhysicalTreeId ?? treeId;
        if (!string.Equals(resolved, boundPhysicalTreeId, StringComparison.Ordinal)) return false;

        return entry?.AliasCutoverTarget is not { } cutoverTarget
            || string.Equals(cutoverTarget, boundPhysicalTreeId, StringComparison.Ordinal);
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="treeId"/>'s own row is
    /// not the routing map anything reads, because the tree is a derived copy its
    /// owner currently aliases, or is cutting over to (issue #6083).
    /// <para>
    /// Routing for a logical tree pairs the physical copy with the map on the
    /// <em>logical</em> row, so a slot diff written onto the copy's own row moves
    /// nothing a router reads: the moved slots keep routing to the donor, which a
    /// fold then retires. <see cref="Admits"/> cannot see this, because the copy's
    /// row resolves to itself. The test reads the copy's
    /// <see cref="TreeRegistryEntry.DerivedFrom"/> and the owner's row, so it
    /// covers resize, shadow-cutover restore and schema remediation copies; a bare
    /// alias onto an independent tree records no owner and is not detected.
    /// </para>
    /// </summary>
    /// <param name="entry">The registry row of <paramref name="treeId"/>, or <see langword="null"/>.</param>
    /// <param name="treeId">The tree whose row a split or fold would rewrite.</param>
    /// <param name="ownerEntry">The row of <paramref name="entry"/>'s <see cref="TreeRegistryEntry.DerivedFrom"/> owner, or <see langword="null"/>.</param>
    public static bool IsAliasTarget(TreeRegistryEntry? entry, string treeId, TreeRegistryEntry? ownerEntry)
    {
        if (entry?.DerivedFrom is not { } owner || string.Equals(owner, treeId, StringComparison.Ordinal))
            return false;

        return ownerEntry is not null
            && (string.Equals(ownerEntry.PhysicalTreeId, treeId, StringComparison.Ordinal)
                || string.Equals(ownerEntry.AliasCutoverTarget, treeId, StringComparison.Ordinal));
    }

    /// <summary>
    /// <see cref="IsAliasTarget"/> after reading the owner's row from
    /// <paramref name="registry"/>. Costs a registry read only for a derived copy.
    /// </summary>
    public static async Task<bool> IsAliasTargetAsync(ILatticeRegistry registry, TreeRegistryEntry? entry, string treeId)
    {
        ArgumentNullException.ThrowIfNull(registry);
        if (entry?.DerivedFrom is not { } owner || string.Equals(owner, treeId, StringComparison.Ordinal))
            return false;

        return IsAliasTarget(entry, treeId, await registry.GetEntryAsync(owner));
    }

    /// <summary>
    /// <see cref="Admits"/>, and additionally refuses a row that is an alias
    /// target (<see cref="IsAliasTarget"/>).
    /// </summary>
    public static async Task<bool> AdmitsAsync(
        ILatticeRegistry registry, TreeRegistryEntry? entry, string treeId, string boundPhysicalTreeId) =>
        Admits(entry, treeId, boundPhysicalTreeId)
        && !await IsAliasTargetAsync(registry, entry, treeId);
}
