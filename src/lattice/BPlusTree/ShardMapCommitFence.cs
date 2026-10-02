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
}
