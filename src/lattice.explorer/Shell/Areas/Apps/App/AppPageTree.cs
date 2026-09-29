using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.App;

/// <summary>
/// One of an app's trees as its page shows it: the logical name, the declared shape and
/// retention, and whether it adopts a pre-existing tree - never a physical or adopted id.
/// </summary>
/// <param name="Name">The app-local tree name.</param>
/// <param name="Rebuildable">Whether the app declares the tree rebuildable from other data.</param>
/// <param name="Adopted">Whether the tree adopts an operator-declared pre-existing tree.</param>
/// <param name="ShardCount">The declared shard count, or <see langword="null"/> for host defaults.</param>
/// <param name="MaxLeafKeys">The declared maximum keys per leaf, or <see langword="null"/>.</param>
/// <param name="MaxInternalChildren">The declared maximum internal-node children, or <see langword="null"/>.</param>
/// <param name="WalPartitions">The declared WAL partition count, or <see langword="null"/>.</param>
/// <param name="SoftDeleteDuration">The declared soft-delete retention, or <see langword="null"/>.</param>
internal sealed record AppPageTree(
    string Name,
    bool Rebuildable,
    bool Adopted,
    int? ShardCount,
    int? MaxLeafKeys,
    int? MaxInternalChildren,
    int? WalPartitions,
    TimeSpan? SoftDeleteDuration)
{
    /// <summary>Projects a role holder's tree.</summary>
    /// <param name="tree">The workspace tree.</param>
    /// <returns>The page's tree.</returns>
    public static AppPageTree From(WorkspaceTreeDescriptor tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        return new(tree.Name, tree.Rebuildable, tree.Adopted, tree.ShardCount, tree.MaxLeafKeys, tree.MaxInternalChildren, tree.WalPartitions, tree.SoftDeleteDuration);
    }

    /// <summary>Projects an administrative tree declaration, dropping its adoption id.</summary>
    /// <param name="tree">The manifest tree.</param>
    /// <returns>The page's tree.</returns>
    public static AppPageTree From(AppTreeDescriptor tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        return new(tree.Name, tree.Rebuildable, tree.AdoptedTreeId is not null, tree.ShardCount, tree.MaxLeafKeys, tree.MaxInternalChildren, tree.WalPartitions, tree.SoftDeleteDuration);
    }
}
