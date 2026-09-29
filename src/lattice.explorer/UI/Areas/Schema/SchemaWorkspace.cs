using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// One tree's schema workspace, cascaded from the tree page to its tabs: which
/// tree, which tab, what the caller may do, the tree's schema state, and how to
/// move between tabs. Rebuilt by the page on every navigation.
/// </summary>
internal sealed class SchemaWorkspace
{
    private readonly ExplorerNavigator _navigator;
    private readonly Func<Task> _refresh;

    /// <summary>Creates the workspace.</summary>
    /// <param name="address">The page's canonical address.</param>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="grants">What the caller may do with the tree's schema.</param>
    /// <param name="row">The tree's schema state.</param>
    /// <param name="navigator">The navigator links and moves go through.</param>
    /// <param name="refresh">Re-reads the tree's schema state after a change.</param>
    public SchemaWorkspace(
        ExplorerAddress address,
        string treeId,
        SchemaGrants grants,
        SchemaTreeRow row,
        ExplorerNavigator navigator,
        Func<Task> refresh)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(grants);
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(navigator);
        ArgumentNullException.ThrowIfNull(refresh);
        Address = address;
        TreeId = treeId;
        Grants = grants;
        Row = row;
        _navigator = navigator;
        _refresh = refresh;
        Tab = SchemaTabs.Parse(address.GetQuery(SchemaAddresses.TabQuery));
    }

    /// <summary>The page's canonical address.</summary>
    public ExplorerAddress Address { get; }

    /// <summary>The logical tree id.</summary>
    public string TreeId { get; }

    /// <summary>The open tab.</summary>
    public string Tab { get; }

    /// <summary>What the caller may do with the tree's schema.</summary>
    public SchemaGrants Grants { get; }

    /// <summary>The tree's schema state when the page last read it.</summary>
    public SchemaTreeRow Row { get; }

    /// <summary>The address of <paramref name="tab"/> of this tree, keeping the tenant.</summary>
    /// <param name="tab">The tab.</param>
    /// <returns>The address.</returns>
    public ExplorerAddress ForTab(string tab) => SchemaAddresses.Tree(TreeId, tab).WithTenant(Address.Tenant);

    /// <summary>The base-relative link to <paramref name="address"/>, canonical for the caller's tenancy.</summary>
    /// <param name="address">The address.</param>
    /// <returns>The href.</returns>
    public string Href(ExplorerAddress address) => _navigator.Canonicalize(address.WithTenant(Address.Tenant)).ToHref();

    /// <summary>Moves to <paramref name="address"/>.</summary>
    /// <param name="address">The address.</param>
    /// <param name="replace">Replace the current history entry rather than adding one.</param>
    public void NavigateTo(ExplorerAddress address, bool replace = false) =>
        _navigator.NavigateTo(address.WithTenant(Address.Tenant), replace);

    /// <summary>Re-reads the tree's schema state, after a change, so the heading and every tab agree.</summary>
    /// <returns>A task that completes when the page has re-read it.</returns>
    public Task RefreshAsync() => _refresh();
}
