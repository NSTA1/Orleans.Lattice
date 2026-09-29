using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// What a tree workspace's tabs share, cascaded by the workspace page: the tree
/// the address resolved to, the address itself, and how to link and navigate
/// within the workspace. A new instance is cascaded on every navigation, so a
/// tab re-reads the query values it cares about.
/// </summary>
internal sealed class DataWorkspace
{
    private readonly ExplorerNavigator _navigator;

    /// <summary>Creates the workspace context.</summary>
    /// <param name="tree">The resolved tree.</param>
    /// <param name="address">The current canonical address.</param>
    /// <param name="navigator">The chrome's navigator.</param>
    /// <param name="directory">The circuit's tree directory.</param>
    public DataWorkspace(DataTreeEntry tree, ExplorerAddress address, ExplorerNavigator navigator, DataDirectory directory)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(address);
        ArgumentNullException.ThrowIfNull(navigator);
        ArgumentNullException.ThrowIfNull(directory);
        Tree = tree;
        Address = address;
        Directory = directory;
        _navigator = navigator;
    }

    /// <summary>The tree the workspace shows.</summary>
    public DataTreeEntry Tree { get; }

    /// <summary>The current address, query included.</summary>
    public ExplorerAddress Address { get; }

    /// <summary>The circuit's tree directory.</summary>
    public DataDirectory Directory { get; }

    /// <summary>The value of the <c>?prefix=</c> query parameter, or <see langword="null"/>.</summary>
    public string? Prefix => Address.GetQuery(ExplorerAddress.PrefixQuery);

    /// <summary>The value of the <c>?key=</c> query parameter, or <see langword="null"/>.</summary>
    public string? Key => Address.GetQuery(ExplorerAddress.KeyQuery);

    /// <summary>The open tab.</summary>
    public string Tab => DataTabs.Parse(Address.GetQuery(DataTabs.TabQuery));

    /// <summary>The current address with <paramref name="key"/> set to <paramref name="value"/> (or removed).</summary>
    /// <param name="key">The query key.</param>
    /// <param name="value">The value, or <see langword="null"/> to remove it.</param>
    public ExplorerAddress With(string key, string? value) => Address.WithQuery(key, string.IsNullOrEmpty(value) ? null : value);

    /// <summary>The address of <paramref name="tab"/> in this workspace, keeping the current key and prefix.</summary>
    /// <param name="tab">The tab id.</param>
    public ExplorerAddress ForTab(string tab) => With(DataTabs.TabQuery, tab == DataTabs.Keys ? null : tab);

    /// <summary>The base-relative href for <paramref name="address"/>.</summary>
    /// <param name="address">The address.</param>
    public string Href(ExplorerAddress address) => _navigator.Canonicalize(address).ToHref();

    /// <summary>Navigates to <paramref name="address"/>.</summary>
    /// <param name="address">The address.</param>
    /// <param name="replace">Whether to replace the current history entry.</param>
    public void NavigateTo(ExplorerAddress address, bool replace = false) => _navigator.NavigateTo(address, replace);

    /// <summary>The workspace address of another reachable tree, keeping the open tab.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="tab">The tab to open there.</param>
    public ExplorerAddress AddressOf(DataTreeEntry tree, string tab = DataTabs.Keys) =>
        tab == DataTabs.Keys ? tree.Address : tree.Address.WithQuery(DataTabs.TabQuery, tab);
}
