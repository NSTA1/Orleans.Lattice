using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// Completes tree names in cluster scope: free text matches any part of a logical
/// tree id, and a raw <c>/cluster/trees/...</c> address completes by prefix.
/// </summary>
/// <param name="catalog">The circuit's tree catalogue.</param>
internal sealed class ClusterCompletionSource(ClusterTreeCatalog catalog) : IAddressCompletionSource
{
    private static readonly string TreesPrefix = "/" + ClusterAddresses.AreaKey + "/" + ClusterAddresses.TreesSegment + "/";

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        string text;
        bool prefixOnly;
        switch (query.Mode)
        {
            case AddressQueryMode.Search:
                text = query.Text.Trim();
                prefixOnly = false;
                break;

            case AddressQueryMode.Address when query.Text.StartsWith(TreesPrefix, StringComparison.OrdinalIgnoreCase):
                text = query.Text[TreesPrefix.Length..];
                prefixOnly = true;
                break;

            default:
                return [];
        }

        if (!prefixOnly && text.Length == 0)
        {
            return [];
        }

        // From a tenant-rooted address only that tenant's own trees complete, rooted at it.
        var scope = query.Current.Tenant;
        var trees = ClusterTreeCatalog.InScope(await catalog.GetAsync(refresh: false, cancellationToken).ConfigureAwait(false), scope);
        var matches = new List<AddressCompletion>(Math.Min(query.Limit, trees.Count));
        foreach (var tree in RankedMatches(trees, text, prefixOnly))
        {
            if (!ClusterAddresses.TryTree(tree.TreeId, ClusterTreeView.Overview, out var target))
            {
                continue;
            }

            matches.Add(new AddressCompletion(tree.TreeId, target.WithTenant(scope), Detail(tree)));
            if (matches.Count == query.Limit)
            {
                break;
            }
        }

        return matches;
    }

    private static IEnumerable<ClusterTreeEntry> RankedMatches(IReadOnlyList<ClusterTreeEntry> trees, string text, bool prefixOnly)
    {
        var prefixed = trees.Where(tree => tree.TreeId.StartsWith(text, StringComparison.OrdinalIgnoreCase));
        if (prefixOnly)
        {
            return prefixed;
        }

        var contained = trees.Where(tree =>
            !tree.TreeId.StartsWith(text, StringComparison.OrdinalIgnoreCase)
            && tree.TreeId.Contains(text, StringComparison.OrdinalIgnoreCase));
        return prefixed.Concat(contained);
    }

    private static string Detail(ClusterTreeEntry tree) =>
        tree.Name.Ownership is { } owners ? $"Cluster tree, {owners}" : "Cluster tree";
}
