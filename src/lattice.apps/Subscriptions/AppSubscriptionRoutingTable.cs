using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Apps;

/// <summary>
/// An immutable routing table from effective tree id to the activated subscriptions observing it,
/// built from one registry snapshot and swapped atomically, so the dispatch path is a single
/// dictionary probe and never resolves manifests, scopes or handlers.
/// </summary>
internal sealed class AppSubscriptionRoutingTable
{
    private readonly Dictionary<string, AppSubscriptionRoute[]> _routes;
    private readonly Dictionary<(TenantId Tenant, AppSlug App), IReadOnlyList<string>> _failures;

    /// <summary>Initializes a routing table.</summary>
    /// <param name="snapshot">The registry snapshot the table was built from, or <c>null</c> when never built.</param>
    /// <param name="routes">The routes by effective tree id (ordinal).</param>
    /// <param name="failures">The enabled installs whose subscription activation failed, with the reasons.</param>
    public AppSubscriptionRoutingTable(
        CompiledAppRegistrySnapshot? snapshot,
        Dictionary<string, AppSubscriptionRoute[]> routes,
        Dictionary<(TenantId Tenant, AppSlug App), IReadOnlyList<string>> failures)
    {
        Snapshot = snapshot;
        _routes = routes;
        _failures = failures;
    }

    /// <summary>The table served before the first build; it matches no registry snapshot.</summary>
    public static AppSubscriptionRoutingTable Empty { get; } = new(null, new(StringComparer.Ordinal), new());

    /// <summary>The registry snapshot the table was built from, or <c>null</c> for <see cref="Empty"/>.</summary>
    public CompiledAppRegistrySnapshot? Snapshot { get; }

    /// <summary>The epoch of <see cref="Snapshot"/>, or <c>-1</c> for <see cref="Empty"/>.</summary>
    public long Epoch => Snapshot?.Epoch ?? -1;

    /// <summary>The number of observed effective trees.</summary>
    public int TreeCount => _routes.Count;

    /// <summary>Looks up the routes observing one effective tree. Allocation-free.</summary>
    /// <param name="treeId">The mutation's tree id.</param>
    /// <param name="routes">The routes when any observe the tree.</param>
    /// <returns><c>true</c> when at least one subscription observes the tree.</returns>
    public bool TryGetRoutes(string? treeId, [NotNullWhen(true)] out AppSubscriptionRoute[]? routes)
    {
        if (treeId is not null && _routes.TryGetValue(treeId, out routes))
            return true;
        routes = null;
        return false;
    }

    /// <summary>Returns the reasons an enabled install's subscription activation failed.</summary>
    /// <param name="tenant">The install's tenant.</param>
    /// <param name="app">The app.</param>
    /// <param name="reasons">The failure reasons when activation failed.</param>
    /// <returns><c>true</c> when the install is enabled but its subscriptions are not active.</returns>
    public bool TryGetFailure(TenantId tenant, AppSlug app, [NotNullWhen(true)] out IReadOnlyList<string>? reasons) =>
        _failures.TryGetValue((tenant, app), out reasons);
}
