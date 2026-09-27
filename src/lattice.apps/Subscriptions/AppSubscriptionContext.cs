namespace Orleans.Lattice.Apps;

/// <summary>
/// One activated change-feed subscription of one app install: which subscription it is, whose
/// tree it observes, and the tenant-composed tree id its mutations arrive on. Built once when the
/// subscription is compiled and handed to <see cref="IAppChangeFeedHandler.HandleAsync"/> on every
/// delivery, so a handler can tell its subscriptions and tenants apart without per-event state.
/// </summary>
public sealed class AppSubscriptionContext
{
    internal AppSubscriptionContext(
        TenantId tenant,
        AppSlug app,
        string name,
        AppSlug observedApp,
        string tree,
        string localTreeId,
        string treeId,
        string? keyPrefix)
    {
        Tenant = tenant;
        App = app;
        Name = name;
        ObservedApp = observedApp;
        Tree = tree;
        LocalTreeId = localTreeId;
        TreeId = treeId;
        KeyPrefix = keyPrefix;
    }

    /// <summary>The tenant of the subscribing install.</summary>
    public TenantId Tenant { get; }

    /// <summary>The subscribing app.</summary>
    public AppSlug App { get; }

    /// <summary>The subscription name declared in the manifest.</summary>
    public string Name { get; }

    /// <summary>The app whose tree is observed; equal to <see cref="App"/> for an own-tree subscription.</summary>
    public AppSlug ObservedApp { get; }

    /// <summary>The observed app-local tree name, as declared.</summary>
    public string Tree { get; }

    /// <summary>The resolved tree id in the tenant-local vocabulary, before tenant composition.</summary>
    public string LocalTreeId { get; }

    /// <summary>The effective (tenant-composed) tree id the observed mutations are committed to.</summary>
    public string TreeId { get; }

    /// <summary>The key prefix limiting observed changes, or <c>null</c> for the whole tree.</summary>
    public string? KeyPrefix { get; }

    /// <summary><c>true</c> when the subscription observes another app's tree.</summary>
    public bool IsCrossApp => ObservedApp != App;
}
