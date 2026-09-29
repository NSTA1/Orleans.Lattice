namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// A logical tree id read in the cluster's composition grammar (#2235 D1):
/// <c>t/{tenant}/</c> when a tenant owns it, then <c>a/{app}/</c> when an app owns
/// it, then the tree's own name. The Cluster area shows the whole logical id in
/// mono and names the owning tenant and app beside it; a physical id is never read
/// or shown here.
/// </summary>
/// <param name="TreeId">The whole logical tree id.</param>
/// <param name="Tenant">The owning tenant, or <see langword="null"/> for a tree with no tenant prefix.</param>
/// <param name="App">The owning app's slug, or <see langword="null"/> for a tree no app owns.</param>
/// <param name="Name">The tree's own name, after any ownership prefixes.</param>
internal readonly record struct ClusterTreeName(string TreeId, string? Tenant, string? App, string Name)
{
    /// <summary>Whether an app owns the tree.</summary>
    public bool IsAppTree => App is not null;

    /// <summary>Reads <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The parsed name. An id that is not in the grammar is its own name.</returns>
    public static ClusterTreeName Parse(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        var rest = treeId;
        string? tenant = null;
        string? app = null;

        if (TryTakeOwner(ref rest, "t/", out var owner))
        {
            tenant = owner;
        }

        if (TryTakeOwner(ref rest, "a/", out owner))
        {
            app = owner;
        }

        return new ClusterTreeName(treeId, tenant, app, rest);
    }

    /// <summary>A short ownership phrase: "app crm, tenant acme", or <see langword="null"/> when nothing owns the tree.</summary>
    public string? Ownership => (Tenant, App) switch
    {
        (null, null) => null,
        (null, { } a) => $"app {a}",
        ({ } t, null) => $"tenant {t}",
        ({ } t, { } a) => $"app {a}, tenant {t}",
    };

    private static bool TryTakeOwner(ref string rest, string prefix, out string owner)
    {
        owner = string.Empty;
        if (!rest.StartsWith(prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var tail = rest[prefix.Length..];
        var slash = tail.IndexOf('/', StringComparison.Ordinal);
        if (slash <= 0 || slash >= tail.Length - 1)
        {
            return false;
        }

        owner = tail[..slash];
        rest = tail[(slash + 1)..];
        return true;
    }
}
