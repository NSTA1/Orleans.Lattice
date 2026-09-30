namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// How a backed-up tree is named on the page. A tree an app owns is named
/// <c>a/{app}/{tree}</c> (under <c>t/{tenant}/</c> when the id is tenant
/// composed), so the page shows the app-local tree name and the owning app
/// rather than the composed id; any other tree shows its id without the tenant
/// root, which the address line already carries.
/// </summary>
/// <param name="TreeId">The tree id exactly as the backup recorded it.</param>
/// <param name="AppSlug">The owning app's slug, or <see langword="null"/> for a tree no app owns.</param>
/// <param name="Name">The name to show: the app-local tree name, or the tree id without its tenant root.</param>
internal sealed record BackupTreeName(string TreeId, string? AppSlug, string Name)
{
    private const string TenantPrefix = "t/";
    private const string AppPrefix = "a/";

    /// <summary>Whether an app owns the tree.</summary>
    public bool IsAppTree => AppSlug is not null;

    /// <summary>Reads the naming out of a tree id.</summary>
    /// <param name="treeId">The tree id.</param>
    public static BackupTreeName Parse(string treeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var rest = treeId;
        if (rest.StartsWith(TenantPrefix, StringComparison.Ordinal))
        {
            var end = rest.IndexOf('/', TenantPrefix.Length);
            if (end > TenantPrefix.Length && end < rest.Length - 1)
            {
                rest = rest[(end + 1)..];
            }
        }

        if (rest.StartsWith(AppPrefix, StringComparison.Ordinal))
        {
            var end = rest.IndexOf('/', AppPrefix.Length);
            if (end > AppPrefix.Length && end < rest.Length - 1)
            {
                return new BackupTreeName(treeId, rest[AppPrefix.Length..end], rest[(end + 1)..]);
            }
        }

        return new BackupTreeName(treeId, null, rest);
    }
}
