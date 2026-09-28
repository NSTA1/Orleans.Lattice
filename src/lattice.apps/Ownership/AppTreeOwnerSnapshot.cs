namespace Orleans.Lattice.Apps;

/// <summary>
/// A snapshot of which installed app owns each tree a manifest reaches across app boundaries, the
/// input that lets <see cref="AppRoleCompiler"/> and <see cref="AppSubscriptionCompiler"/> stay pure
/// functions while refusing a cross-app scope or subscription whose owner is not installed.
/// </summary>
/// <remarks>
/// Keys are effective (tenant-composed) tree ids; each maps to the slug of the installed (not
/// uninstalled) app whose ownership claim covers it. A cross-app target absent from the snapshot, or
/// owned by a different app than the one the template names, fails compilation.
/// </remarks>
public sealed class AppTreeOwnerSnapshot
{
    private readonly Dictionary<string, AppSlug>? _owners;

    private AppTreeOwnerSnapshot(Dictionary<string, AppSlug>? owners) => _owners = owners;

    /// <summary>A snapshot in which no tree has an installed owner: every cross-app target fails.</summary>
    public static AppTreeOwnerSnapshot None { get; } = new(null);

    /// <summary>The number of owned trees in the snapshot.</summary>
    public int Count => _owners?.Count ?? 0;

    /// <summary>Creates a snapshot from effective tree ids and the installed apps that own them.</summary>
    /// <param name="owners">Effective (tenant-composed) tree id to owning app slug; the last entry wins for a repeated id.</param>
    /// <returns>The snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="owners"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException">An entry has an empty tree id or an uninitialised slug.</exception>
    public static AppTreeOwnerSnapshot Create(IEnumerable<KeyValuePair<string, AppSlug>> owners)
    {
        ArgumentNullException.ThrowIfNull(owners);
        Dictionary<string, AppSlug>? map = null;
        foreach (var (treeId, slug) in owners)
        {
            if (string.IsNullOrEmpty(treeId))
                throw new ArgumentException("An owned tree id cannot be empty.", nameof(owners));
            if (slug.Value is null)
                throw new ArgumentException($"The owner of tree '{treeId}' is the uninitialised slug.", nameof(owners));
            (map ??= new(StringComparer.Ordinal))[treeId] = slug;
        }

        return map is null ? None : new(map);
    }

    /// <summary>Whether <paramref name="app"/> is the installed owner of <paramref name="effectiveTreeId"/>.</summary>
    /// <param name="effectiveTreeId">The effective (tenant-composed) tree id.</param>
    /// <param name="app">The app a cross-app template names.</param>
    /// <returns><c>true</c> only when the snapshot records exactly that owner for the tree.</returns>
    public bool IsOwnedBy(string effectiveTreeId, AppSlug app) =>
        _owners is not null
        && effectiveTreeId is not null
        && _owners.TryGetValue(effectiveTreeId, out var owner)
        && owner == app;
}
