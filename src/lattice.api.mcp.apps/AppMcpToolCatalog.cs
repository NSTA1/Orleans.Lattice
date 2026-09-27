using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// The app tool surface's precomputed view of one registry epoch: per tenant, the enabled
/// installs whose tool activation succeeded, plus every activation that failed. Built
/// once when the registry epoch advances and read without locking by every session, so
/// the per-session path only selects from prebuilt lists.
/// </summary>
internal sealed class AppMcpToolCatalog
{
    private readonly Dictionary<TenantId, AppMcpInstalledApp[]> _byTenant;

    /// <summary>Initializes a new <see cref="AppMcpToolCatalog"/>.</summary>
    /// <param name="epoch">The registry epoch the catalog was built from.</param>
    /// <param name="byTenant">The installs, per tenant, ordered by slug.</param>
    /// <param name="activations">Every activation built for the epoch, keyed by app and version.</param>
    public AppMcpToolCatalog(
        long epoch,
        Dictionary<TenantId, AppMcpInstalledApp[]> byTenant,
        Dictionary<(AppSlug Slug, AppVersion Version), AppMcpToolActivation> activations)
    {
        ArgumentNullException.ThrowIfNull(byTenant);
        ArgumentNullException.ThrowIfNull(activations);
        Epoch = epoch;
        _byTenant = byTenant;
        Activations = activations;
        var failures = new List<AppMcpToolActivation>();
        foreach (var activation in activations.Values)
        {
            if (!activation.Succeeded)
                failures.Add(activation);
        }

        failures.Sort(static (x, y) => string.CompareOrdinal(x.Slug.Value, y.Slug.Value));
        Failures = failures;
    }

    /// <summary>An empty catalog at epoch <c>-1</c>, which never matches a real epoch.</summary>
    public static AppMcpToolCatalog Empty { get; } = new(-1, new(), new());

    /// <summary>The registry epoch the catalog was built from.</summary>
    public long Epoch { get; }

    /// <summary>Every activation built for the epoch, keyed by app and version.</summary>
    public IReadOnlyDictionary<(AppSlug Slug, AppVersion Version), AppMcpToolActivation> Activations { get; }

    /// <summary>The activations that failed, ordered by slug.</summary>
    public IReadOnlyList<AppMcpToolActivation> Failures { get; }

    /// <summary>Returns a tenant's installs, ordered by slug.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <returns>The installs; empty when the tenant has none.</returns>
    public IReadOnlyList<AppMcpInstalledApp> GetTenantApps(TenantId tenant) =>
        tenant.Value is not null && _byTenant.TryGetValue(tenant, out var apps) ? apps : [];

    /// <summary>Looks up one tenant install.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="slug">The app.</param>
    /// <param name="app">The install when found.</param>
    /// <returns><c>true</c> when the tenant has an active install of the app.</returns>
    public bool TryGetApp(TenantId tenant, AppSlug slug, out AppMcpInstalledApp app)
    {
        foreach (var candidate in GetTenantApps(tenant))
        {
            if (candidate.Activation.Slug == slug)
            {
                app = candidate;
                return true;
            }
        }

        app = null!;
        return false;
    }
}
