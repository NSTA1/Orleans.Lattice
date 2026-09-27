using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Apps;

/// <summary>
/// An immutable, compiled projection of the app registry, rebuilt from a full registry
/// scan whenever the reserved <c>sys-app-registry</c> tree mutates. Every lookup is a
/// warm, allocation-free in-memory probe: the per-tenant indexes and record lists are
/// built once at compile time and handed out as cached, read-only views.
/// </summary>
public sealed class CompiledAppRegistrySnapshot
{
    private readonly Dictionary<TenantId, TenantApps> _tenants;

    private CompiledAppRegistrySnapshot(long epoch, AppRegistryRecord[] records, Dictionary<TenantId, TenantApps> tenants)
    {
        Epoch = epoch;
        Records = records;
        _tenants = tenants;
    }

    /// <summary>The empty snapshot a cold maintainer serves before its first build (epoch <c>0</c>).</summary>
    public static CompiledAppRegistrySnapshot Empty { get; } =
        new(0, Array.Empty<AppRegistryRecord>(), new Dictionary<TenantId, TenantApps>());

    /// <summary>The maintainer epoch this snapshot was published at; <c>0</c> for <see cref="Empty"/>.</summary>
    public long Epoch { get; }

    /// <summary>Every install record, including uninstalled ones, ordered by tenant then slug (ordinal).</summary>
    public IReadOnlyList<AppRegistryRecord> Records { get; }

    /// <summary>The number of install records.</summary>
    public int Count => Records.Count;

    /// <summary>Looks up one install record.</summary>
    /// <param name="tenant">The owning tenant.</param>
    /// <param name="slug">The app slug.</param>
    /// <param name="record">The record when found; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when the tenant has a record for the slug.</returns>
    public bool TryGet(TenantId tenant, AppSlug slug, [NotNullWhen(true)] out AppRegistryRecord? record)
    {
        if (tenant.Value is not null
            && slug.Value is not null
            && _tenants.TryGetValue(tenant, out var apps)
            && apps.BySlug.TryGetValue(slug, out record))
        {
            return true;
        }

        record = null;
        return false;
    }

    /// <summary>Returns every install record of one tenant, including uninstalled ones, ordered by slug.</summary>
    /// <param name="tenant">The owning tenant.</param>
    /// <returns>A cached, read-only list; empty when the tenant has no installs.</returns>
    public IReadOnlyList<AppRegistryRecord> GetTenantApps(TenantId tenant) =>
        tenant.Value is not null && _tenants.TryGetValue(tenant, out var apps) ? apps.All : Array.Empty<AppRegistryRecord>();

    /// <summary>Returns the <see cref="AppRegistryLifecycleState.Enabled"/> installs of one tenant, ordered by slug.</summary>
    /// <param name="tenant">The owning tenant.</param>
    /// <returns>A cached, read-only list; empty when the tenant has no enabled app.</returns>
    public IReadOnlyList<AppRegistryRecord> GetEnabledTenantApps(TenantId tenant) =>
        tenant.Value is not null && _tenants.TryGetValue(tenant, out var apps) ? apps.Enabled : Array.Empty<AppRegistryRecord>();

    /// <summary>
    /// Compiles a scanned record set into a snapshot. Records are ordered by tenant then
    /// slug (ordinal), matching the registry's key order; a duplicate (tenant, slug) keeps
    /// the last record, though a well-formed scan never yields one.
    /// </summary>
    /// <param name="records">The scanned records. Must not be <c>null</c>.</param>
    /// <param name="epoch">The epoch the snapshot is published at.</param>
    /// <returns>The compiled snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="records"/> is <c>null</c>.</exception>
    internal static CompiledAppRegistrySnapshot Compile(IEnumerable<AppRegistryRecord> records, long epoch)
    {
        ArgumentNullException.ThrowIfNull(records);

        var byTenant = new Dictionary<TenantId, Dictionary<AppSlug, AppRegistryRecord>>();
        foreach (var record in records)
        {
            if (!byTenant.TryGetValue(record.Tenant, out var slugs))
            {
                slugs = new Dictionary<AppSlug, AppRegistryRecord>();
                byTenant.Add(record.Tenant, slugs);
            }

            slugs[record.Slug] = record;
        }

        var tenants = new Dictionary<TenantId, TenantApps>(byTenant.Count);
        var all = new List<AppRegistryRecord>();
        foreach (var tenant in byTenant.Keys.OrderBy(t => t.Value, StringComparer.Ordinal))
        {
            var slugs = byTenant[tenant];
            var ordered = slugs.Values.OrderBy(r => r.Slug.Value, StringComparer.Ordinal).ToArray();
            var enabled = Array.FindAll(ordered, r => r.State == AppRegistryLifecycleState.Enabled);
            tenants.Add(tenant, new TenantApps(slugs, ordered, enabled));
            all.AddRange(ordered);
        }

        return new CompiledAppRegistrySnapshot(epoch, all.ToArray(), tenants);
    }

    /// <summary>One tenant's compiled installs.</summary>
    private sealed record TenantApps(
        Dictionary<AppSlug, AppRegistryRecord> BySlug,
        AppRegistryRecord[] All,
        AppRegistryRecord[] Enabled);
}
