using System.Collections.Frozen;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// An immutable, in-memory compilation of the per-tenant admission inputs: each
/// registered tenant's resolved <see cref="TenantQuotas"/> joined with its global
/// usage fold and this cluster's local usage slot, arranged for an
/// allocation-free lookup on the write-admission path. Built by <see cref="Compile"/>
/// from the tenant registry and the usage tree and swapped atomically by
/// <see cref="TenantUsageIndexMaintainer"/> whenever either tree changes, so a warm
/// admission decision is a pure in-memory lookup.
/// </summary>
/// <remarks>
/// This type is in-process singleton state. It is never serialized and never
/// crosses a grain boundary, so it carries no Orleans serialization attributes.
/// Enforcement keys on the registry: only tenants with a registry record (and thus
/// a resolved quota) get a view. A usage record with no registry record is skipped
/// because there is no quota to admit it against. A registry tenant with no usage
/// record yet gets a view whose usage aggregates are <see cref="LocalUsageSample.Empty"/>,
/// so enforcement fails open until the first sample lands.
/// </remarks>
internal sealed class CompiledTenantUsage
{
    private readonly FrozenDictionary<string, TenantUsageView> _tenants;

    private CompiledTenantUsage(FrozenDictionary<string, TenantUsageView> tenants) => _tenants = tenants;

    /// <summary>The empty snapshot: no tenants. Used before the first compile.</summary>
    public static CompiledTenantUsage Empty { get; } =
        new(FrozenDictionary<string, TenantUsageView>.Empty);

    /// <summary>The number of tenants in the snapshot. Exposed for tests.</summary>
    internal int TenantCount => _tenants.Count;

    /// <summary>
    /// The tenants in the snapshot, keyed by tenant id text, each with its usage
    /// view. Exposed for the off-path per-tenant observability enumeration; the
    /// warm admission path uses only <see cref="TryGetView"/>. The backing frozen
    /// dictionary is immutable, so this is a zero-copy view.
    /// </summary>
    public IReadOnlyDictionary<string, TenantUsageView> Tenants => _tenants;

    /// <summary>
    /// Attempts to get the warm admission view for a tenant. A pure in-memory
    /// lookup with no allocation on either path.
    /// </summary>
    /// <param name="tenant">The tenant to look up.</param>
    /// <param name="view">The admission view when present; otherwise <c>default</c>.</param>
    /// <returns><c>true</c> when the tenant is present in the snapshot.</returns>
    public bool TryGetView(TenantId tenant, out TenantUsageView view)
    {
        if (tenant.Value is null)
        {
            view = default;
            return false;
        }

        return _tenants.TryGetValue(tenant.Value, out view);
    }

    /// <summary>
    /// Compiles the tenant registry records and usage records into an immutable
    /// admission snapshot for the cluster identified by <paramref name="localClusterId"/>.
    /// Each registry record contributes its resolved quota; its matching usage
    /// record (if any) contributes the global fold (sum over all cluster slots) and
    /// this cluster's local slot. Records with the uninitialised tenant id are
    /// skipped, as are usage records with no registry record.
    /// </summary>
    /// <param name="registry">The tenant registry records (the quota source). Must not be <c>null</c>.</param>
    /// <param name="usage">The tenant usage records (the usage source). Must not be <c>null</c>.</param>
    /// <param name="localClusterId">This cluster's id, selecting the local slot. Must not be <c>null</c>.</param>
    /// <returns>The compiled admission snapshot.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static CompiledTenantUsage Compile(
        IEnumerable<TenantRecord> registry,
        IEnumerable<TenantUsageRecord> usage,
        string localClusterId)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(usage);
        ArgumentNullException.ThrowIfNull(localClusterId);

        var usageById = new Dictionary<string, TenantUsageRecord>(StringComparer.Ordinal);
        foreach (var record in usage)
        {
            if (record is not null && record.Id.Value is { } id)
            {
                usageById[id] = record;
            }
        }

        var tenants = new Dictionary<string, TenantUsageView>(StringComparer.Ordinal);
        foreach (var record in registry)
        {
            if (record is null || record.Id.Value is not { } id)
            {
                continue;
            }

            LocalUsageSample global;
            LocalUsageSample local;
            if (usageById.TryGetValue(id, out var usageRecord))
            {
                global = usageRecord.Fold();
                local = usageRecord.LocalSample(localClusterId);
            }
            else
            {
                global = LocalUsageSample.Empty;
                local = LocalUsageSample.Empty;
            }

            tenants[id] = new TenantUsageView(record.Quotas, global, local);
        }

        return tenants.Count == 0
            ? Empty
            : new CompiledTenantUsage(tenants.ToFrozenDictionary(StringComparer.Ordinal));
    }
}
