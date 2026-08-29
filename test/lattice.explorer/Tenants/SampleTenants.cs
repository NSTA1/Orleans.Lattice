using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;
using Orleans.Lattice.Explorer.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// Fixed Explorer-term replies and a one-line workspace builder for the Tenants
/// plugin's tests. Every figure is a literal, so no test depends on a live
/// sampler, a wall clock, or an ordering.
/// </summary>
internal static class SampleTenants
{
    public const string Acme = "acme";
    public const string Globex = "globex";
    public const string DefaultTenant = "default";
    public const string Subject = "user:ada";
    public const string Scope = "t/acme/orders";
    public const string Region = "westeurope";
    public const string OtherRegion = "eastus";

    /// <summary>A tenant summary, active and not the reserved default by default.</summary>
    public static ExplorerTenantSummary Summary(
        string tenantId = Acme,
        ExplorerTenantLifecycle status = ExplorerTenantLifecycle.Active,
        bool isDefault = false) =>
        new(tenantId, status, isDefault);

    /// <summary>A tenant detail carrying one online region and the mixed quota ceilings.</summary>
    public static ExplorerTenantDetail Detail(
        string tenantId = Acme,
        ExplorerTenantLifecycle status = ExplorerTenantLifecycle.Active,
        bool isDefault = false,
        IReadOnlyList<ExplorerTenantRegion>? regions = null,
        ExplorerTenantQuotaLimits? quotas = null) =>
        new()
        {
            TenantId = tenantId,
            Status = status,
            IsDefault = isDefault,
            Regions = regions ?? [OnlineRegion()],
            Quotas = quotas ?? Limits(),
        };

    /// <summary>A region the tenant is allowed into and resident in.</summary>
    public static ExplorerTenantRegion OnlineRegion(string regionId = Region) =>
        new(regionId, ExplorerTenantRegionLifecycle.Online, IsAllowed: true);

    /// <summary>A region the tenant is allowed into but not resident in.</summary>
    public static ExplorerTenantRegion AllowedButEmptyRegion(string regionId = OtherRegion) =>
        new(regionId, ExplorerTenantRegionLifecycle.None, IsAllowed: true);

    /// <summary>
    /// Ceilings where two dimensions are bounded, one is bounded at exactly zero,
    /// and two are unbounded - so a test can prove none of the three cases is
    /// flattened into another.
    /// </summary>
    public static ExplorerTenantQuotaLimits Limits() => new()
    {
        MaxBytes = 1_000,
        MaxKeys = 500,
        MaxMemoryBytes = 0,
        MaxTreeCount = null,
        MaxOpsPerSecond = null,
        BurstPercent = 10,
    };

    /// <summary>
    /// A warm reading exercising every distinction at once: a measured bounded
    /// dimension, a measured dimension against a ceiling of zero, a measured
    /// dimension with no ceiling, an unmeasured dimension with a ceiling, and a
    /// dimension that is neither bounded nor measured.
    /// </summary>
    public static ExplorerTenantQuotaUsage Usage(
        string tenantId = Acme,
        ExplorerTenantQuotaEnforcement scope = ExplorerTenantQuotaEnforcement.GlobalConverged,
        bool hasUsage = true,
        long? trees = 3) =>
        new()
        {
            TenantId = tenantId,
            IsDefault = false,
            EnforcementScope = scope,
            HasUsage = hasUsage,
            Bytes = new ExplorerTenantQuotaDimension { Usage = 250, Limit = 1_000, BurstLimit = 1_100 },
            Keys = new ExplorerTenantQuotaDimension { Usage = 0, Limit = 500, BurstLimit = 550 },
            MemoryBytes = new ExplorerTenantQuotaDimension
            {
                Usage = 64,
                Limit = 0,
                BurstLimit = 0,
                Overage = 64,
                MeteredOverage = 128,
            },
            TreeCount = new ExplorerTenantQuotaDimension { Usage = trees, Limit = null, BurstLimit = null },
            OpsPerSecond = new ExplorerTenantQuotaDimension { Usage = null, Limit = 900, BurstLimit = 990 },
            BurstPercent = 10,
            Limits = Limits(),
        };

    /// <summary>A cross-tenant grant in the requested state.</summary>
    public static ExplorerTenantGrant Grant(
        ExplorerTenantGrantState state = ExplorerTenantGrantState.Pending,
        ExplorerTenantGrantAccess operations = ExplorerTenantGrantAccess.Read,
        string granter = Acme,
        string grantee = Globex,
        string grantId = "grant-1") =>
        new(grantId, granter, grantee, Scope, operations, state);

    /// <summary>
    /// Builds a workspace over a fresh fake domain and access store, with the
    /// plugin gate already published as allowed unless a test says otherwise.
    /// </summary>
    /// <param name="access">The gate decision to publish; allowed by default.</param>
    /// <returns>The workspace, its domain, and the store, for the test to drive.</returns>
    public static (TenantsWorkspace Workspace, FakeTenancyDomain Domain, ExplorerPluginAccessStore Store)
        Workspace(ExplorerPluginAccess? access = null)
    {
        var domain = new FakeTenancyDomain();
        var store = new ExplorerPluginAccessStore();
        store.Set(TenantsPluginKeys.PluginId, access ?? ExplorerPluginAccess.Allowed);
        return (new TenantsWorkspace(domain, store), domain, store);
    }

    /// <summary>
    /// Builds a workspace whose domain already lists <paramref name="tenantId"/>
    /// with a detail, a usage reading, regions, admin subjects, and grants, so a
    /// test can select it without arranging five collections by hand.
    /// </summary>
    /// <param name="tenantId">The tenant to seed.</param>
    /// <returns>The workspace and its domain.</returns>
    public static (TenantsWorkspace Workspace, FakeTenancyDomain Domain) Seeded(string tenantId = Acme)
    {
        var (workspace, domain, _) = Workspace();
        var service = domain.Service;
        service.Tenants.Add(Summary(tenantId));
        service.Details[tenantId] = Detail(tenantId);
        service.Usage[tenantId] = Usage(tenantId);
        service.Regions[tenantId] = [OnlineRegion(), AllowedButEmptyRegion()];
        service.AdminSubjects[tenantId] = [Subject, "user:grace"];
        service.Grants[tenantId] = new ExplorerTenantGrants
        {
            TenantId = tenantId,
            Issued = [Grant(ExplorerTenantGrantState.Pending)],
            Received = [Grant(ExplorerTenantGrantState.Active, ExplorerTenantGrantAccess.ReadWrite, Globex, tenantId, "grant-2")],
        };

        return (workspace, domain);
    }
}
