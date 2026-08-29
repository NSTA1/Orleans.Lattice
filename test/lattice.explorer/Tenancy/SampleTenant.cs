using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Fixed control-API replies for the tenancy seam's tests. Every figure is a
/// literal, so no test depends on a live sampler, a wall clock, or an ordering:
/// a quota reading here is exactly what the fake client hands back.
/// </summary>
internal static class SampleTenant
{
    public const string TenantId = "acme";
    public const string OtherTenantId = "globex";
    public const string SubjectId = "user:ada";
    public const string Scope = "t/acme/orders";

    public static TenantDescriptor Descriptor(
        string tenantId = TenantId,
        TenantLifecycleStatus status = TenantLifecycleStatus.Active,
        bool isDefault = false) =>
        new() { TenantId = tenantId, Status = status, IsDefault = isDefault };

    public static TenantStatusReport StatusReport(
        string tenantId = TenantId,
        TenantLifecycleStatus status = TenantLifecycleStatus.Active,
        bool isDefault = false) =>
        new()
        {
            TenantId = tenantId,
            Status = status,
            IsDefault = isDefault,
            Regions = [RegionDescriptor()],
            Quotas = Quotas(),
        };

    public static TenantRegionStatusDescriptor RegionDescriptor(
        string regionId = "westeurope",
        TenantRegionLifecycleStatus status = TenantRegionLifecycleStatus.Online,
        bool isAllowed = true) =>
        new() { RegionId = regionId, Status = status, IsAllowed = isAllowed };

    /// <summary>
    /// Quotas where two dimensions are bounded, one is bounded at exactly zero,
    /// and two are unbounded - so a test can prove none of the three cases is
    /// flattened into another.
    /// </summary>
    public static TenantQuotasDescriptor Quotas() => new()
    {
        MaxBytes = 1_000,
        MaxKeys = 500,
        MaxMemoryBytes = 0,
        MaxTreeCount = null,
        MaxOpsPerSecond = null,
        BurstPercent = 10,
    };

    /// <summary>
    /// A warm usage reading that exercises every distinction at once: a measured
    /// bounded dimension, a measured dimension against a ceiling of zero, a
    /// measured dimension with no ceiling, an unmeasured dimension with a
    /// ceiling, and a dimension that is neither bounded nor measured.
    /// </summary>
    public static TenantQuotaUsageReport UsageReport(
        string tenantId = TenantId,
        TenantQuotaEnforcementScope scope = TenantQuotaEnforcementScope.GlobalConverged,
        bool hasUsage = true) =>
        new()
        {
            TenantId = tenantId,
            IsDefault = false,
            EnforcementScope = scope,
            HasUsage = hasUsage,
            Bytes = new TenantQuotaDimensionUsage { Usage = 250, Limit = 1_000, BurstLimit = 1_100 },
            Keys = new TenantQuotaDimensionUsage { Usage = 0, Limit = 500, BurstLimit = 550 },
            MemoryBytes = new TenantQuotaDimensionUsage
            {
                Usage = 64,
                Limit = 0,
                BurstLimit = 0,
                Overage = 64,
                MeteredOverage = 128,
            },
            TreeCount = new TenantQuotaDimensionUsage { Usage = 3, Limit = null, BurstLimit = null },
            OpsPerSecond = new TenantQuotaDimensionUsage { Usage = null, Limit = 900, BurstLimit = 990 },
            BurstPercent = 10,
            Quotas = Quotas(),
        };

    public static TenantGrantDescriptor Grant(
        TenantGrantLifecycleState state = TenantGrantLifecycleState.Pending,
        TenantGrantAccess operations = TenantGrantAccess.Read,
        string granter = TenantId,
        string grantee = OtherTenantId) =>
        new()
        {
            GrantId = "grant-1",
            GranterTenantId = granter,
            GranteeTenantId = grantee,
            Scope = Scope,
            Operations = operations,
            State = state,
        };

    public static TenantGrantReport GrantReport(string tenantId = TenantId) => new()
    {
        TenantId = tenantId,
        Issued = [Grant(TenantGrantLifecycleState.Pending)],
        Received = [Grant(TenantGrantLifecycleState.Active, TenantGrantAccess.ReadWrite, OtherTenantId, tenantId)],
    };
}
