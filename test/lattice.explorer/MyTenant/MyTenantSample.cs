using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// Fixed Explorer-term sample data for the My Tenant plugin's tests.
/// <para>
/// Every figure here is a literal. No test in this fixture reads a clock, waits
/// on an ordering, or drives a live sampler: a quota reading is exactly what
/// this file says it is, so a gauge assertion can never be flaky.
/// </para>
/// </summary>
internal static class MyTenantSample
{
    /// <summary>The tenant the workspace under test administers.</summary>
    public const string TenantId = "acme";

    /// <summary>A second tenant, used to prove the isolation boundary.</summary>
    public const string OtherTenantId = "globex";

    /// <summary>A third tenant, party to grants that involve neither of the above.</summary>
    public const string ThirdTenantId = "initech";

    /// <summary>The scope a sample grant covers.</summary>
    public const string Scope = "t/acme/orders";

    /// <summary>Creates a tenant summary.</summary>
    public static ExplorerTenantSummary Summary(
        string tenantId = TenantId,
        ExplorerTenantLifecycle status = ExplorerTenantLifecycle.Active,
        bool isDefault = false) =>
        new(tenantId, status, isDefault);

    /// <summary>Creates a region row.</summary>
    public static ExplorerTenantRegion Region(
        string regionId,
        ExplorerTenantRegionLifecycle status = ExplorerTenantRegionLifecycle.Online,
        bool isAllowed = true) =>
        new(regionId, status, isAllowed);

    /// <summary>
    /// Regions where one is allowed and resident, one is allowed but not
    /// resident, and one is neither - so a test can prove the two-set model is
    /// not collapsed into a single flag.
    /// </summary>
    public static IReadOnlyList<ExplorerTenantRegion> Regions() =>
    [
        Region("eastus", ExplorerTenantRegionLifecycle.None, isAllowed: true),
        Region("northeurope", ExplorerTenantRegionLifecycle.None, isAllowed: false),
        Region("westeurope", ExplorerTenantRegionLifecycle.Online, isAllowed: true),
    ];

    /// <summary>A tenant resident in exactly one region, so the last-region invariant bites.</summary>
    public static IReadOnlyList<ExplorerTenantRegion> SingleResidencyRegions() =>
    [
        Region("eastus", ExplorerTenantRegionLifecycle.None, isAllowed: true),
        Region("westeurope", ExplorerTenantRegionLifecycle.Online, isAllowed: true),
    ];

    /// <summary>A tenant resident in two regions, so a removal is permitted.</summary>
    public static IReadOnlyList<ExplorerTenantRegion> DualResidencyRegions() =>
    [
        Region("eastus", ExplorerTenantRegionLifecycle.Online, isAllowed: true),
        Region("westeurope", ExplorerTenantRegionLifecycle.Online, isAllowed: true),
    ];

    /// <summary>Creates a tenant detail carrying the standard region set.</summary>
    public static ExplorerTenantDetail Detail(
        string tenantId = TenantId,
        ExplorerTenantLifecycle status = ExplorerTenantLifecycle.Active,
        bool isDefault = false,
        IReadOnlyList<ExplorerTenantRegion>? regions = null) =>
        new()
        {
            TenantId = tenantId,
            Status = status,
            IsDefault = isDefault,
            Regions = regions ?? Regions(),
            Quotas = Limits(),
        };

    /// <summary>
    /// Ceilings where two dimensions are bounded, one is bounded at exactly
    /// zero, and two are unbounded.
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
    /// A reading that exercises every distinction at once, so one fixture proves
    /// none of the four cases is flattened into another:
    /// <list type="bullet">
    ///   <item><description>Bytes - bounded and measured, so a real bar.</description></item>
    ///   <item><description>Keys - bounded and measured at exactly zero.</description></item>
    ///   <item><description>MemoryBytes - a ceiling of zero with usage against it, so all overage.</description></item>
    ///   <item><description>TreeCount - measured with no ceiling at all.</description></item>
    ///   <item><description>OpsPerSecond - a ceiling with no usage sample.</description></item>
    /// </list>
    /// </summary>
    public static ExplorerTenantQuotaUsage Usage(
        string tenantId = TenantId,
        ExplorerTenantQuotaEnforcement scope = ExplorerTenantQuotaEnforcement.GlobalConverged,
        bool hasUsage = true) =>
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
            TreeCount = new ExplorerTenantQuotaDimension { Usage = 3, Limit = null, BurstLimit = null },
            OpsPerSecond = new ExplorerTenantQuotaDimension { Usage = null, Limit = 900, BurstLimit = 990 },
            BurstPercent = 10,
            Limits = Limits(),
        };

    /// <summary>Creates a grant.</summary>
    public static ExplorerTenantGrant Grant(
        string granter = TenantId,
        string grantee = OtherTenantId,
        ExplorerTenantGrantState state = ExplorerTenantGrantState.Pending,
        ExplorerTenantGrantAccess operations = ExplorerTenantGrantAccess.Read,
        string scope = Scope,
        string grantId = "grant-1") =>
        new(grantId, granter, grantee, scope, operations, state);

    /// <summary>
    /// A grant report for the tenant under test: one pending offer it made, and
    /// one pending offer made to it - the latter being the row whose approval is
    /// the step that makes a grant live.
    /// </summary>
    public static ExplorerTenantGrants Grants(string tenantId = TenantId) => new()
    {
        TenantId = tenantId,
        Issued = [Grant(granter: tenantId, grantee: OtherTenantId, grantId: "issued-1")],
        Received =
        [
            Grant(
                granter: OtherTenantId,
                grantee: tenantId,
                state: ExplorerTenantGrantState.Pending,
                operations: ExplorerTenantGrantAccess.ReadWrite,
                grantId: "received-1"),
        ],
    };
}
