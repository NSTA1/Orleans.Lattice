using System.Globalization;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Fabrikam;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// The Fabrikam tenancy model, pinned (#5364): the tenants' quotas and regions, the platform
/// rules hal writes in T1 and T3, acme's own group and rule (T2), and the cross-tenant grants
/// (T3). The T journeys author exactly these through the Explorer and the seeders write them
/// through the TenantAdmin and Auth APIs, so both paths produce one world.
/// </summary>
public static class FabrikamAccess
{
    /// <summary>The west region.</summary>
    public const string West = "west";

    /// <summary>The east region.</summary>
    public const string East = "east";

    /// <summary>acme's quotas (T1): 5 trees, a 1 GiB storage ceiling, a 20 percent burst.</summary>
    public static TenantQuotasDescriptor AcmeQuotas { get; } = new() { MaxTreeCount = 5, MaxBytes = 1L << 30, BurstPercent = 20 };

    /// <summary>globex's quotas (T1): 2 trees.</summary>
    public static TenantQuotasDescriptor GlobexQuotas { get; } = new() { MaxTreeCount = 2 };

    /// <summary>
    /// What a tenant administrator's tenant-tier rule allows on every tree of the tenant (T1): the
    /// data-plane operations, the only ones a tenant rule may carry (<c>LatticeAuthOperations.All</c>).
    /// </summary>
    public const LatticeOperation TenantDataAdministration =
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete
        | LatticeOperation.BulkLoad | LatticeOperation.Admin | LatticeOperation.SchemaAdmin | LatticeOperation.Backup
        | LatticeOperation.Restore;

    /// <summary>
    /// What a tenant administrator's platform rule allows on each named tree of the tenant (T1):
    /// the platform-only operations a tenant rule cannot carry.
    /// </summary>
    public const LatticeOperation PlatformTreeAdministration = LatticeOperation.TreeLifecycle | LatticeOperation.Replication;

    /// <summary>The four acme trees the harness seeds in T2, modelling acme's existing use, so acme reaches its 5-tree quota.</summary>
    public static IReadOnlyList<string> AcmeExistingTrees { get; } = ["acme-orders", "acme-invoices", "acme-returns", "acme-suppliers"];

    /// <summary>
    /// Every acme tree a T chapter creates or administers, by acme's name: the four existing
    /// trees, the inventory and the over-quota tree (T2). The platform names each one in its own
    /// rule record, since a platform rule can't target <c>t/acme/*</c>.
    /// </summary>
    public static IReadOnlyList<string> AcmeAdministeredTrees { get; } =
        [.. AcmeExistingTrees, Names.Trees.AcmeInventory, Names.Trees.AcmeOverQuota];

    /// <summary>
    /// T1's platform rules, one record per governed tree. The platform refuses a tenant-wide
    /// <c>t/{tenant}/*</c> rule (tenant-wide rules belong to the tenant tier), and the tenant tier
    /// refuses Telemetry, Replication, TreeLifecycle and AppInstall (platform-only). So ada holds
    /// TreeLifecycle and Replication on each named acme tree. No tenant admin holds Telemetry: it
    /// is authorized only on <c>*</c>, which would expose every tenant's and the platform's
    /// telemetry (tenant-scoped telemetry is a gap, #5582). <c>dee-default-read</c> lets dee read the all-trees tier. Apps are installed into a tenant by
    /// the platform operator (T7), so no rule grants App install. globex names no trees, so gus
    /// holds no platform rule.
    /// </summary>
    public static IReadOnlyList<PinnedRule> T1Rules { get; } =
    [
        .. AcmeAdministeredTrees.Select(tree =>
            User(Names.Rules.AcmeAdminsPlatform, Personas.Ada.Id, Qualified(Names.Tenants.Acme, tree), PlatformTreeAdministration)),
        User(Names.Rules.DeeDefaultRead, Personas.Dee.Id, LatticeScope.ClusterWideTreeId, ContosoAccess.ReadOps),
    ];

    /// <summary>
    /// T1's tenant-tier rules, each authored by its tenant's own administrator: the
    /// administrator's data-plane operations on every tree of the tenant.
    /// </summary>
    public static IReadOnlyList<(string Tenant, string Admin, TenantRuleDraft Rule)> T1TenantRules { get; } =
    [
        (Names.Tenants.Acme, Personas.Ada.Id, AdminRule(Names.Rules.AcmeAdmins, Personas.Ada.Id)),
        (Names.Tenants.Globex, Personas.Gus.Id, AdminRule(Names.Rules.GlobexAdmins, Personas.Gus.Id)),
    ];

    /// <summary>T3's platform rule: the receiving user gia reads, and finally writes, acme's inventory.</summary>
    public static PinnedRule GiaReadAcmeInventory { get; } = User(
        Names.Rules.GiaReadAcmeInventory, Personas.Gia.Id, Names.Trees.AcmeInventoryQualified, ContosoAccess.ReadOps | LatticeOperation.Write);

    /// <summary>acme's tenant rule (T2): <c>acme-analysts</c> reads every acme tree.</summary>
    public static TenantRuleDraft AcmeAnalystsRead { get; } = new()
    {
        RuleId = "acme-analysts-read",
        SubjectId = Names.Groups.AcmeAnalysts,
        SubjectKind = TenantSubjectKind.TenantGroup,
        ScopeKind = TenantRuleScopeKind.TenantWide,
        Operations = ContosoAccess.ReadOps,
        Effect = LatticeEffect.Allow,
    };

    /// <summary>T3's rejected offer: all of acme, read.</summary>
    public const string RejectedGrantScope = "t/" + Names.Tenants.Acme;

    /// <summary>T3's approved, then revoked, grant: acme's inventory, read and write.</summary>
    public const string RevokedGrantScope = Names.Trees.AcmeInventoryQualified;

    /// <summary>The inventory rows T2 imports from <c>inventory.csv</c>: key template <c>item/{sku}</c>, each row a JSON object of its columns as text.</summary>
    public static IReadOnlyList<StoryEntry> Inventory { get; } =
        [.. Enumerable.Range(1, InventoryRows).Select(number =>
        {
            var sku = string.Create(CultureInfo.InvariantCulture, $"SKU-{number:D4}");
            return new StoryEntry(
                "item/" + sku,
                string.Create(CultureInfo.InvariantCulture, $"{{\"sku\":\"{sku}\",\"name\":\"Widget {number}\",\"qty\":\"{(number * 7) % 50}\"}}"));
        })];

    /// <summary>How many rows <c>inventory.csv</c> holds.</summary>
    public const int InventoryRows = 25;

    /// <summary><c>inventory.csv</c>'s content.</summary>
    public static string InventoryCsv => "sku,name,qty\n" + string.Concat(Enumerable.Range(1, InventoryRows).Select(number =>
        string.Create(CultureInfo.InvariantCulture, $"SKU-{number:D4},Widget {number},{(number * 7) % 50}\n")));

    /// <summary>Writes <c>inventory.csv</c> under <paramref name="directory"/> and returns its path.</summary>
    /// <param name="directory">The directory, created if missing.</param>
    public static string WriteInventoryFile(string directory)
    {
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, Names.Files.InventoryCsv);
        File.WriteAllText(path, InventoryCsv);
        return path;
    }

    /// <summary>The tree id a tenant-local name has on the platform: <c>t/{tenant}/{name}</c>.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The tenant-local tree name.</param>
    public static string Qualified(string tenant, string name) => $"t/{tenant}/{name}";

    private static TenantRuleDraft AdminRule(string ruleId, string userId) => new()
    {
        RuleId = ruleId,
        SubjectId = userId,
        SubjectKind = TenantSubjectKind.User,
        ScopeKind = TenantRuleScopeKind.TenantWide,
        Operations = TenantDataAdministration,
        Effect = LatticeEffect.Allow,
    };

    private static PinnedRule User(string ruleId, string userId, string treeId, LatticeOperation operations) =>
        new(ruleId, LatticeSubjectSelector.User(userId), treeId, operations);
}
