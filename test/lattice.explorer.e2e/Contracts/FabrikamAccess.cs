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

    /// <summary>What a tenant administrator's platform rule allows on its tenant's trees (T1).</summary>
    public const LatticeOperation TenantAdministration =
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write | LatticeOperation.Delete
        | LatticeOperation.BulkLoad | LatticeOperation.Admin | LatticeOperation.SchemaAdmin | LatticeOperation.Backup
        | LatticeOperation.Restore | LatticeOperation.Replication | LatticeOperation.TreeLifecycle;

    /// <summary>
    /// T1's platform rules, one record per governed tree: each tenant administrator's
    /// administration of <c>t/{tenant}/*</c>, and the scopeless Telemetry capability, which can
    /// only be granted cluster-wide; and <c>dee-default-read</c>, dee reading the all-trees tier.
    /// Apps are installed into a tenant by the platform operator (T7), so no rule grants App install.
    /// </summary>
    public static IReadOnlyList<PinnedRule> T1Rules { get; } =
    [
        User(Names.Rules.AcmeAdminsPlatform, Personas.Ada.Id, TenantWide(Names.Tenants.Acme), TenantAdministration),
        User(Names.Rules.AcmeAdminsPlatform, Personas.Ada.Id, LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry),
        User(Names.Rules.GlobexAdminsPlatform, Personas.Gus.Id, TenantWide(Names.Tenants.Globex), TenantAdministration),
        User(Names.Rules.GlobexAdminsPlatform, Personas.Gus.Id, LatticeScope.ClusterWideTreeId, LatticeOperation.Telemetry),
        User(Names.Rules.DeeDefaultRead, Personas.Dee.Id, LatticeScope.ClusterWideTreeId, ContosoAccess.ReadOps),
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

    /// <summary>The four acme trees the harness seeds in T2, modelling acme's existing use, so acme reaches its 5-tree quota.</summary>
    public static IReadOnlyList<string> AcmeExistingTrees { get; } = ["acme-orders", "acme-invoices", "acme-returns", "acme-suppliers"];

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

    private static string TenantWide(string tenant) => $"t/{tenant}/*";

    private static PinnedRule User(string ruleId, string userId, string treeId, LatticeOperation operations) =>
        new(ruleId, LatticeSubjectSelector.User(userId), treeId, operations);
}
