using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The delegated-access world (epic #4154): tenancy with delegated tenant access
/// administration switched on, where <see cref="WorldIdentities.GlobexAdmin"/>
/// administers globex and nothing else.
/// </summary>
internal sealed partial class ExplorerWorld
{
    /// <summary>The tenant <see cref="WorldIdentities.GlobexAdmin"/> administers in the delegated-access world.</summary>
    public const string Globex = "globex";

    /// <summary>The other tenant of the delegated-access world, which globex's administrator must never see.</summary>
    public const string Acme = "acme";

    /// <summary>globex's orders tree (tenant-local name); no rule reaches it until a journey grants one.</summary>
    public const string GlobexOrdersTree = "orders";

    /// <summary>globex's invoices tree (tenant-local name), where a Platform rule shadows globex's own rule.</summary>
    public const string GlobexInvoicesTree = "invoices";

    /// <summary>globex's own group that holds <see cref="WorldIdentities.Alice"/> and is in globex's member set.</summary>
    public const string GlobexReadersGroup = "readers";

    /// <summary>globex's own rule granting <see cref="GlobexReadersGroup"/> Read on its invoices.</summary>
    public const string GlobexReadersRuleId = "readers-read-invoices";

    /// <summary>The Platform rule that denies <see cref="WorldIdentities.Alice"/> globex's invoices, deciding before globex's rule.</summary>
    public const string GlobexPlatformRuleId = "platform-deny-alice-globex-invoices";

    /// <summary>acme's own group, which globex's administrator must never see.</summary>
    public const string AcmeGroup = "acme-team";

    /// <summary>
    /// Starts a world that serves tenancy with delegated tenant access administration
    /// switched on: globex administered by <see cref="WorldIdentities.GlobexAdmin"/>
    /// beside the operator, acme by the operator alone.
    /// </summary>
    public static Task<ExplorerWorld> StartWithDelegatedAccessAsync() => StartCoreAsync(tenancy: true, delegatedAccess: true);

    /// <summary>
    /// The tenant directory facade as <paramref name="user"/> sees it over the world's
    /// gRPC surface, asserting <paramref name="tenant"/> as the Explorer would.
    /// </summary>
    /// <param name="user">The caller.</param>
    /// <param name="tenant">The asserted tenant.</param>
    public ILatticeTenantDirectoryAdmin TenantDirectory(string user, string tenant) =>
        LatticeTenantAdminApiGrpcClient.Create(Invoker(user, tenant), Head.Services);

    /// <summary>
    /// The tenant policy facade as <paramref name="user"/> sees it over the world's
    /// gRPC surface, asserting <paramref name="tenant"/> as the Explorer would.
    /// </summary>
    /// <param name="user">The caller.</param>
    /// <param name="tenant">The asserted tenant.</param>
    public ILatticeTenantPolicyAdmin TenantPolicy(string user, string tenant) =>
        LatticeTenantAdminApiGrpcClient.Create(Invoker(user, tenant), Head.Services);

    /// <summary>
    /// Removes globex's group <paramref name="name"/> if it exists - with its member-set
    /// entry and the globex rules that name it - so a journey that creates it starts
    /// from the same state however often the world has run it.
    /// </summary>
    /// <param name="name">The group's local name.</param>
    public async Task RemoveGlobexGroupAsync(string name)
    {
        using (UseOperator())
        {
            await Head.Services.GetRequiredService<ILatticeTenantDirectoryAdmin>().RemoveGroupAsync(Globex, name);
        }
    }

    private async Task SeedDelegatedAccessAsync()
    {
        var services = Head.Services;
        using (UseOperator())
        {
            var admin = services.GetRequiredService<ILatticeTenantAdmin>();
            await admin.CreateTenantAsync(Acme, [WorldIdentities.Admin]);
            await admin.CreateTenantAsync(Globex, [WorldIdentities.GlobexAdmin, WorldIdentities.Admin]);
        }

        var grains = services.GetRequiredService<IGrainFactory>();
        using (LatticeSystemOrigin.Enter())
        {
            foreach (var (tree, prefix, count) in new[]
            {
                ($"t/{Globex}/{GlobexOrdersTree}", "order", 3),
                ($"t/{Globex}/{GlobexInvoicesTree}", "invoice", 2),
                ($"t/{Acme}/{GlobexOrdersTree}", "acme-order", 2),
            })
            {
                var lattice = grains.GetGrain<ILattice>(tree);
                for (var i = 1; i <= count; i++)
                {
                    await lattice.SetAsync($"{prefix}-{i:D3}", Encoding.UTF8.GetBytes($"{{\"n\":{i}}}"));
                }
            }

            // The Platform layer decides first: alice may not read globex's invoices,
            // whatever globex's own rules say.
            var store = services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
            await store.PutRuleAsync(new LatticeAuthorizationRule(
                ruleId: GlobexPlatformRuleId,
                subject: LatticeSubjectSelector.User(WorldIdentities.Alice),
                scope: LatticeScope.Tree($"t/{Globex}/{GlobexInvoicesTree}"),
                operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                effect: LatticeEffect.Deny));

            // globex's administrator works with globex's data, as the sample's tenant
            // administrators do, so its trees are listed for the rule and explain pickers.
            foreach (var tree in new[] { GlobexOrdersTree, GlobexInvoicesTree })
            {
                await store.PutRuleAsync(new LatticeAuthorizationRule(
                    ruleId: $"globex-admin-reads-{tree}",
                    subject: LatticeSubjectSelector.User(WorldIdentities.GlobexAdmin),
                    scope: LatticeScope.Tree($"t/{Globex}/{tree}"),
                    operations: LatticeOperation.Read | LatticeOperation.RangeRead,
                    effect: LatticeEffect.Allow));
            }
        }

        using (UseOperator())
        {
            var directory = services.GetRequiredService<ILatticeTenantDirectoryAdmin>();
            var policy = services.GetRequiredService<ILatticeTenantPolicyAdmin>();

            await directory.UpsertGroupAsync(Acme, new TenantGroupDescriptor { Name = AcmeGroup, DisplayName = "Acme team" });

            await directory.UpsertGroupAsync(Globex, new TenantGroupDescriptor { Name = GlobexReadersGroup, DisplayName = "Globex readers" });
            await directory.AddGroupMemberAsync(Globex, GlobexReadersGroup, WorldIdentities.Alice, TenantSubjectKind.User);
            await directory.AddMemberAsync(Globex, GlobexReadersGroup, TenantSubjectKind.TenantGroup);
            await policy.PutRuleAsync(Globex, new TenantRuleDraft
            {
                RuleId = GlobexReadersRuleId,
                SubjectId = GlobexReadersGroup,
                SubjectKind = TenantSubjectKind.TenantGroup,
                ScopeKind = TenantRuleScopeKind.Tree,
                TreeName = GlobexInvoicesTree,
                Operations = LatticeOperation.Read | LatticeOperation.RangeRead,
                Effect = LatticeEffect.Allow,
            });
        }
    }

    private static IDisposable UseOperator() =>
        LatticeCredentialContext.Use(
            Convert.ToBase64String(Encoding.UTF8.GetBytes(WorldIdentities.Admin + ":" + WorldIdentities.Password)),
            scheme: TrustedUserAuthenticator.Scheme);
}
