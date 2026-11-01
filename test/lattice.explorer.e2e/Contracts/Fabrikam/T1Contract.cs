using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Fabrikam;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Fabrikam;

/// <summary>
/// Tenant onboarding (T1, #5393), established as hal. Produces <c>acme</c> (admin ada; quotas
/// <see cref="FabrikamAccess.AcmeQuotas"/>; allowed west and east; resident and Online in west;
/// hal a delegated-access member entry, not an admin)
/// and <c>globex</c> (admin gus; <see cref="FabrikamAccess.GlobexQuotas"/>; allowed and resident
/// in west), the platform rules <see cref="FabrikamAccess.T1Rules"/>, and each tenant admin's
/// tenant-tier rule <see cref="FabrikamAccess.T1TenantRules"/>, authored by that admin (a
/// tenant-wide rule belongs to the tenant tier). T5 moves acme east and deletes globex.
/// </summary>
public sealed class T1Contract() : ChapterContract("T1")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context, Cast.Personas.Hal.Id);
        var tenants = facades.TenantAdmin;
        await OnboardAsync(tenants, Names.Tenants.Acme, Cast.Personas.Ada.Id, FabrikamAccess.AcmeQuotas, [FabrikamAccess.West, FabrikamAccess.East]);
        await OnboardAsync(tenants, Names.Tenants.Globex, Cast.Personas.Gus.Id, FabrikamAccess.GlobexQuotas, [FabrikamAccess.West]);

        // hal joins acme as a delegated-access member entry (not an admin), so he can act in
        // the tenant he supports: install its apps (T7) and visit it (T4).
        var members = await tenants.ListMembersAsync(Names.Tenants.Acme, new());
        if (!members.Entries.Any(member => member.SubjectId == Cast.Personas.Hal.Id))
        {
            await tenants.AddMemberAsync(Names.Tenants.Acme, Cast.Personas.Hal.Id);
        }
        foreach (var rule in FabrikamAccess.T1Rules)
        {
            await ContractCalls.PutRuleAsync(facades.Auth, rule);
        }

        foreach (var (tenant, admin, rule) in FabrikamAccess.T1TenantRules)
        {
            await ContractFacades.Of(context, admin, tenant: tenant).TenantAdmin.PutRuleAsync(tenant, rule);
        }
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context, Cast.Personas.Hal.Id);
        var tenants = facades.TenantAdmin;
        var moved = context.IsProduced("T5");

        await VerifyTenantAsync(tenants, Names.Tenants.Acme, Cast.Personas.Ada.Id, FabrikamAccess.AcmeQuotas);
        Require(
            (await tenants.ListMembersAsync(Names.Tenants.Acme, new())).Entries.Any(member => member.SubjectId == Cast.Personas.Hal.Id),
            "hal is an acme member entry");
        Require(
            !(await tenants.ListTenantAdminSubjectsAsync(Names.Tenants.Acme)).Subjects.Contains(Cast.Personas.Hal.Id),
            "hal is not an acme admin subject");
        if (!moved)
        {
            var acme = await tenants.GetTenantRegionStatusAsync(Names.Tenants.Acme);
            Require(Allowed(acme).SetEquals([FabrikamAccess.West, FabrikamAccess.East]), "acme may use west and east");
            Require(Status(acme, FabrikamAccess.West) == TenantRegionLifecycleStatus.Online, "acme is resident and online in west");

            await VerifyTenantAsync(tenants, Names.Tenants.Globex, Cast.Personas.Gus.Id, FabrikamAccess.GlobexQuotas);
            var globex = await tenants.GetTenantRegionStatusAsync(Names.Tenants.Globex);
            Require(Allowed(globex).SetEquals([FabrikamAccess.West]), "globex may use west only");
        }

        foreach (var rule in FabrikamAccess.T1Rules)
        {
            Require(await ContractCalls.RuleHoldsAsync(facades.Auth, rule), $"platform rule {rule.RuleId} on {rule.TreeId} allows {rule.Operations} to {rule.Subject.Id}");
        }

        foreach (var (tenant, admin, draft) in FabrikamAccess.T1TenantRules)
        {
            if (moved && tenant == Names.Tenants.Globex)
            {
                continue;
            }

            var rule = await ContractFacades.Of(context, admin, tenant: tenant).TenantAdmin.GetRuleAsync(tenant, draft.RuleId);
            Require(
                rule is { ScopeKind: TenantRuleScopeKind.TenantWide } && rule.SubjectId == admin && (rule.Operations & draft.Operations) == draft.Operations,
                $"tenant rule {draft.RuleId} in {tenant} allows {draft.Operations} to {admin} on every {tenant} tree");
        }
    }

    /// <summary>The regions a tenant may use.</summary>
    /// <param name="report">The tenant's region report.</param>
    public static HashSet<string> Allowed(TenantRegionStatusReport report) =>
        [.. report.Regions.Where(region => region.IsAllowed).Select(region => region.RegionId)];

    /// <summary>A region's lifecycle for a tenant, <see cref="TenantRegionLifecycleStatus.None"/> when unlisted.</summary>
    /// <param name="report">The tenant's region report.</param>
    /// <param name="region">The region.</param>
    public static TenantRegionLifecycleStatus Status(TenantRegionStatusReport report, string region) =>
        report.Regions.FirstOrDefault(entry => entry.RegionId == region)?.Status ?? TenantRegionLifecycleStatus.None;

    private static async Task OnboardAsync(
        Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantAdminApiGrpcClient tenants, string tenant, string admin, TenantQuotasDescriptor quotas, string[] allowed)
    {
        if (!(await tenants.ListTenantAdminSubjectsTolerantAsync(tenant)).Contains(admin))
        {
            await tenants.CreateTenantAsync(tenant, [admin]);
        }

        await tenants.SetTenantQuotasAsync(tenant, quotas);
        await tenants.AuthorizeAllowedRegionsAsync(tenant, allowed);
        if (Status(await tenants.GetTenantRegionStatusAsync(tenant), FabrikamAccess.West) != TenantRegionLifecycleStatus.Online)
        {
            await tenants.SetTenantResidencyAsync(tenant, [FabrikamAccess.West]);
        }

        await ContractWait.UntilAsync($"{tenant} to be online in west", async () =>
            Status(await tenants.GetTenantRegionStatusAsync(tenant), FabrikamAccess.West) == TenantRegionLifecycleStatus.Online);
    }

    private async Task VerifyTenantAsync(
        Orleans.Lattice.Api.TenantAdmin.Grpc.LatticeTenantAdminApiGrpcClient tenants, string tenant, string admin, TenantQuotasDescriptor quotas)
    {
        Require((await tenants.ListTenantAdminSubjectsAsync(tenant)).Subjects.Contains(admin), $"{admin} administers {tenant}");
        var usage = await tenants.GetTenantQuotaUsageAsync(tenant);
        RequireEqual(usage.Quotas.MaxTreeCount, quotas.MaxTreeCount, $"{tenant}'s tree quota");
        RequireEqual(usage.Quotas.MaxBytes, quotas.MaxBytes, $"{tenant}'s storage ceiling");
    }
}
