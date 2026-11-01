using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Fabrikam;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Fabrikam;

/// <summary>
/// A tenant administrator's first week (T2, #5394), established as ada acting in acme. Consumes
/// acme's existing use (<see cref="SeedAcmeExistingTreesAsync"/>, off-UI). Produces the tenant
/// group <c>acme-analysts</c>{ari} with its tenant-wide read rule
/// <see cref="FabrikamAccess.AcmeAnalystsRead"/>, <c>acme-inventory</c> seeded with
/// <see cref="FabrikamAccess.Inventory"/>, and acme at its 5-tree quota.
/// </summary>
public sealed class T2Contract() : ChapterContract("T2")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        await SeedAcmeExistingTreesAsync(context.Estate);
        var ada = ContractFacades.Of(context, Personas.Ada.Id, tenant: Names.Tenants.Acme);
        var tenants = ada.TenantAdmin;
        var acme = Names.Tenants.Acme;

        try
        {
            await tenants.AddMemberAsync(acme, Personas.Ari.Id);
        }
        catch (Grpc.Core.RpcException ex) when (!ContractWait.IsTransient(ex.StatusCode))
        {
            // Already a member.
        }

        await tenants.UpsertGroupAsync(acme, new TenantGroupDescriptor { Name = Names.Groups.AcmeAnalysts, DisplayName = "Acme analysts" });
        if (!(await tenants.ListGroupMembersAsync(acme, Names.Groups.AcmeAnalysts)).Any(member => member.MemberId == Personas.Ari.Id))
        {
            await tenants.AddGroupMemberAsync(acme, Names.Groups.AcmeAnalysts, Personas.Ari.Id);
        }

        await tenants.PutRuleAsync(acme, FabrikamAccess.AcmeAnalystsRead);

        await ada.TreeAdmin.CreateTreeAsync(Names.Trees.AcmeInventory);
        await ContractCalls.SetManyAsync(ada.Data, Names.Trees.AcmeInventory, FabrikamAccess.Inventory);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var region = context.IsProduced("T5") ? FabrikamAccess.East : FabrikamAccess.West;
        var ada = ContractFacades.Of(context, Personas.Ada.Id, region, Names.Tenants.Acme);
        var acme = Names.Tenants.Acme;

        Require((await ada.TenantAdmin.ListGroupMembersAsync(acme, Names.Groups.AcmeAnalysts)).Any(member => member.MemberId == Personas.Ari.Id), "acme-analysts holds ari");
        var rule = await ada.TenantAdmin.GetRuleAsync(acme, FabrikamAccess.AcmeAnalystsRead.RuleId);
        Require(
            rule is { SubjectId: Names.Groups.AcmeAnalysts, ScopeKind: TenantRuleScopeKind.TenantWide } && (rule.Operations & ContosoAccess.ReadOps) == ContosoAccess.ReadOps,
            "acme-analysts reads every acme tree");
        RequireEqual(await ContractCalls.CountPrefixAsync(ada.Data, Names.Trees.AcmeInventory, "item/"), FabrikamAccess.InventoryRows, "acme-inventory holds the inventory");

        var usage = await ContractFacades.Of(context, Personas.Hal.Id).TenantAdmin.GetTenantQuotaUsageAsync(acme);
        RequireEqual(usage.TreeCount.Usage, FabrikamAccess.AcmeQuotas.MaxTreeCount, "acme is at its 5-tree quota");
    }

    /// <summary>
    /// T2's consume: acme's existing use - four trees acme already holds
    /// (<see cref="FabrikamAccess.AcmeExistingTrees"/>), created as ada - so the inventory takes
    /// acme to its quota and a sixth tree is refused. The T2 journey calls it before step 1.
    /// </summary>
    /// <param name="estate">The Fabrikam estate.</param>
    public static async Task SeedAcmeExistingTreesAsync(Estate.JourneyEstate estate)
    {
        var admin = ContractFacades.Of(estate, Personas.Ada.Id, tenant: Names.Tenants.Acme).TreeAdmin;
        foreach (var tree in FabrikamAccess.AcmeExistingTrees)
        {
            await admin.CreateTreeAsync(tree);
        }
    }
}
