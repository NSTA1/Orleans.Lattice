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

        // abe: an acme member with no rule, the witness that membership alone grants nothing.
        if (!(await tenants.ListMembersAsync(acme, new())).Entries.Any(member => member.SubjectId == Personas.Abe.Id))
        {
            await tenants.AddMemberAsync(acme, Personas.Abe.Id);
        }

        await tenants.UpsertGroupAsync(acme, new TenantGroupDescriptor { Name = Names.Groups.AcmeAnalysts, DisplayName = "Acme analysts" });
        if (!(await tenants.ListGroupMembersAsync(acme, Names.Groups.AcmeAnalysts)).Any(member => member.MemberId == Personas.Ari.Id))
        {
            await tenants.AddGroupMemberAsync(acme, Names.Groups.AcmeAnalysts, Personas.Ari.Id);
        }

        await tenants.PutRuleAsync(acme, FabrikamAccess.AcmeAnalystsRead);

        await ada.TreeAdmin.CreateTreeAsync(Names.Trees.AcmeInventory);
        await ContractCalls.SetManyAsync(ada.Data, Names.Trees.AcmeInventory, FabrikamAccess.Inventory);
        await AtQuotaAsync(context);
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

        // Membership confers no data rights of its own: ari is an acme member whose only rule is
        // acme-analysts' read, so a write to acme's trees is refused. (hal, also a member, can't
        // prove this: as a bootstrap administrator every operation is allowed to him by design,
        // before membership is consulted.)
        var ari = ContractFacades.Of(context, Personas.Ari.Id, region, acme).Data;
        Require(await WriteRefusedAsync(ari), "ari, an acme member with a read rule only, may not write acme-inventory");
        var abe = ContractFacades.Of(context, Personas.Abe.Id, region, acme).Data;
        Require(await ReadRefusedAsync(abe), "abe, an acme member with no rule, may not read acme-inventory");
        Require(await WriteRefusedAsync(abe), "abe, an acme member with no rule, may not write acme-inventory");

        Require(await AtQuotaAsync(context), "acme is at its 5-tree quota");
    }

    // Tree-count usage is a metered sample (TenantUsageAccountingOptions.MeterInterval, 30 s by
    // default), so it trails a create by up to one interval: wait for the meter, bounded.
    private static async Task<bool> AtQuotaAsync(ChapterContext context)
    {
        var tenants = ContractFacades.Of(context, Personas.Hal.Id).TenantAdmin;
        try
        {
            await ContractWait.UntilAsync("acme's metered tree count to reach its quota", async () =>
            {
                var usage = await tenants.GetTenantQuotaUsageAsync(Names.Tenants.Acme);
                return usage.HasUsage && usage.TreeCount.Usage == FabrikamAccess.AcmeQuotas.MaxTreeCount;
            }, MeterPatience);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    private static readonly TimeSpan MeterPatience = TimeSpan.FromMinutes(2);

    private static async Task<bool> ReadRefusedAsync(Orleans.Lattice.Api.Data.Grpc.LatticeDataApiGrpcClient data)
    {
        // A denied read is not an error: a point read of a hidden key reports it absent, and a
        // range read prunes to the authorized subset (Orleans.Lattice.Api.Data). The key exists
        // (ada reads all of the inventory), so absent and an empty range mean refused.
        try
        {
            return await ContractCalls.GetAsync(data, Names.Trees.AcmeInventory, FabrikamAccess.Inventory[0].Key) is null
                && await ContractCalls.CountPrefixAsync(data, Names.Trees.AcmeInventory, "item/") == 0;
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.PermissionDenied)
        {
            return true;
        }
    }

    private static async Task<bool> WriteRefusedAsync(Orleans.Lattice.Api.Data.Grpc.LatticeDataApiGrpcClient data)
    {
        try
        {
            await ContractCalls.SetManyAsync(data, Names.Trees.AcmeInventory, [new StoryEntry("probe/membership-write", "{}")]);
            return false;
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.PermissionDenied)
        {
            return true;
        }
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
