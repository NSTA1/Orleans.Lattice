using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Fabrikam;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Fabrikam;

/// <summary>
/// Cross-tenant sharing (T3, #5396). Produces acme's grant of all of acme
/// (<see cref="FabrikamAccess.RejectedGrantScope"/>, read) to globex, rejected by gus; its grant of
/// <c>acme-inventory</c> (<see cref="FabrikamAccess.RevokedGrantScope"/>, read and write),
/// approved by gus and later revoked by ada; globex's group <c>globex-buyers</c>{gia}; and hal's
/// platform rule <see cref="FabrikamAccess.GiaReadAcmeInventory"/> (the receiving user's read,
/// then write). T5 deletes globex.
/// </summary>
public sealed class T3Contract() : ChapterContract("T3")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var acme = Names.Tenants.Acme;
        var globex = Names.Tenants.Globex;
        var ada = ContractFacades.Of(context, Personas.Ada.Id, tenant: acme).TenantAdmin;
        var gus = ContractFacades.Of(context, Personas.Gus.Id, tenant: globex).TenantAdmin;

        if (!(await ada.IssuedGrantsAsync(acme)).Any(grant => grant.Scope == FabrikamAccess.RejectedGrantScope && grant.State == TenantGrantLifecycleState.Rejected))
        {
            await ada.OfferCrossTenantGrantAsync(acme, globex, FabrikamAccess.RejectedGrantScope, TenantGrantAccess.Read);
            await gus.RejectCrossTenantGrantAsync(acme, globex, FabrikamAccess.RejectedGrantScope);
        }

        var revoked = (await ada.IssuedGrantsAsync(acme)).Any(grant => grant.Scope == FabrikamAccess.RevokedGrantScope && grant.State == TenantGrantLifecycleState.Revoked);
        if (!revoked)
        {
            await ada.OfferCrossTenantGrantAsync(acme, globex, FabrikamAccess.RevokedGrantScope, TenantGrantAccess.Read | TenantGrantAccess.Write);
            await gus.ApproveCrossTenantGrantAsync(acme, globex, FabrikamAccess.RevokedGrantScope);
        }

        try
        {
            await gus.AddMemberAsync(globex, Personas.Gia.Id);
        }
        catch (Grpc.Core.RpcException ex) when (!ContractWait.IsTransient(ex.StatusCode))
        {
            // Already a member.
        }

        await gus.UpsertGroupAsync(globex, new TenantGroupDescriptor { Name = Names.Groups.GlobexBuyers, DisplayName = "Globex buyers" });
        if (!(await gus.ListGroupMembersAsync(globex, Names.Groups.GlobexBuyers)).Any(member => member.MemberId == Personas.Gia.Id))
        {
            await gus.AddGroupMemberAsync(globex, Names.Groups.GlobexBuyers, Personas.Gia.Id);
        }

        await ContractCalls.PutRuleAsync(ContractFacades.Of(context, Personas.Hal.Id).Auth, FabrikamAccess.GiaReadAcmeInventory);

        if (!revoked)
        {
            await ada.RevokeCrossTenantGrantAsync(acme, globex, FabrikamAccess.RevokedGrantScope);
        }
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var hal = ContractFacades.Of(context, Personas.Hal.Id);
        Require(await ContractCalls.RuleHoldsAsync(hal.Auth, FabrikamAccess.GiaReadAcmeInventory), "gia-read-acme-inventory allows gia to read and write acme-inventory");
        if (context.IsProduced("T5"))
        {
            return;
        }

        var grants = await ContractFacades.Of(context, Personas.Ada.Id, tenant: Names.Tenants.Acme).TenantAdmin.IssuedGrantsAsync(Names.Tenants.Acme);
        Require(grants.Any(grant => grant.Scope == FabrikamAccess.RejectedGrantScope && grant.State == TenantGrantLifecycleState.Rejected), "one grant was rejected");
        Require(grants.Any(grant => grant.Scope == FabrikamAccess.RevokedGrantScope && grant.State == TenantGrantLifecycleState.Revoked), "one grant was revoked");
        Require(!grants.Any(grant => grant.State == TenantGrantLifecycleState.Active), "no grant is active");

        var members = await ContractFacades.Of(context, Personas.Gus.Id, tenant: Names.Tenants.Globex).TenantAdmin.ListGroupMembersAsync(Names.Tenants.Globex, Names.Groups.GlobexBuyers);
        Require(members.Any(member => member.MemberId == Personas.Gia.Id), "globex-buyers holds gia");
    }
}
