using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Api.TenantAdmin.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Fabrikam;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Fabrikam;

/// <summary>
/// Residency change and tenant lifecycle (T5, #5398), established as hal. Produces acme resident
/// and Online in east only, west drained to Removed and no longer allowed (the documented
/// sequence: add east and let it backfill Online, drop west and let it drain, then revoke it);
/// and globex suspended, resumed and deleted.
/// </summary>
public sealed class T5Contract() : ChapterContract("T5")
{
    private static readonly TimeSpan RegionPatience = TimeSpan.FromMinutes(6);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var tenants = ContractFacades.Of(context, Personas.Hal.Id).TenantAdmin;
        var acme = Names.Tenants.Acme;

        if (T1Contract.Status(await tenants.GetTenantRegionStatusAsync(acme), FabrikamAccess.East) != TenantRegionLifecycleStatus.Online)
        {
            await tenants.SetTenantResidencyAsync(acme, [FabrikamAccess.West, FabrikamAccess.East]);
            await WaitForAsync(tenants, acme, FabrikamAccess.East, TenantRegionLifecycleStatus.Online);
        }

        if (T1Contract.Status(await tenants.GetTenantRegionStatusAsync(acme), FabrikamAccess.West) is not (TenantRegionLifecycleStatus.Removed or TenantRegionLifecycleStatus.None))
        {
            await tenants.SetTenantResidencyAsync(acme, [FabrikamAccess.East]);
            await WaitForAsync(tenants, acme, FabrikamAccess.West, TenantRegionLifecycleStatus.Removed);
        }

        await tenants.AuthorizeAllowedRegionsAsync(acme, [FabrikamAccess.East]);

        if (await tenants.TenantExistsAsync(Names.Tenants.Globex))
        {
            await tenants.SuspendTenantAsync(Names.Tenants.Globex);
            await tenants.ResumeTenantAsync(Names.Tenants.Globex);
            await tenants.DeleteTenantAsync(Names.Tenants.Globex);
        }
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var tenants = ContractFacades.Of(context, Personas.Hal.Id).TenantAdmin;
        var acme = await tenants.GetTenantRegionStatusAsync(Names.Tenants.Acme);
        Require(T1Contract.Allowed(acme).SetEquals([FabrikamAccess.East]), "acme may use east only");
        RequireEqual(T1Contract.Status(acme, FabrikamAccess.East), TenantRegionLifecycleStatus.Online, "acme is resident and online in east");
        Require(T1Contract.Status(acme, FabrikamAccess.West) is TenantRegionLifecycleStatus.Removed or TenantRegionLifecycleStatus.None, "acme is no longer served in west");
        Require(!await tenants.TenantExistsAsync(Names.Tenants.Globex), "globex is deleted");
    }

    private static Task WaitForAsync(LatticeTenantAdminApiGrpcClient tenants, string tenant, string region, TenantRegionLifecycleStatus status) =>
        ContractWait.UntilAsync($"{tenant} to be {status} in {region}", async () =>
            T1Contract.Status(await tenants.GetTenantRegionStatusAsync(tenant), region) == status, RegionPatience);
}
