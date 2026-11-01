using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Fabrikam;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Fabrikam;

/// <summary>
/// Region lifecycle (T6, #5399). Its stop-serving, held-backfill override and staged-then-reset
/// residency edit leave one state: acme served from east only, with no residency change in
/// flight (no region Provisioning, Backfilling or Draining). The seeder waits for that state.
/// </summary>
public sealed class T6Contract() : ChapterContract("T6")
{
    /// <inheritdoc />
    public override Task EstablishAsync(ChapterContext context) =>
        ContractWait.UntilAsync("acme's residency to settle", () => SettledAsync(context), TimeSpan.FromMinutes(6));

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var acme = await ContractFacades.Of(context, Personas.Hal.Id).TenantAdmin.GetTenantRegionStatusAsync(Names.Tenants.Acme);
        RequireEqual(T1Contract.Status(acme, FabrikamAccess.East), TenantRegionLifecycleStatus.Online, "acme is served from east");
        Require(!acme.Regions.Any(region => region.RegionId != FabrikamAccess.East && region.Status == TenantRegionLifecycleStatus.Online), "acme is served from east only");
        Require(await SettledAsync(context), "no residency change is in flight");
    }

    private static async Task<bool> SettledAsync(ChapterContext context)
    {
        var acme = await ContractFacades.Of(context, Personas.Hal.Id).TenantAdmin.GetTenantRegionStatusAsync(Names.Tenants.Acme);
        return !acme.Regions.Any(region => region.Status is TenantRegionLifecycleStatus.Provisioning or TenantRegionLifecycleStatus.Backfilling or TenantRegionLifecycleStatus.Draining);
    }
}
