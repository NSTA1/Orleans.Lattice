using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Samples.Explorer.TaskBoard;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// The console asserts the tenant its address names on every cluster call, so a
/// tenant admin's Data lists that tenant's trees, an operator's Apps lists and
/// installs in the tenant the address names, and the reserved default tenant is
/// reached by asserting none.
/// </summary>
public sealed partial class EstateSmokeTests
{
    [Test]
    public async Task A_tenant_admins_console_lists_its_own_tenants_trees()
    {
        await using var circuit = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.AcmeAdmin, SampleIdentities.AcmeTenant);

        var trees = await circuit.ListTreesAsync();

        Assert.That(
            trees,
            Does.Contain(LatticeTenantTrees.Compose(TenantId.Parse(SampleIdentities.AcmeTenant), SampleIdentities.TenantOrdersTree)),
            "acme-admin at /t/acme/data sees acme's orders tree");
    }

    [Test]
    public async Task An_operators_console_lists_and_installs_apps_in_the_tenant_its_address_names()
    {
        await using var circuit = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.Administrator, SampleIdentities.AcmeTenant);
        var acmeApps = await circuit.Apps.ListAsync();
        var board = acmeApps.Apps.Single(app => app.Slug == TaskBoardApp.Slug);

        circuit.ScopeTo(SampleIdentities.GlobexTenant);
        var globexBefore = await circuit.ListInstalledAppsAsync();
        var installed = await circuit.Apps.InstallAsync(new AppInstallRequest
        {
            Slug = board.Slug,
            Version = board.Version,
            SourceKey = board.Provenance.Source,
            Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = LatticeOperation.Read | LatticeOperation.RangeRead },
        });
        var globexAfter = await circuit.ListInstalledAppsAsync();
        circuit.ScopeTo(TenantId.DefaultId);
        var defaultAfter = await circuit.ListInstalledAppsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(globexBefore, Does.Not.Contain(TaskBoardApp.Slug), "globex had no task board before");
            Assert.That(installed.State, Is.EqualTo(AppLifecycleState.Installed));
            Assert.That(globexAfter, Does.Contain(TaskBoardApp.Slug), "the install at /t/globex/apps landed in globex");
            Assert.That(defaultAfter, Does.Not.Contain(TaskBoardApp.Slug), "and not in the default tenant");
        });
    }
}
