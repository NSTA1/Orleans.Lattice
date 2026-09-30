using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Samples.Explorer.TaskBoard;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// The console asserts the tenant its address names on every cluster call, so a
/// tenant admin's Data lists that tenant's trees, an operator's Apps lists and
/// installs in the tenant the address names, and the reserved default tenant is
/// reached by asserting none. The operator's Tenancy and Access stops open their
/// pages from the default tenant.
/// </summary>
public sealed partial class EstateSmokeTests
{
    [Test]
    public async Task The_operators_tenancy_and_access_stops_open_their_pages_from_the_default_tenant()
    {
        var home = await SampleTestHost.GetHomeAsync(_sample);
        var targets = DirectorySpine.ReadTargets(home);

        var (tenancyAddress, tenancy) = await SampleTestHost.GetPageAsync(_sample, targets["tenancy"]);
        var (accessAddress, access) = await SampleTestHost.GetPageAsync(_sample, targets["access"]);
        var (workspaceAddress, workspace) = await SampleTestHost.GetPageAsync(_sample, "t/default/tenancy");

        Assert.Multiple(() =>
        {
            Assert.That(targets["tenancy"], Is.EqualTo("tenancy"), "the operator's Tenancy stop leads to the tenant directory from /t/default");
            Assert.That(tenancyAddress.AbsolutePath, Is.EqualTo("/tenancy"));
            Assert.That(tenancy, Does.Contain("<h1 class=\"lt-shell-page-title\">Tenancy</h1>"), "the tenant directory renders");
            Assert.That(tenancy, Does.Not.Contain("This tenant could not be read"));
            Assert.That(workspaceAddress.AbsolutePath, Is.EqualTo("/tenancy"), "the default tenant's workspace root is the directory");
            Assert.That(workspace, Does.Not.Contain("This tenant could not be read"));
            Assert.That(accessAddress.AbsolutePath, Is.EqualTo("/access"));
            Assert.That(access, Does.Contain("<h1 class=\"lt-shell-page-title\">Access</h1>"), "the Access area renders");
            Assert.That(access, Does.Not.Contain("Nothing lives at this address"));
        });
    }

    [Test]
    public async Task Once_globex_approves_acmes_offer_its_data_lists_acmes_orders_as_shared_and_reads_them()
    {
        var acmeOrders = SampleSeeder.OrdersTree(SampleIdentities.AcmeTenant);
        await using var circuit = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.GlobexAdmin, SampleIdentities.GlobexTenant);

        var offered = (await circuit.Grants.ListGrantsAsync(SampleIdentities.GlobexTenant)).Received
            .Single(grant => grant.GranterTenantId == SampleIdentities.AcmeTenant);
        await circuit.Grants.ApproveGrantAsync(SampleIdentities.AcmeTenant, SampleIdentities.GlobexTenant, offered.Scope);

        // The tenant gate resolves grants from a compiled policy snapshot that a registry write
        // schedules a background rebuild of, so an approval is admitted once that rebuild lands.
        Orleans.Lattice.Api.State.Grpc.EntryGetResponse read = null!;
        var admitted = await SampleTestHost.EventuallyAsync(
            async () => (read = await circuit.ReadAsync(acmeOrders, "order-1001")).Status == Orleans.Lattice.Api.State.StateQueryStatus.Found,
            GrantBudget);
        var (address, data) = await SampleTestHost.GetPageAsync(_sample, "t/globex/data");

        Assert.Multiple(() =>
        {
            Assert.That(offered.Scope, Is.EqualTo(acmeOrders), "acme offers its orders by their full tree id, which the tenant gate matches");
            Assert.That(admitted, Is.True, $"globex reads acme's orders by the granted id, not re-rooted into globex, within {GrantBudget.TotalSeconds}s of approving (last status {read.Status})");
            Assert.That(read.Entry, Is.Not.Null);
            Assert.That(address.AbsolutePath, Is.EqualTo("/t/globex/data"));
            Assert.That(data, Does.Contain($"href=\"t/globex/data/{acmeOrders}\""), "globex's directory links acme's orders under globex's own root");
            Assert.That(data, Does.Contain("Shared tree").And.Contain("Read only"));
        });
    }

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
