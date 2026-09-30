using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Issue #4025: the Backups area lists only the listing tenant's own backups.
/// The fake catalogue answers every tenant's backups, as the cluster does for a
/// platform operator asserting no tenant (the reserved default tenant), so each
/// assertion that another tenant's backup is left out is made against a listing
/// that was handed it.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupsTenantScopeTests : BackupsTestContext
{
    private void UseTenant(string? asserted) =>
        Services.AddSingleton<ILatticeActiveTenantProvider>(new FakeActiveTenantProvider(asserted));

    private void SeedEstate() => Seed(
        FakeBackupControl.Manifest("default-nightly", tree: "orders"),
        FakeBackupControl.Manifest("acme-nightly", tree: "t/acme/orders"),
        FakeBackupControl.Manifest("globex-nightly", tree: "t/globex/orders"),
        FakeBackupControl.Manifest("platform-nightly", tree: "sys-tenant-registry"));

    private IEnumerable<BackupCatalogRequest> ListRequests =>
        Backups.Calls.Where(call => call.Verb == nameof(ILatticeBackupControl.ListBackupsAsync)).Select(call => (BackupCatalogRequest)call.Argument!);

    [Test]
    public void At_the_default_tenant_the_catalogue_lists_only_its_own_backups()
    {
        UseTenant(null);
        SeedEstate();

        var cut = RenderAt<BackupsCataloguePage>("t/default/backups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("default-nightly"));
            Assert.That(cut.Markup, Does.Not.Contain("acme-nightly").And.Not.Contain("globex-nightly").And.Not.Contain("platform-nightly"));
            Assert.That(ListRequests, Is.Not.Empty.And.All.Matches<BackupCatalogRequest>(request => request.ActiveTenantOnly));
        });
    }

    [Test]
    public void Under_a_tenant_the_catalogue_lists_only_its_own_backups()
    {
        UseTenant("acme");
        SeedEstate();

        var cut = RenderAt<BackupsCataloguePage>("t/acme/backups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("acme-nightly"));
            Assert.That(cut.Markup, Does.Not.Contain("default-nightly").And.Not.Contain("globex-nightly"));
        });
    }

    [Test]
    public void With_tenancy_off_every_backup_is_listed_and_nothing_is_narrowed()
    {
        SeedEstate();

        var cut = RenderAt<BackupsCataloguePage>("backups");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("default-nightly").And.Contain("globex-nightly").And.Contain("platform-nightly"));
            Assert.That(ListRequests, Has.None.Matches<BackupCatalogRequest>(request => request.ActiveTenantOnly));
        });
    }

    [Test]
    public void A_tenant_address_for_another_tenants_backup_is_not_found()
    {
        UseTenant(null);
        SeedEstate();
        var notFound = false;
        Navigation.OnNotFound += (_, _) => notFound = true;

        var cut = RenderAt<BackupPage>("t/default/backups/acme-nightly");

        cut.WaitUntil(() =>
        {
            Assert.That(notFound, Is.True);
            Assert.That(cut.Markup, Does.Not.Contain("t/acme/orders"));
        });
    }

    [Test]
    public void A_tenant_address_for_its_own_backup_renders_it()
    {
        UseTenant("acme");
        SeedEstate();
        var notFound = false;
        Navigation.OnNotFound += (_, _) => notFound = true;

        var cut = RenderAt<BackupPage>("t/acme/backups/acme-nightly");

        cut.WaitUntil(() =>
        {
            Assert.That(notFound, Is.False);
            Assert.That(cut.Markup, Does.Contain("acme-nightly"));
        });
    }

    [Test]
    public async Task Completions_offer_only_the_listing_tenants_backups()
    {
        UseTenant(null);
        SeedEstate();
        var source = Services.GetRequiredService<BackupsCompletionSource>();

        var byId = await source.CompleteAsync(new AddressQuery("backup:", AddressQueryMode.Search, ExplorerAddress.Home.WithTenant("default")), CancellationToken.None);
        var byName = await source.CompleteAsync(new AddressQuery("nightly", AddressQueryMode.Search, ExplorerAddress.Home.WithTenant("default")), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(byId.Select(completion => completion.Label), Has.All.Contains("default-nightly"));
            Assert.That(byId, Has.Count.EqualTo(1));
            Assert.That(byName, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task The_home_status_reads_only_the_listing_tenants_backups()
    {
        UseTenant("acme");
        SeedEstate();
        var area = Services.GetServices<IExplorerArea>().OfType<BackupsArea>().Single();

        await area.GetHomeStatusAsync(CancellationToken.None);

        Assert.That(ListRequests, Is.Not.Empty.And.All.Matches<BackupCatalogRequest>(request => request.ActiveTenantOnly));
    }

    [TestCase("orders", "default", true)]
    [TestCase("t/acme/orders", "default", false)]
    [TestCase("t/acme/orders", "acme", true)]
    [TestCase("sys-tenant-registry", "default", false)]
    [TestCase("t/acme/orders", null, true)]
    public void Lists_decides_by_the_tenancy_ownership_grammar(string tree, string? tenant, bool listed)
    {
        Assert.That(BackupsAccess.Lists(tenant, FakeBackupControl.Manifest("b", tree: tree)), Is.EqualTo(listed));
    }

    [Test]
    public void Narrow_asks_for_the_active_tenant_only_with_tenancy_on()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BackupsAccess.Narrow(new BackupCatalogRequest(), "default").ActiveTenantOnly, Is.True);
            Assert.That(BackupsAccess.Narrow(new BackupCatalogRequest(), null).ActiveTenantOnly, Is.False);
        });
    }
}
