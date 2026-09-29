using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Shell.Areas.Tenancy;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Tenancy;

/// <summary>
/// The area's address completions and the accessible-tenant list the address
/// root's tenant selector reads: one source of truth, fail-closed.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyCompletionTests : TenancyTestContext
{
    [Test]
    public async Task Plain_text_completes_workspaces_and_for_an_operator_administration_prefix_matches_first()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex").WithTenant("my-acme-labs");

        var completions = await Complete("acm");

        Assert.Multiple(() =>
        {
            Assert.That(completions.Select(completion => completion.Label), Is.EqualTo(new[] { "t/acme", "tenancy/acme", "t/my-acme-labs", "tenancy/my-acme-labs" }));
            Assert.That(completions[0].Target.Format(), Is.EqualTo("/t/acme/tenancy"));
            Assert.That(completions[1].Target.Format(), Is.EqualTo("/tenancy/acme"));
            Assert.That(completions[0].Detail, Is.EqualTo("Your tenant, active"));
            Assert.That(completions[2].Detail, Is.EqualTo("Switch to this tenant, active"));
            Assert.That(completions[1].Detail, Is.EqualTo("Administer tenant acme"));
        });
    }

    [Test]
    public async Task A_tenant_admin_is_offered_only_workspaces()
    {
        UseTenancyAs(isOperator: false);

        var completions = await Complete("acme");

        Assert.That(completions.Select(completion => completion.Label), Is.EqualTo(new[] { "t/acme" }));
    }

    [Test]
    public async Task The_t_prefix_lists_every_workspace_and_the_tenancy_prefix_every_administration()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex", TenantLifecycleStatus.Suspended);

        var workspaces = await Complete("t/");
        var administration = await Complete("tenancy/g");

        Assert.Multiple(() =>
        {
            Assert.That(workspaces.Select(completion => completion.Label), Is.EqualTo(new[] { "t/acme", "t/globex" }));
            Assert.That(workspaces[1].Detail, Is.EqualTo("Switch to this tenant, suspended"));
            Assert.That(administration.Select(completion => completion.Label), Is.EqualTo(new[] { "tenancy/globex" }));
        });
    }

    [Test]
    public async Task A_tenant_admin_typing_the_administration_prefix_gets_nothing()
    {
        UseTenancyAs(isOperator: false);

        Assert.That(await Complete("tenancy/"), Is.Empty);
    }

    [Test]
    public async Task A_raw_tenancy_address_completes_administration_pages()
    {
        UseTenancyAs(isOperator: true);

        var completions = await Complete("/tenancy/ac", AddressQueryMode.Address);

        Assert.That(completions.Select(completion => completion.Target.Format()), Is.EqualTo(new[] { "/tenancy/acme" }));
    }

    [Test]
    [TestCase("", "Search")]
    [TestCase("/data/orders", "Address")]
    [TestCase("crm", "App")]
    [TestCase("zzz", "Search")]
    public async Task Other_input_completes_nothing(string text, string mode)
    {
        UseTenancyAs();

        Assert.That(await Complete(text, Enum.Parse<AddressQueryMode>(mode)), Is.Empty);
    }

    [Test]
    public async Task No_more_than_the_limit_is_returned()
    {
        UseTenancyAs(isOperator: true);
        for (var i = 0; i < 30; i++)
        {
            Cluster.WithTenant($"t{i:00}");
        }

        Assert.That(await Complete("t0"), Has.Count.EqualTo(AddressQuery.MaximumResults));
    }

    [Test]
    public void A_failed_read_propagates_so_the_chrome_can_say_so()
    {
        UseTenancyAs();
        Cluster.Fail(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync), new TimeoutException());

        Assert.ThrowsAsync<TimeoutException>(async () => await Complete("acme"));
    }

    [Test]
    public void A_null_query_is_refused()
    {
        Assert.ThrowsAsync<ArgumentNullException>(async () => await new TenancyCompletionSource(Catalog).CompleteAsync(null!, CancellationToken.None));
    }

    [Test]
    public async Task The_accessible_list_leads_with_the_established_tenant_and_skips_suspended_ones()
    {
        Cluster.WithTenant("globex").WithTenant("initech", TenantLifecycleStatus.Suspended);
        var source = new TenancyAccessibleTenantSource(Catalog, new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("globex") });

        var tenants = await source.GetAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.Value), Is.EqualTo(new[] { "globex", "acme" }));
    }

    [Test]
    public async Task The_established_tenant_is_kept_even_when_suspended()
    {
        Cluster.WithTenant("globex", TenantLifecycleStatus.Suspended);
        var source = new TenancyAccessibleTenantSource(Catalog, new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("globex") });

        var tenants = await source.GetAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.Value), Is.EqualTo(new[] { "globex", "acme" }));
    }

    [Test]
    public async Task A_failed_read_reports_only_the_established_tenant_or_nothing()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync), FakeTenancyCluster.Denied());

        var scoped = await new TenancyAccessibleTenantSource(Catalog, new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") }).GetAccessibleTenantsAsync();
        var unscoped = await new TenancyAccessibleTenantSource(Catalog, null).GetAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(scoped.Select(tenant => tenant.Value), Is.EqualTo(new[] { "acme" }));
            Assert.That(unscoped, Is.Empty);
        });
    }

    [Test]
    public async Task An_unchanged_answer_is_the_same_list_instance()
    {
        var source = new TenancyAccessibleTenantSource(Catalog, null);

        var first = await source.GetAccessibleTenantsAsync();
        var second = await source.GetAccessibleTenantsAsync();

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public async Task The_list_is_read_from_the_catalogue_so_a_created_tenant_appears_after_invalidation()
    {
        var source = new TenancyAccessibleTenantSource(Catalog, null);
        await source.GetAccessibleTenantsAsync();

        Cluster.WithTenant("globex");
        var stale = await source.GetAccessibleTenantsAsync();
        Catalog.Invalidate();
        var fresh = await source.GetAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(stale.Select(tenant => tenant.Value), Is.EqualTo(new[] { "acme" }));
            Assert.That(fresh.Select(tenant => tenant.Value), Is.EqualTo(new[] { "acme", "globex" }));
        });
    }

    [Test]
    public void A_cancelled_read_propagates()
    {
        Cluster.Fail(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync), new OperationCanceledException());

        Assert.ThrowsAsync<OperationCanceledException>(async () => await new TenancyAccessibleTenantSource(Catalog, null).GetAccessibleTenantsAsync());
    }

    private async Task<IReadOnlyList<AddressCompletion>> Complete(string text, AddressQueryMode mode = AddressQueryMode.Search) =>
        await new TenancyCompletionSource(Catalog).CompleteAsync(new AddressQuery(text, mode, ExplorerAddress.Home), CancellationToken.None);
}
