using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The one-source-of-truth seam: the list the shell's tenant picker offers and
/// the list this area administers are the same list, read once from the cluster.
/// </summary>
[TestFixture]
public sealed class TenantsAccessibleTenantSourceTests
{
    private sealed class FixedContext : IExplorerTenantContext
    {
        public ExplorerTenantId? ActiveTenant { get; set; }

        public ExplorerTenantVisibility RequestedVisibility { get; set; }
    }

    private static (TenantsAccessibleTenantSource Source, FakeTenantAdminService Service, FixedContext Context)
        Create(string? activeTenant = null)
    {
        var service = new FakeTenantAdminService();
        var context = new FixedContext
        {
            ActiveTenant = activeTenant is null ? null : new ExplorerTenantId(activeTenant),
        };

        return (new TenantsAccessibleTenantSource(service, context), service, context);
    }

    [Test]
    public void Constructor_null_service_throws()
    {
        Assert.That(
            () => new TenantsAccessibleTenantSource(null!, new FixedContext()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_context_throws()
    {
        Assert.That(
            () => new TenantsAccessibleTenantSource(new FakeTenantAdminService(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task It_reports_the_tenants_the_cluster_named()
    {
        var (source, service, _) = Create();
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Acme));
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(
            reachable.Select(tenant => tenant.Value),
            Is.EqualTo(new[] { SampleTenants.Acme, SampleTenants.Globex }));
    }

    [Test]
    public async Task The_established_tenant_leads_so_it_is_the_fallback()
    {
        var (source, service, _) = Create(SampleTenants.Globex);
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Acme));
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reachable[0].Value, Is.EqualTo(SampleTenants.Globex));
            Assert.That(reachable.Select(tenant => tenant.Value), Is.Unique);
            Assert.That(reachable, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task A_suspended_tenant_is_not_offered_because_its_data_plane_refuses_everything()
    {
        var (source, service, _) = Create();
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Acme));
        service.Tenants.Add(
            SampleTenants.Summary(SampleTenants.Globex, ExplorerTenantLifecycle.Suspended));

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(
            reachable.Select(tenant => tenant.Value),
            Is.EqualTo(new[] { SampleTenants.Acme }));
    }

    [Test]
    public async Task The_caller_own_tenant_is_kept_whatever_its_lifecycle()
    {
        // A list that omitted where the caller already is would claim they
        // cannot reach it.
        var (source, service, _) = Create(SampleTenants.Globex);
        service.Tenants.Add(
            SampleTenants.Summary(SampleTenants.Globex, ExplorerTenantLifecycle.Suspended));

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(
            reachable.Select(tenant => tenant.Value),
            Is.EqualTo(new[] { SampleTenants.Globex }));
    }

    [Test]
    public async Task A_refused_read_falls_back_to_the_established_tenant_and_nothing_more()
    {
        var (source, service, _) = Create(SampleTenants.Acme);
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));
        service.Fail(FakeTenantAdminService.Op.List, TenantOperationStatus.Denied);

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(
            reachable.Select(tenant => tenant.Value),
            Is.EqualTo(new[] { SampleTenants.Acme }));
    }

    [Test]
    public async Task A_refused_read_with_no_established_tenant_reports_nothing_at_all()
    {
        var (source, service, _) = Create();
        service.Fail(FakeTenantAdminService.Op.List, TenantOperationStatus.Denied);

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(reachable, Is.Empty);
    }

    [Test]
    public async Task An_empty_list_reports_nothing_rather_than_guessing()
    {
        var (source, _, _) = Create();

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(reachable, Is.Empty);
    }

    [Test]
    public async Task An_unchanged_answer_is_handed_back_by_reference()
    {
        var (source, service, _) = Create();
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Acme));

        var first = await source.GetAccessibleTenantsAsync();
        var second = await source.GetAccessibleTenantsAsync();

        // Asked on the resolve path and on every tenant-control refresh, so a
        // steady state must cost no allocation.
        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public async Task A_tenant_created_since_the_last_read_appears_without_a_reconnect()
    {
        var (source, service, _) = Create();
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Acme));
        await source.GetAccessibleTenantsAsync();

        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));
        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(
            reachable.Select(tenant => tenant.Value),
            Is.EqualTo(new[] { SampleTenants.Acme, SampleTenants.Globex }));
    }

    [Test]
    public async Task A_tenant_with_no_id_is_dropped_rather_than_offered()
    {
        var (source, service, _) = Create();
        service.Tenants.Add(new ExplorerTenantSummary(string.Empty, ExplorerTenantLifecycle.Active, false));
        service.Tenants.Add(SampleTenants.Summary(SampleTenants.Acme));

        var reachable = await source.GetAccessibleTenantsAsync();

        Assert.That(
            reachable.Select(tenant => tenant.Value),
            Is.EqualTo(new[] { SampleTenants.Acme }));
    }
}
