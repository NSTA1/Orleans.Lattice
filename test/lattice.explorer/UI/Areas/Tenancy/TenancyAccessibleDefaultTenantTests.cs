using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The reserved default tenant, which the cluster never lists, is offered to a
/// proven platform operator by the accessible-tenant list the scope selector and
/// the identity resolver read, and to nobody else.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyAccessibleDefaultTenantTests : TenancyTestContext
{
    [Test]
    public async Task An_operator_who_administers_a_tenant_is_offered_the_default_tenant_too()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("zeta");
        var source = new TenancyAccessibleTenantSource(Catalog, new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") });

        var tenants = await source.GetAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.Value), Is.EqualTo(new[] { "acme", TenantId.DefaultId, "zeta" }),
            "the established tenant leads, and the default tenant takes its place in id order among the rest");
    }

    [Test]
    public async Task An_operator_with_no_established_tenant_is_offered_the_default_tenant_in_order()
    {
        UseTenancyAs(isOperator: true);
        var source = new TenancyAccessibleTenantSource(Catalog, null);

        var tenants = await source.GetAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.Value), Is.EqualTo(new[] { "acme", TenantId.DefaultId }));
    }

    [Test]
    public async Task An_operator_scoped_to_the_default_tenant_is_not_offered_it_twice()
    {
        UseTenancyAs(isOperator: true);
        var source = new TenancyAccessibleTenantSource(Catalog, new ExplorerTenantContext { ActiveTenant = ExplorerTenantId.Default });

        var tenants = await source.GetAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.Value), Is.EqualTo(new[] { TenantId.DefaultId, "acme" }));
    }

    [Test]
    public async Task A_non_operator_list_is_exactly_what_the_cluster_named()
    {
        UseTenancyAs(isOperator: false);
        var source = new TenancyAccessibleTenantSource(Catalog, new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") });

        var tenants = await source.GetAccessibleTenantsAsync();

        Assert.That(tenants.Select(tenant => tenant.Value), Is.EqualTo(new[] { "acme" }));
    }

    [Test]
    public async Task The_identity_resolver_lands_an_operator_on_the_default_tenant_rather_than_the_first_they_administer()
    {
        UseTenancyAs(isOperator: true);
        var context = new ExplorerTenantContext();
        var view = new ExplorerTenantView(context, new StubGate(true));
        var resolver = new DefaultExplorerTenantIdentityResolver(view, Auth, context, new TenancyAccessibleTenantSource(Catalog, context));

        await resolver.ResolveAsync();

        Assert.That(context.ActiveTenant, Is.EqualTo(ExplorerTenantId.Default));
    }

    private sealed class StubGate(bool isOperator) : IExplorerTenantOperatorGate
    {
        public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) => new(isOperator);
    }
}
