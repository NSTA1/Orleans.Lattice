using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Covers the controlled domain model the tenancy plugins resolve: it exposes
/// the operations surface and the availability probe, and delegates every
/// tenant-identity question to the Explorer's existing switcher rather than
/// re-deciding it. A head that never enabled tenant scoping gets the inert,
/// fail-closed shape.
/// </summary>
[TestFixture]
public class TenancyDomainTests
{
    private FakeTenantAdminClient _client = null!;
    private TenantAdminService _service = null!;

    [SetUp]
    public void SetUp()
    {
        _client = new FakeTenantAdminClient();
        _service = new TenantAdminService(_client);
    }

    private TenancyDomain Create(StubTenantSwitcher? switcher) =>
        new(_service, new TenancyAvailability(_client, switcher), switcher);

    [Test]
    public void Constructor_rejects_null_dependencies() =>
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new TenancyDomain(null!, new TenancyAvailability(_client)),
                Throws.ArgumentNullException);
            Assert.That(() => new TenancyDomain(_service, null!), Throws.ArgumentNullException);
        });

    [Test]
    public void The_operations_surface_is_the_one_the_seam_was_built_over() =>
        Assert.That(Create(new StubTenantSwitcher()).Tenants, Is.SameAs(_service));

    [Test]
    public void Tenancy_is_enabled_only_when_the_tenant_view_is_active() =>
        Assert.Multiple(() =>
        {
            Assert.That(Create(new StubTenantSwitcher()).IsTenancyEnabled, Is.True);
            Assert.That(Create(new StubTenantSwitcher(isActive: false)).IsTenancyEnabled, Is.False);
            Assert.That(Create(switcher: null).IsTenancyEnabled, Is.False);
        });

    [Test]
    public void The_active_tenant_and_visibility_come_from_the_switcher()
    {
        var switcher = new StubTenantSwitcher
        {
            ActiveTenant = new ExplorerTenantId("globex"),
            RequestedVisibility = ExplorerTenantVisibility.AllTenants,
        };

        var domain = Create(switcher);

        Assert.Multiple(() =>
        {
            Assert.That(domain.ActiveTenant, Is.EqualTo(new ExplorerTenantId("globex")));
            Assert.That(domain.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
        });
    }

    [Test]
    public void Without_a_tenant_view_identity_falls_back_to_the_fail_closed_shape()
    {
        var domain = Create(switcher: null);

        Assert.Multiple(() =>
        {
            Assert.That(domain.ActiveTenant, Is.Null);
            Assert.That(domain.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.ActiveTenant));
        });
    }

    [Test]
    public async Task The_operator_verdict_is_the_switchers_and_is_false_without_one()
    {
        var operatorVerdict = await Create(new StubTenantSwitcher(isOperator: true)).IsPlatformOperatorAsync();
        var tenantVerdict = await Create(new StubTenantSwitcher()).IsPlatformOperatorAsync();
        var noViewVerdict = await Create(switcher: null).IsPlatformOperatorAsync();

        Assert.Multiple(() =>
        {
            Assert.That(operatorVerdict, Is.True);
            Assert.That(tenantVerdict, Is.False);
            Assert.That(noViewVerdict, Is.False);
        });
    }

    [Test]
    public async Task Switching_tenant_is_applied_for_an_operator_and_denied_otherwise()
    {
        var operatorSwitcher = new StubTenantSwitcher(isOperator: true);
        var applied = await Create(operatorSwitcher).SwitchTenantAsync(new ExplorerTenantId("globex"));

        var tenantSwitcher = new StubTenantSwitcher();
        var denied = await Create(tenantSwitcher).SwitchTenantAsync(new ExplorerTenantId("globex"));

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(operatorSwitcher.SwitchedTo, Is.EqualTo(new ExplorerTenantId("globex")));
            Assert.That(denied, Is.False);
            Assert.That(tenantSwitcher.SwitchedTo, Is.Null);
        });
    }

    [Test]
    public async Task Requesting_a_visibility_is_applied_for_an_operator_and_denied_otherwise()
    {
        var operatorSwitcher = new StubTenantSwitcher(isOperator: true);
        var applied = await Create(operatorSwitcher)
            .SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        var tenantSwitcher = new StubTenantSwitcher();
        var denied = await Create(tenantSwitcher).SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(operatorSwitcher.RequestedScope, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
            Assert.That(denied, Is.False);
            Assert.That(tenantSwitcher.RequestedScope, Is.Null);
        });
    }

    [Test]
    public async Task Without_a_tenant_view_every_identity_mutation_is_an_inert_no_op()
    {
        var domain = Create(switcher: null);

        var switched = await domain.SwitchTenantAsync(new ExplorerTenantId("globex"));
        var scoped = await domain.SetVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(switched, Is.False);
            Assert.That(scoped, Is.False);
        });
    }

    [Test]
    public async Task The_availability_probe_reports_unavailable_without_a_tenant_view()
    {
        var access = await Create(switcher: null).ProbeAvailabilityAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.IsVisible, Is.False);
        });
    }

    [Test]
    public async Task The_availability_probe_reports_allowed_on_a_reachable_tenancy_cluster()
    {
        var access = await Create(new StubTenantSwitcher()).ProbeAvailabilityAsync();

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
    }
}
