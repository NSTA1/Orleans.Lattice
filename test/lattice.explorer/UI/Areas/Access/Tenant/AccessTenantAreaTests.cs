using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access.Tenant;

/// <summary>
/// Issue #4158: the Access stop's visibility probe is tenant-aware. A tenant
/// admin who is not a cluster access administrator sees the area while the
/// posture probe reports delegated tenant access administration enabled for the
/// circuit's asserted tenant; a member, the feature off, the reserved default
/// tenant, an anonymous circuit, or a head without the tenant policy facade
/// widens nothing.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessTenantAreaTests : AccessTestContext
{
    [Test]
    public async Task A_tenant_admin_who_is_not_a_cluster_administrator_sees_the_area()
    {
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();

        var availability = await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(TenantFacades.Gate.Calls, Is.EqualTo(new[] { "GetPostureAsync" }));
        });
    }

    [Test]
    public async Task A_tenant_admin_who_sees_the_area_is_not_a_cluster_access_administrator()
    {
        // The platform-operator gate asks this question; seeing the area through the
        // tenant posture must never read as standing over every tenant.
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();
        var area = new AccessArea(Services);

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);
        var operatorStanding = await area.IsClusterAccessAdministratorAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(operatorStanding, Is.False);
        });
    }

    [Test]
    public async Task A_cluster_access_administrator_has_operator_standing_without_the_posture()
    {
        AssertTenant("acme");
        TenantFacades.AsTenantAdmin();
        var area = new AccessArea(Services);

        var first = await area.IsClusterAccessAdministratorAsync(CancellationToken.None);
        var second = await area.IsClusterAccessAdministratorAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True);
            Assert.That(second, Is.True, "remembered for the caller");
            Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ListGroupsAsync)), Is.EqualTo(1), "probed once");
            Assert.That(TenantFacades.Gate.Calls, Is.Empty, "the tenant posture is never asked");
        });
    }

    [Test]
    public async Task A_platform_operator_reported_by_the_posture_sees_the_area()
    {
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsOperator();

        Assert.That(await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
    }

    [Test]
    public async Task A_member_who_administers_nothing_does_not_see_the_area()
    {
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsMember();

        Assert.That(await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_caller_the_posture_probe_refuses_does_not_see_the_area()
    {
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();
        TenantFacades.Gate.Denied = true;

        Assert.That(await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task With_the_feature_off_a_tenant_admin_does_not_see_the_area()
    {
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();
        TenantFacades.Gate.Enabled = false;

        Assert.That(await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_cluster_administrator_sees_the_area_without_the_posture_being_asked()
    {
        AssertTenant("acme");
        TenantFacades.AsTenantAdmin();

        var availability = await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(TenantFacades.Gate.Calls, Is.Empty);
        });
    }

    [Test]
    [TestCase(null)]
    [TestCase("default")]
    public async Task Without_a_tenant_to_administer_the_posture_is_never_asked(string? tenant)
    {
        if (tenant is not null)
        {
            AssertTenant(tenant);
        }

        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();

        var availability = await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(TenantFacades.Gate.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task An_anonymous_circuit_is_still_told_to_sign_in_and_the_posture_is_not_asked()
    {
        AssertTenant("acme");
        await Auth.LogoutAsync();
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();

        var availability = await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(TenantFacades.Gate.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task A_head_without_the_tenant_policy_facade_widens_nothing()
    {
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();
        TenantFacades.ServesPolicy = false;

        Assert.That(await new AccessArea(Services).GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task The_tenant_verdict_is_remembered_for_the_caller()
    {
        AssertTenant("acme");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        TenantFacades.AsTenantAdmin();
        var area = new AccessArea(Services);

        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(TenantFacades.Gate.Calls, Is.EqualTo(new[] { "GetPostureAsync" }));
    }

    private static LatticeAuthorizationDeniedException Denied() =>
        new("_lattice_policy", LatticeOperation.Admin, "ops@example.com", "not an administrator");
}
