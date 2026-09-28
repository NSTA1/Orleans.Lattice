using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell;
using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// The Access area's contract: its registration, its fail-closed availability
/// probe (visible, hidden, or unavailable with a sign-in sentence), memoisation
/// per identity, its Home status, its commands, and its cluster-wide addresses.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessAreaTests : AccessTestContext
{
    [Test]
    public void The_shell_registers_the_area_and_its_catalogue_scoped()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        Assert.Multiple(() =>
        {
            Assert.That(services.Where(descriptor => descriptor.ServiceType == typeof(IExplorerArea) && descriptor.ImplementationType == typeof(AccessArea))
                .Select(descriptor => descriptor.Lifetime), Is.EqualTo(new[] { ServiceLifetime.Scoped }));
            Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(AccessCatalog)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
        });
    }

    [Test]
    public void The_area_is_the_cluster_wide_access_stop_at_position_thirty()
    {
        var area = CreateArea();

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("access"));
            Assert.That(area.DisplayName, Is.EqualTo("Access"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(30));
            Assert.That(area.IsTenantScoped, Is.False);
            Assert.That(area.Completions, Is.InstanceOf<AccessCompletionSource>());
            Assert.That(area.Commands.Select(command => command.Id), Is.EqualTo(new[] { "access.explain", "access.create-rule", "access.create-group" }));
            Assert.That(area.Commands.Select(command => command.Target!.Format()),
                Is.EqualTo(new[] { "/access/explain", "/access/rules?new=true", "/access/groups?new=true" }));
        });
    }

    [Test]
    public async Task An_administrator_sees_the_area()
    {
        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(Admin.Calls, Is.EqualTo(new[] { nameof(FakeAuthAdmin.ListGroupsAsync) }));
        });
    }

    [Test]
    public async Task A_signed_in_identity_without_the_grant_does_not_see_the_area()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task An_anonymous_circuit_is_told_to_sign_in_rather_than_denied()
    {
        await Auth.LogoutAsync();
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(availability.Reason, Is.EqualTo(AccessArea.SignInReason));
        });
    }

    [Test]
    public async Task Without_the_auth_facade_the_area_is_hidden()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();
        var area = new AccessArea(provider);

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);
        var status = await area.GetHomeStatusAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(area.Completions, Is.Null);
            Assert.That(status, Is.Null);
        });
    }

    [Test]
    [TestCase("unserved")]
    [TestCase("unconfigured")]
    [TestCase("transport")]
    public async Task A_cluster_that_cannot_answer_hides_the_area(string fault)
    {
        Exception failure = fault switch
        {
            "unserved" => new NotSupportedException(),
            "unconfigured" => new InvalidOperationException("not configured"),
            _ => new TimeoutException(),
        };
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), failure);

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_cancelled_probe_propagates_and_is_not_remembered()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), new OperationCanceledException());
        var area = CreateArea();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await area.GetAvailabilityAsync(CancellationToken.None));

        Admin.Heal(nameof(FakeAuthAdmin.ListGroupsAsync));
        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
    }

    [Test]
    public async Task The_verdict_is_remembered_until_the_identity_changes()
    {
        var area = CreateArea();

        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetAvailabilityAsync(CancellationToken.None);
        Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ListGroupsAsync)), Is.EqualTo(1));

        Auth.SignIn("someone-else@example.com");
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());

        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
        Assert.That(Admin.Calls.Count(call => call == nameof(FakeAuthAdmin.ListGroupsAsync)), Is.EqualTo(2));
    }

    [Test]
    [TestCase(true, "Rules are enforced.")]
    [TestCase(false, "Rules are recorded but not enforced.")]
    public async Task Home_states_whether_rules_are_enforced(bool enforced, string expected)
    {
        Admin.Model = Admin.Model with { RulesEnforced = enforced };

        Assert.That(await CreateArea().GetHomeStatusAsync(CancellationToken.None), Is.EqualTo(expected));
    }

    [Test]
    public async Task An_unread_access_model_gives_no_home_status()
    {
        Admin.Fail(nameof(FakeAuthAdmin.GetAccessModelAsync), new InvalidOperationException());

        Assert.That(await CreateArea().GetHomeStatusAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public void With_tenancy_on_access_addresses_are_never_tenant_rooted()
    {
        UseTenancy("acme");
        var navigator = Services.GetRequiredService<ExplorerNavigator>();

        Assert.Multiple(() =>
        {
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/acme/access/rules")).Format(), Is.EqualTo("/access/rules"));
            Assert.That(navigator.Canonicalize(AccessRoutes.Group("ops")).Format(), Is.EqualTo("/access/groups/ops"));
            Assert.That(navigator.Canonicalize(AccessRoutes.AppRoles("crm")).Format(), Is.EqualTo("/t/acme/apps/crm/roles"));
        });
    }

    [Test]
    public void With_tenancy_off_a_tenant_rooted_access_address_is_made_plain()
    {
        var navigator = Services.GetRequiredService<ExplorerNavigator>();

        Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/acme/access/explain")).Format(), Is.EqualTo("/access/explain"));
    }

    [Test]
    public async Task The_directory_lists_the_area_for_an_administrator_and_not_for_a_restricted_identity()
    {
        var directory = Services.GetRequiredService<ExplorerAreaDirectory>();
        var shown = await directory.GetEntriesAsync();

        Assert.That(shown.Select(entry => entry.Area.Key), Does.Contain("access"));
    }

    [Test]
    public async Task The_directory_hides_the_area_from_a_restricted_identity()
    {
        Admin.Fail(nameof(FakeAuthAdmin.ListGroupsAsync), Denied());
        var directory = Services.GetRequiredService<ExplorerAreaDirectory>();

        var shown = await directory.GetEntriesAsync();

        Assert.That(shown.Select(entry => entry.Area.Key), Does.Not.Contain("access"));
    }

    private static LatticeAuthorizationDeniedException Denied() =>
        new("_lattice_policy", LatticeOperation.Admin, "ops@example.com", "not an administrator");

    private AccessArea CreateArea() => new(Services);
}
