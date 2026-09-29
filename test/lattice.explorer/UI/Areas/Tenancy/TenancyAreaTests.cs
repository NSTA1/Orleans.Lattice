using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// The Tenancy area's contract: its registration, its fail-closed availability
/// (hidden without tenancy, visible to an operator or a tenant admin, hidden from
/// a restricted identity, a sign-in sentence for an anonymous one), memoisation,
/// its Home status and spine badge, and the commands its standing admits.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyAreaTests : TenancyTestContext
{
    [Test]
    public void The_shell_registers_the_area_its_catalogue_and_the_accessible_tenant_list_scoped()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell();

        Assert.Multiple(() =>
        {
            Assert.That(services.Where(descriptor => descriptor.ServiceType == typeof(IExplorerArea) && descriptor.ImplementationType == typeof(TenancyArea))
                .Select(descriptor => descriptor.Lifetime), Is.EqualTo(new[] { ServiceLifetime.Scoped }));
            Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(TenancyCatalog)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
            Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(IExplorerAccessibleTenantSource)).Lifetime, Is.EqualTo(ServiceLifetime.Scoped));
        });
    }

    [Test]
    public void The_shells_accessible_tenant_list_wins_over_cores_default_when_tenancy_is_registered_after_it()
    {
        var services = new ServiceCollection().AddLatticeExplorerShell().AddExplorerTenantView();

        Assert.That(services.Single(descriptor => descriptor.ServiceType == typeof(IExplorerAccessibleTenantSource)).ImplementationFactory, Is.Not.Null);
    }

    [Test]
    public void The_area_is_the_tenancy_stop_at_position_fifty()
    {
        var area = CreateArea();

        Assert.Multiple(() =>
        {
            Assert.That(area.Key, Is.EqualTo("tenancy"));
            Assert.That(area.DisplayName, Is.EqualTo("Tenancy"));
            Assert.That(area.DirectoryOrder, Is.EqualTo(50));
            Assert.That(area.IsTenantScoped, Is.True);
            Assert.That(area.Completions, Is.InstanceOf<TenancyCompletionSource>());
        });
    }

    [Test]
    public void Only_a_tenant_rooted_address_follows_the_active_tenant()
    {
        var area = CreateArea();

        Assert.Multiple(() =>
        {
            Assert.That(area.IsTenantScopedAt(ExplorerAddress.Parse("/t/acme/tenancy")), Is.True);
            Assert.That(area.IsTenantScopedAt(ExplorerAddress.Parse("/t/acme/tenancy/sharing")), Is.True);
            Assert.That(area.IsTenantScopedAt(ExplorerAddress.Parse("/tenancy")), Is.False);
            Assert.That(area.IsTenantScopedAt(ExplorerAddress.Parse("/tenancy/acme/grants")), Is.False);
        });
    }

    [Test]
    public void With_tenancy_on_the_directory_stays_plain_and_my_tenant_stays_rooted()
    {
        UseTenancyAs("acme");
        var navigator = Services.GetRequiredService<ExplorerNavigator>();

        Assert.Multiple(() =>
        {
            Assert.That(navigator.Canonicalize(TenancyRoutes.Directory).Format(), Is.EqualTo("/tenancy"));
            Assert.That(navigator.Canonicalize(TenancyRoutes.TenantGrants("globex")).Format(), Is.EqualTo("/tenancy/globex/grants"));
            Assert.That(navigator.Canonicalize(TenancyRoutes.MyTenant("acme", TenancyRoutes.QuotaSegment)).Format(), Is.EqualTo("/t/acme/tenancy/quota"));
            Assert.That(navigator.ReRoot(TenancyRoutes.Directory, "globex").Format(), Is.EqualTo("/tenancy"));
            Assert.That(navigator.ReRoot(TenancyRoutes.MyTenant("acme"), "globex").Format(), Is.EqualTo("/t/globex/tenancy"));
        });
    }

    [Test]
    public void With_tenancy_off_a_tenant_rooted_address_is_made_plain()
    {
        var navigator = Services.GetRequiredService<ExplorerNavigator>();

        Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/acme/tenancy")).Format(), Is.EqualTo("/tenancy"));
    }

    [Test]
    public async Task Another_tenants_workspace_goes_through_the_switch_and_its_administration_does_not()
    {
        UseTenancyAs("acme", allowSwitch: false);
        var navigator = Services.GetRequiredService<ExplorerNavigator>();

        var workspace = await navigator.ResolveAsync(ExplorerAddress.Parse("/t/globex/tenancy"));
        var administration = await navigator.ResolveAsync(ExplorerAddress.Parse("/tenancy/globex"));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Address.Format(), Is.EqualTo("/t/acme/tenancy"));
            Assert.That(workspace.Notice, Does.Contain("globex"));
            Assert.That(administration.Address.Format(), Is.EqualTo("/tenancy/globex"));
            Assert.That(administration.Notice, Is.Null);
        });
        await Switcher!.Received(1).SwitchTenantAsync(new ExplorerTenantId("globex"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task With_tenancy_off_the_area_is_hidden_and_asks_the_cluster_nothing()
    {
        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(Cluster.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task Without_the_self_service_facade_the_area_is_hidden()
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        using var provider = new ServiceCollection().AddSingleton(new ExplorerTenancy(view)).BuildServiceProvider();
        var area = new TenancyArea(provider);

        var availability = await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(area.Completions, Is.Null);
            Assert.That(area.Commands, Is.Empty);
        });
    }

    [Test]
    public async Task A_platform_operator_sees_the_area()
    {
        UseTenancyAs(isOperator: true);

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(Cluster.Calls, Does.Contain(nameof(FakeTenancyCluster.GetCurrentTenantAsync)));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.ListAdminSubjectsAsync)), "an operator needs no tenant-admin proof");
        });
    }

    [Test]
    public async Task An_admin_of_the_scoped_tenant_sees_the_area()
    {
        UseTenancyAs(isOperator: false);

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Visible));
            Assert.That(Cluster.Calls, Does.Contain(nameof(FakeTenancyCluster.ListAdminSubjectsAsync)));
        });
    }

    [Test]
    public async Task A_signed_in_identity_that_administers_nothing_does_not_see_the_area()
    {
        UseTenancyAs(isOperator: false);
        Cluster.Fail(nameof(FakeTenancyCluster.ListAdminSubjectsAsync), FakeTenancyCluster.Denied());

        Assert.That(await CreateArea().GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_caller_scoped_to_the_default_tenant_who_is_not_an_operator_sees_nothing()
    {
        UseTenancyAs(active: TenantId.DefaultId, isOperator: false);

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability, Is.EqualTo(AreaAvailability.Hidden));
            Assert.That(Cluster.Calls, Does.Not.Contain(nameof(FakeTenancyCluster.ListAdminSubjectsAsync)));
        });
    }

    [Test]
    public async Task An_anonymous_circuit_is_told_to_sign_in()
    {
        UseTenancyAs(isOperator: false);
        await Auth.LogoutAsync();
        Cluster.Fail(nameof(FakeTenancyCluster.GetCurrentTenantAsync), FakeTenancyCluster.Denied());

        var availability = await CreateArea().GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(availability.Kind, Is.EqualTo(AreaAvailabilityKind.Unavailable));
            Assert.That(availability.Reason, Is.EqualTo(TenancyArea.SignInReason));
        });
    }

    [Test]
    public async Task An_anonymous_circuit_that_administers_nothing_is_also_told_to_sign_in()
    {
        UseTenancyAs(isOperator: false);
        await Auth.LogoutAsync();
        Cluster.Fail(nameof(FakeTenancyCluster.ListAdminSubjectsAsync), FakeTenancyCluster.Denied());

        Assert.That((await CreateArea().GetAvailabilityAsync(CancellationToken.None)).Reason, Is.EqualTo(TenancyArea.SignInReason));
    }

    [Test]
    [TestCase("unserved")]
    [TestCase("unconfigured")]
    [TestCase("transport")]
    public async Task A_cluster_that_cannot_answer_hides_the_area(string fault)
    {
        UseTenancyAs();
        Exception failure = fault switch
        {
            "unserved" => new NotSupportedException(),
            "unconfigured" => new InvalidOperationException("not configured"),
            _ => new TimeoutException(),
        };
        Cluster.Fail(nameof(FakeTenancyCluster.GetCurrentTenantAsync), failure);

        Assert.That(await CreateArea().GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task An_operator_gate_that_throws_is_read_as_not_an_operator()
    {
        UseTenancyAs(isOperator: false);
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns<ValueTask<bool>>(_ => throw new InvalidOperationException("gate down"));
        Cluster.Fail(nameof(FakeTenancyCluster.ListAdminSubjectsAsync), FakeTenancyCluster.Denied());

        Assert.That(await CreateArea().GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
    }

    [Test]
    public async Task A_cancelled_probe_propagates_and_is_not_remembered()
    {
        UseTenancyAs();
        Cluster.Fail(nameof(FakeTenancyCluster.GetCurrentTenantAsync), new OperationCanceledException());
        var area = CreateArea();

        Assert.ThrowsAsync<OperationCanceledException>(async () => await area.GetAvailabilityAsync(CancellationToken.None));

        Cluster.Heal(nameof(FakeTenancyCluster.GetCurrentTenantAsync));
        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Visible));
    }

    [Test]
    public async Task The_verdict_is_remembered_until_the_identity_changes()
    {
        UseTenancyAs(isOperator: false);
        var area = CreateArea();

        await area.GetAvailabilityAsync(CancellationToken.None);
        await area.GetAvailabilityAsync(CancellationToken.None);
        Assert.That(Cluster.Calls.Count(call => call == nameof(FakeTenancyCluster.GetCurrentTenantAsync)), Is.EqualTo(1));

        Auth.SignIn("someone-else@example.com");
        Cluster.Fail(nameof(FakeTenancyCluster.ListAdminSubjectsAsync), FakeTenancyCluster.Denied());

        Assert.That(await area.GetAvailabilityAsync(CancellationToken.None), Is.EqualTo(AreaAvailability.Hidden));
        Assert.That(Cluster.Calls.Count(call => call == nameof(FakeTenancyCluster.GetCurrentTenantAsync)), Is.EqualTo(2));
    }

    [Test]
    public async Task An_operator_is_offered_both_commands_and_a_tenant_admin_only_the_offer()
    {
        UseTenancyAs(isOperator: true);
        var area = CreateArea();
        Assert.That(area.Commands, Is.Empty, "no command before the standing is proven");

        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(area.Commands.Select(command => command.Id), Is.EqualTo(new[] { TenancyArea.CreateTenantCommandId, TenancyArea.OfferGrantCommandId }));
            Assert.That(area.Commands.Select(command => command.Target!.Format()), Is.EqualTo(new[] { "/tenancy?new=true", "/t/acme/tenancy/sharing?new=true" }));
        });

        Catalog.InvalidateStanding();
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(false));
        await Catalog.GetStandingAsync(CancellationToken.None);

        Assert.That(area.Commands.Select(command => command.Id), Is.EqualTo(new[] { TenancyArea.OfferGrantCommandId }));
    }

    [Test]
    public async Task An_operator_scoped_to_the_default_tenant_is_not_offered_a_grant()
    {
        UseTenancyAs(active: TenantId.DefaultId, isOperator: true);
        // The layout proves operator standing for the reserved default tenant before
        // any area is asked; until it does, tenancy chrome is withheld (tenant-scope.md).
        await Services.GetRequiredService<ExplorerTenancy>().RefreshAsync();
        var area = CreateArea();

        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(area.Commands.Select(command => command.Id), Is.EqualTo(new[] { TenancyArea.CreateTenantCommandId }));
    }

    [Test]
    public async Task Home_counts_tenants_for_an_operator()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex", TenantLifecycleStatus.Suspended).WithTenant("initech");
        var area = CreateArea();
        Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.Null, "nothing before the standing is proven");

        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("3 tenants, 1 suspended."));
        Assert.That(await area.GetDirectoryBadgeAsync(CancellationToken.None), Is.EqualTo("3"));
    }

    [Test]
    public async Task Home_names_one_tenant_without_a_suspension_note()
    {
        UseTenancyAs(isOperator: true);
        var area = CreateArea();
        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("1 tenant."));
    }

    [Test]
    public async Task Home_names_the_tenant_a_tenant_admin_administers_and_shows_no_badge()
    {
        UseTenancyAs(isOperator: false);
        var area = CreateArea();
        await area.GetAvailabilityAsync(CancellationToken.None);

        Assert.That(await area.GetHomeStatusAsync(CancellationToken.None), Is.EqualTo("You administer tenant acme."));
        Assert.That(await area.GetDirectoryBadgeAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task The_directory_lists_the_area_for_an_operator_and_not_without_tenancy()
    {
        UseTenancyAs();
        var shown = await Services.GetRequiredService<ExplorerAreaDirectory>().GetEntriesAsync();

        Assert.That(shown.Select(entry => entry.Area.Key), Does.Contain("tenancy"));
    }

    [Test]
    public async Task The_directory_hides_the_area_when_tenancy_is_off()
    {
        var shown = await Services.GetRequiredService<ExplorerAreaDirectory>().GetEntriesAsync();

        Assert.That(shown.Select(entry => entry.Area.Key), Does.Not.Contain("tenancy"));
    }
}
