using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// Issue #3962: the top-bar tenant switch. It is offered only to a signed-in
/// operator with tenancy on and two or more reachable tenants, the offer is filed
/// under the identity and tenant it was read for, and a switch is the address
/// line's operator-gated switch - re-rooting a tenant-scoped address, and
/// switching in place at a cluster-wide one.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ExplorerTenantSwitchTests
{
    private readonly FakeArea _data = new("data", "Data", 1);
    private readonly FakeArea _cluster = new("cluster", "Cluster", 2) { IsTenantScoped = false };
    private readonly FakeAuthSession _auth = new();
    private readonly LtToastService _toasts = new();
    private readonly TestNavigationManager _navigation = new("t/acme/data/orders");
    private IExplorerTenantView _view = null!;
    private IExplorerTenantSwitcher _switcher = null!;
    private IExplorerAccessibleTenantSource _source = null!;
    private string[] _reachable = [];

    [SetUp]
    public void SetUp()
    {
        _view = Substitute.For<IExplorerTenantView>();
        _view.IsActive.Returns(true);
        _view.ActiveTenant.Returns(new ExplorerTenantId("acme"));

        _switcher = Substitute.For<IExplorerTenantSwitcher>();
        _switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));
        _switcher.SwitchTenantAsync(Arg.Any<ExplorerTenantId>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                _view.ActiveTenant.Returns(call.Arg<ExplorerTenantId>());
                return new ValueTask<bool>(true);
            });

        _reachable = ["acme", "default", "globex"];
        _source = Substitute.For<IExplorerAccessibleTenantSource>();
        _source.GetAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<IReadOnlyList<ExplorerTenantId>>([.. _reachable.Select(tenant => new ExplorerTenantId(tenant))]));

        _auth.SignIn("dana");
    }

    [Test]
    public async Task A_signed_in_operator_with_several_tenants_is_offered_every_one_including_default()
    {
        var choices = await Switch().RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(choices.Offered, Is.True);
            Assert.That(choices.Active, Is.EqualTo("acme"));
            Assert.That(choices.Tenants, Is.EqualTo(new[] { "acme", "default", "globex" }));
        });
    }

    [Test]
    public async Task Nothing_is_offered_with_tenancy_off()
    {
        var choices = await Switch(new ExplorerTenancy()).RefreshAsync();

        Assert.That(choices, Is.SameAs(TenantSwitchChoices.None));
    }

    [Test]
    public async Task Nothing_is_offered_with_a_single_reachable_tenant()
    {
        _reachable = ["acme"];

        var choices = await Switch().RefreshAsync();

        Assert.That(choices.Offered, Is.False);
    }

    [Test]
    public async Task Nothing_is_offered_to_a_caller_the_switcher_does_not_let_switch()
    {
        _switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(false));

        var choices = await Switch().RefreshAsync();

        Assert.That(choices.Offered, Is.False);
    }

    [Test]
    public async Task Nothing_is_offered_to_a_signed_out_caller()
    {
        var signedOut = new FakeAuthSession();

        var choices = await new ExplorerTenantSwitch(Tenancy(), Navigator(Tenancy()), _toasts, signedOut).RefreshAsync();

        Assert.That(choices.Offered, Is.False);
        await _source.DidNotReceive().GetAccessibleTenantsAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_fault_reading_the_tenants_offers_nothing()
    {
        _source.GetAccessibleTenantsAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());

        var choices = await Switch().RefreshAsync();

        Assert.That(choices.Offered, Is.False);
    }

    [Test]
    public async Task A_fault_proving_operator_standing_offers_nothing()
    {
        _switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());

        var choices = await Switch().RefreshAsync();

        Assert.That(choices.Offered, Is.False);
    }

    [Test]
    public async Task An_unchanged_offer_is_the_same_instance_so_a_bound_field_keeps_its_state()
    {
        var tenantSwitch = Switch();

        var first = await tenantSwitch.RefreshAsync();
        var second = await tenantSwitch.RefreshAsync();

        Assert.That(second, Is.SameAs(first));
        Assert.That(tenantSwitch.Current, Is.SameAs(first));
    }

    [Test]
    public async Task The_current_offer_is_withdrawn_when_the_caller_signs_out_or_the_identity_changes()
    {
        var tenantSwitch = Switch();
        await tenantSwitch.RefreshAsync();

        _auth.SignIn("erin");
        Assert.That(tenantSwitch.Current.Offered, Is.False, "a new identity never sees the previous caller's list");

        await tenantSwitch.RefreshAsync();
        Assert.That(tenantSwitch.Current.Offered, Is.True, "and it is read again for the new identity");

        await _auth.LogoutAsync();
        Assert.That(tenantSwitch.Current.Offered, Is.False);
    }

    [Test]
    public async Task The_current_offer_is_withdrawn_when_the_active_tenant_changes()
    {
        var tenantSwitch = Switch();
        await tenantSwitch.RefreshAsync();

        _view.ActiveTenant.Returns(new ExplorerTenantId("globex"));

        Assert.That(tenantSwitch.Current.Offered, Is.False);
    }

    [Test]
    public async Task An_offer_read_while_the_identity_changed_is_neither_kept_nor_shown()
    {
        var tenantSwitch = Switch();
        _source.GetAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _auth.SignIn("erin");
                return new ValueTask<IReadOnlyList<ExplorerTenantId>>([new("acme"), new("globex")]);
            });

        var choices = await tenantSwitch.RefreshAsync();

        Assert.That(choices.Offered, Is.False);
        Assert.That(tenantSwitch.Current.Offered, Is.False);
    }

    [Test]
    public async Task Switching_at_a_tenant_scoped_address_re_roots_it_at_the_tenant()
    {
        var outcome = await Switch().SwitchAsync("globex");

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(TenantSwitchOutcome.Navigated));
            Assert.That(_navigation.Navigations.Single(), Is.EqualTo(("t/globex/data/orders", false)));
            Assert.That(_toasts.Toasts, Is.Empty, "the layout resolves and announces the switch, as for a typed address");
        });
        await _switcher.DidNotReceive().SwitchTenantAsync(Arg.Any<ExplorerTenantId>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Switching_at_a_cluster_wide_address_stays_there_and_switches_in_place()
    {
        _navigation.NavigateTo("cluster/shards");
        _navigation.Navigations.Clear();

        var outcome = await Switch().SwitchAsync("globex");

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(TenantSwitchOutcome.Switched));
            Assert.That(_view.ActiveTenant, Is.EqualTo(new ExplorerTenantId("globex")));
            Assert.That(_navigation.Navigations.Single(), Is.EqualTo(("cluster/shards", true)), "the layout is asked to synchronise without moving");
            Assert.That(_toasts.Toasts.Single().Message, Is.EqualTo(ExplorerNavigator.SwitchedNotice("globex")));
        });
    }

    [Test]
    public async Task A_refused_switch_at_a_cluster_wide_address_shows_the_address_lines_notice()
    {
        _switcher.SwitchTenantAsync(Arg.Any<ExplorerTenantId>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(false));
        _navigation.NavigateTo("cluster");
        _navigation.Navigations.Clear();

        var outcome = await Switch().SwitchAsync("globex");

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(TenantSwitchOutcome.Refused));
            Assert.That(_navigation.Navigations, Is.Empty);
            Assert.That(_toasts.Toasts.Single().Message, Is.EqualTo(ExplorerNavigator.RefusedNotice("globex", "acme")));
            Assert.That(_toasts.Toasts.Single().Tone, Is.EqualTo(LtToastTone.Warning));
        });
    }

    [Test]
    public async Task Choosing_the_active_tenant_changes_nothing()
    {
        var outcome = await Switch().SwitchAsync("acme");

        Assert.That(outcome, Is.EqualTo(TenantSwitchOutcome.Unchanged));
        Assert.That(_navigation.Navigations, Is.Empty);
    }

    [Test]
    public void An_open_request_is_raised_and_taken_exactly_once()
    {
        var tenantSwitch = Switch();
        var raised = 0;
        tenantSwitch.OpenRequested += () => raised++;

        tenantSwitch.RequestOpen();

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(1));
            Assert.That(tenantSwitch.IsOpenRequested, Is.True);
            Assert.That(tenantSwitch.TryTakeOpenRequest(), Is.True);
            Assert.That(tenantSwitch.TryTakeOpenRequest(), Is.False);
            Assert.That(tenantSwitch.IsOpenRequested, Is.False);
        });
    }

    [Test]
    public async Task Tenancy_can_switch_only_for_a_proven_operator_with_tenancy_on()
    {
        var operatorOn = await Tenancy().CanSwitchAsync();
        var tenancyOff = await new ExplorerTenancy().CanSwitchAsync();
        var noSwitcher = await new ExplorerTenancy(_view).CanSwitchAsync();
        _switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).ThrowsAsync(new InvalidOperationException());
        var faulted = await Tenancy().CanSwitchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(operatorOn, Is.True);
            Assert.That(tenancyOff, Is.False, "tenancy off");
            Assert.That(noSwitcher, Is.False, "no switcher");
            Assert.That(faulted, Is.False, "a fault reads as may not");
        });
    }

    [Test]
    public void The_switch_rejects_missing_collaborators_and_an_empty_tenant()
    {
        var tenancy = Tenancy();
        var navigator = Navigator(tenancy);

        Assert.Multiple(() =>
        {
            Assert.That(() => new ExplorerTenantSwitch(null!, navigator, _toasts), Throws.ArgumentNullException);
            Assert.That(() => new ExplorerTenantSwitch(tenancy, null!, _toasts), Throws.ArgumentNullException);
            Assert.That(() => new ExplorerTenantSwitch(tenancy, navigator, null!), Throws.ArgumentNullException);
            Assert.That(async () => await Switch().SwitchAsync(string.Empty), Throws.ArgumentException);
        });
    }

    [Test]
    public void The_notices_name_the_tenant_asked_for_and_the_one_kept()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerNavigator.SwitchedNotice("globex"), Is.EqualTo("Scoped to tenant globex."));
            Assert.That(ExplorerNavigator.RefusedNotice("globex", "acme"), Is.EqualTo("You can't scope to tenant globex, so this shows tenant acme instead."));
            Assert.That(ExplorerNavigator.RefusedNotice("globex", null), Is.EqualTo("You can't scope to tenant globex."));
        });
    }

    private ExplorerTenancy Tenancy() => new(_view, _switcher, _source);

    private ExplorerNavigator Navigator(ExplorerTenancy tenancy) =>
        new(_navigation, new ExplorerAreaDirectory([_data, _cluster], new ExplorerChromeOptions(), new ManualTimeProvider()), tenancy);

    private ExplorerTenantSwitch Switch(ExplorerTenancy? tenancy = null)
    {
        tenancy ??= Tenancy();
        return new ExplorerTenantSwitch(tenancy, Navigator(tenancy), _toasts, _auth, new ShellAssertedTenant(new AssertedFromView(_view)));
    }

    /// <summary>Asserts the view's active tenant, as Core's provider does for a non-default one.</summary>
    private sealed class AssertedFromView(IExplorerTenantView view) : ILatticeActiveTenantProvider
    {
        public string? AssertedTenant => view.ActiveTenant?.Value;
    }
}
