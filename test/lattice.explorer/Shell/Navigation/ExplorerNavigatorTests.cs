using NSubstitute;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>
/// The navigator: canonical addresses for tenancy off and on, the operator-gated
/// re-rooting of a tenant address, and the nearest valid ancestor of an address
/// that did not resolve.
/// </summary>
[TestFixture]
public sealed class ExplorerNavigatorTests
{
    private readonly FakeArea _data = new("data", "Data", 1);
    private readonly FakeArea _cluster = new("cluster", "Cluster", 2) { IsTenantScoped = false };

    [Test]
    public void With_tenancy_off_no_address_carries_a_tenant()
    {
        var navigator = Navigator(new ExplorerTenancy());

        Assert.Multiple(() =>
        {
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/acme/data/orders")).Format(), Is.EqualTo("/data/orders"));
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/acme")).Format(), Is.EqualTo("/"));
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/data")).Format(), Is.EqualTo("/data"));
        });
    }

    [Test]
    public void With_tenancy_on_a_tenant_scoped_address_is_rooted_at_the_active_tenant()
    {
        var navigator = Navigator(Tenancy("acme", out _));

        Assert.Multiple(() =>
        {
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/data/orders")).Format(), Is.EqualTo("/t/acme/data/orders"));
            Assert.That(navigator.Canonicalize(ExplorerAddress.Home).Format(), Is.EqualTo("/t/acme"));
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/globex/data")).Format(), Is.EqualTo("/t/globex/data"),
                "an explicit tenant is left for ResolveAsync to switch or refuse");
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/acme/cluster")).Format(), Is.EqualTo("/cluster"),
                "a cluster-wide area never carries a tenant");
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/unknown")).Format(), Is.EqualTo("/t/acme/unknown"));
        });
    }

    [Test]
    public void With_tenancy_on_and_no_active_tenant_nothing_is_rooted()
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        var tenancy = new ExplorerTenancy(view);

        Assert.Multiple(() =>
        {
            Assert.That(tenancy.ActiveTenant, Is.Null);
            Assert.That(Navigator(tenancy).Canonicalize(ExplorerAddress.Parse("/data")).Format(), Is.EqualTo("/data"));
        });
    }

    [Test]
    public async Task Resolving_a_canonical_address_changes_nothing()
    {
        var resolution = await Navigator(new ExplorerTenancy()).ResolveAsync(ExplorerAddress.Parse("/data/orders"));

        Assert.Multiple(() =>
        {
            Assert.That(resolution.Address.Format(), Is.EqualTo("/data/orders"));
            Assert.That(resolution.RedirectTo, Is.Null);
            Assert.That(resolution.Notice, Is.Null);
        });
    }

    [Test]
    public async Task Tenancy_off_redirects_a_tenant_address_to_its_plain_form()
    {
        var resolution = await Navigator(new ExplorerTenancy()).ResolveAsync(ExplorerAddress.Parse("/t/acme/data/orders?key=k"));

        Assert.Multiple(() =>
        {
            Assert.That(resolution.RedirectTo!.Format(), Is.EqualTo("/data/orders?key=k"));
            Assert.That(resolution.Notice, Is.Null);
        });
    }

    [Test]
    public async Task Another_tenants_address_switches_tenant_when_the_switch_is_granted()
    {
        var tenancy = Tenancy("acme", out var switcher, allowSwitch: true);

        var resolution = await Navigator(tenancy).ResolveAsync(ExplorerAddress.Parse("/t/globex/data"));

        Assert.Multiple(() =>
        {
            Assert.That(resolution.RedirectTo, Is.Null);
            Assert.That(resolution.Address.Format(), Is.EqualTo("/t/globex/data"));
            Assert.That(resolution.Notice, Is.EqualTo("Scoped to tenant globex."));
        });
        await switcher.Received(1).SwitchTenantAsync(new ExplorerTenantId("globex"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_refused_switch_redirects_back_to_the_active_tenant_and_says_why()
    {
        var resolution = await Navigator(Tenancy("acme", out _)).ResolveAsync(ExplorerAddress.Parse("/t/globex/data/orders"));

        Assert.Multiple(() =>
        {
            Assert.That(resolution.RedirectTo!.Format(), Is.EqualTo("/t/acme/data/orders"));
            Assert.That(resolution.Notice, Is.EqualTo("You can't scope to tenant globex, so this shows tenant acme instead."));
        });
    }

    [Test]
    public async Task A_missing_tenant_root_is_added_by_redirect()
    {
        var resolution = await Navigator(Tenancy("acme", out _)).ResolveAsync(ExplorerAddress.Parse("/data"));

        Assert.That(resolution.RedirectTo!.Format(), Is.EqualTo("/t/acme/data"));
    }

    [Test]
    public void NavigateTo_sends_the_canonical_base_relative_address()
    {
        var navigation = new TestNavigationManager();
        var navigator = Navigator(Tenancy("acme", out _), navigation);

        navigator.NavigateTo(ExplorerAddress.Parse("/data/Orders"), replace: true);

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Navigations.Single(), Is.EqualTo(("t/acme/data/%4Frders", true)));
            Assert.That(navigator.Current!.Format(), Is.EqualTo("/t/acme/data/%4Frders"));
            Assert.That(navigator.CurrentRelativePath, Is.EqualTo("/t/acme/data/%4Frders"));
        });
    }

    [Test]
    public void Current_is_null_for_a_url_that_is_not_an_address()
    {
        var navigator = Navigator(new ExplorerTenancy(), new TestNavigationManager("data/%ZZ"));

        Assert.Multiple(() =>
        {
            Assert.That(navigator.Current, Is.Null);
            Assert.That(navigator.CurrentRelativePath, Is.EqualTo("/data/%ZZ"));
        });
    }

    [Test]
    public void ReRoot_moves_a_tenant_scoped_address_and_leaves_a_cluster_wide_one()
    {
        var navigator = Navigator(Tenancy("acme", out _));

        Assert.Multiple(() =>
        {
            Assert.That(navigator.ReRoot(ExplorerAddress.Parse("/t/acme/data/orders"), "globex").Format(), Is.EqualTo("/t/globex/data/orders"));
            Assert.That(navigator.ReRoot(ExplorerAddress.Parse("/cluster"), "globex").Format(), Is.EqualTo("/cluster"));
            Assert.That(() => navigator.ReRoot(ExplorerAddress.Home, string.Empty), Throws.ArgumentException);
        });
    }

    [Test]
    public async Task The_nearest_valid_ancestor_is_the_area_root_when_the_area_is_visible()
    {
        var navigator = Navigator(new ExplorerTenancy());

        Assert.Multiple(async () =>
        {
            Assert.That((await navigator.GetNearestValidAncestorAsync(ExplorerAddress.Parse("/data/nope?key=k"))).Format(), Is.EqualTo("/data"));
            Assert.That((await navigator.GetNearestValidAncestorAsync(ExplorerAddress.Parse("/data"))).Format(), Is.EqualTo("/"),
                "the area root itself did not resolve, so its ancestor is Home");
            Assert.That((await navigator.GetNearestValidAncestorAsync(ExplorerAddress.Parse("/unknown/x"))).Format(), Is.EqualTo("/"));
            Assert.That((await navigator.GetNearestValidAncestorAsync(null)).Format(), Is.EqualTo("/"));
        });
    }

    [Test]
    public async Task The_nearest_valid_ancestor_of_a_hidden_area_is_home_under_the_tenant()
    {
        _data.Availability = _ => ValueTask.FromResult(AreaAvailability.Hidden);
        var navigator = Navigator(Tenancy("acme", out _));

        var ancestor = await navigator.GetNearestValidAncestorAsync(ExplorerAddress.Parse("/t/acme/data/orders"));

        Assert.That(ancestor.Format(), Is.EqualTo("/t/acme"));
    }

    [Test]
    public void Null_arguments_are_rejected()
    {
        var navigator = Navigator(new ExplorerTenancy());
        var directory = new ExplorerAreaDirectory([], new ExplorerChromeOptions(), new ManualTimeProvider());

        Assert.Multiple(() =>
        {
            Assert.That(() => new ExplorerNavigator(null!, directory, new ExplorerTenancy()), Throws.ArgumentNullException);
            Assert.That(() => new ExplorerNavigator(new TestNavigationManager(), null!, new ExplorerTenancy()), Throws.ArgumentNullException);
            Assert.That(() => new ExplorerNavigator(new TestNavigationManager(), directory, null!), Throws.ArgumentNullException);
            Assert.That(() => navigator.Canonicalize(null!), Throws.ArgumentNullException);
            Assert.That(() => navigator.NavigateTo(null!), Throws.ArgumentNullException);
            Assert.That(() => navigator.ReRoot(null!, "t"), Throws.ArgumentNullException);
            Assert.That(async () => await navigator.ResolveAsync(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task Tenancy_reads_Core_fail_closed()
    {
        var off = new ExplorerTenancy();
        var on = Tenancy("acme", out _, allowSwitch: false);

        Assert.Multiple(async () =>
        {
            Assert.That(off.IsActive, Is.False);
            Assert.That(off.ActiveTenant, Is.Null);
            Assert.That(await off.GetAccessibleTenantsAsync(), Is.Empty);
            Assert.That(await off.TrySwitchAsync("acme"), Is.False);
            Assert.That(on.IsActive, Is.True);
            Assert.That(await on.TrySwitchAsync("acme"), Is.True, "the active tenant needs no switch");
            Assert.That(await on.TrySwitchAsync("globex"), Is.False);
            Assert.That(await new ExplorerTenancy(ViewOf("acme")).GetAccessibleTenantsAsync(), Is.EqualTo(new[] { "acme" }),
                "without a source, only the active tenant is known");
            Assert.That(await new ExplorerTenancy(ViewOf("acme")).TrySwitchAsync("globex"), Is.False, "without a switcher nothing switches");
            Assert.That(async () => await on.TrySwitchAsync(string.Empty), Throws.ArgumentException);
        });
    }

    private static IExplorerTenantView ViewOf(string active)
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        view.ActiveTenant.Returns(new ExplorerTenantId(active));
        return view;
    }

    [Test]
    public async Task A_default_tenant_non_operator_sees_no_tenancy_chrome()
    {
        var tenancy = Tenancy("default", out var switcher);
        switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(false));

        await tenancy.RefreshAsync();
        var navigator = Navigator(tenancy);
        var resolution = await navigator.ResolveAsync(ExplorerAddress.Parse("/t/default/data"));

        Assert.Multiple(() =>
        {
            Assert.That(tenancy.IsActive, Is.False);
            Assert.That(tenancy.ActiveTenant, Is.Null);
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/t/default/data")).Format(), Is.EqualTo("/data"));
            Assert.That(navigator.Canonicalize(ExplorerAddress.Home).Format(), Is.EqualTo("/"));
            Assert.That(resolution.RedirectTo!.Format(), Is.EqualTo("/data"));
        });
    }

    [Test]
    public async Task A_default_tenant_operator_keeps_the_tenant_root()
    {
        var tenancy = Tenancy("default", out var switcher);
        switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));

        await tenancy.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tenancy.IsActive, Is.True);
            Assert.That(Navigator(tenancy).Canonicalize(ExplorerAddress.Parse("/data")).Format(), Is.EqualTo("/t/default/data"));
        });
    }

    [Test]
    public void A_default_tenant_caller_is_treated_as_a_non_operator_until_the_verdict_is_read()
    {
        var tenancy = Tenancy("default", out var switcher);
        switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));

        Assert.That(tenancy.IsActive, Is.False);
    }

    [Test]
    public async Task A_faulted_operator_verdict_fails_closed_to_no_tenancy_chrome()
    {
        var tenancy = Tenancy("default", out var switcher);
        switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns<ValueTask<bool>>(_ => throw new InvalidOperationException("probe failed"));

        await tenancy.RefreshAsync();

        Assert.That(tenancy.IsActive, Is.False);
    }

    [Test]
    public async Task The_operator_verdict_is_asked_only_for_the_default_tenant()
    {
        var tenancy = Tenancy("acme", out var switcher);

        await tenancy.RefreshAsync();

        Assert.That(tenancy.IsActive, Is.True);
        await switcher.DidNotReceive().IsOperatorAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_operator_verdict_for_the_default_tenant_does_not_outlive_a_scope_change()
    {
        var view = ViewOf("default");
        var switcher = Substitute.For<IExplorerTenantSwitcher>();
        switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true), new ValueTask<bool>(false));
        var tenancy = new ExplorerTenancy(view, switcher);

        await tenancy.RefreshAsync();
        var asOperator = tenancy.IsActive;
        view.ActiveTenant.Returns(new ExplorerTenantId("acme"));
        await tenancy.RefreshAsync();
        view.ActiveTenant.Returns(new ExplorerTenantId("default"));
        await tenancy.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(asOperator, Is.True);
            Assert.That(tenancy.IsActive, Is.False, "the verdict is read again for the default tenant, and now denies");
        });
    }

    private static ExplorerTenancy Tenancy(string active, out IExplorerTenantSwitcher switcher, bool allowSwitch = false)
    {
        var view = ViewOf(active);
        var granted = Substitute.For<IExplorerTenantSwitcher>();
        granted.SwitchTenantAsync(Arg.Any<ExplorerTenantId>(), Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(allowSwitch));
        switcher = granted;
        return new ExplorerTenancy(view, granted);
    }

    private ExplorerNavigator Navigator(ExplorerTenancy tenancy, TestNavigationManager? navigation = null) =>
        new(
            navigation ?? new TestNavigationManager(),
            new ExplorerAreaDirectory([_data, _cluster], new ExplorerChromeOptions(), new ManualTimeProvider()),
            tenancy);
}
