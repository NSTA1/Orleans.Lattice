using NSubstitute;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// The reserved default tenant is reachable for an operator: switching to it from
/// another tenant lands on its tenant-rooted address rather than being bounced
/// off it, while a non-operator's addresses are unchanged. Also the navigator's
/// tenant-scope question and the chrome's reading of an unresolved tenant.
/// </summary>
[TestFixture]
public sealed class ExplorerNavigatorDefaultTenantTests
{
    private readonly FakeArea _data = new("data", "Data", 1);
    private readonly FakeArea _cluster = new("cluster", "Cluster", 2) { IsTenantScoped = false };

    [Test]
    public async Task An_operator_opening_the_default_tenant_from_another_lands_on_it_without_a_redirect()
    {
        var (tenancy, switcher) = Tenancy("acme", isOperator: true, allowSwitch: true);
        await tenancy.RefreshAsync();

        var resolution = await Navigator(tenancy).ResolveAsync(ExplorerAddress.Parse("/t/default/data"));

        Assert.Multiple(() =>
        {
            Assert.That(resolution.RedirectTo, Is.Null);
            Assert.That(resolution.Address.Format(), Is.EqualTo("/t/default/data"));
            Assert.That(resolution.Notice, Is.EqualTo("Scoped to tenant default."));
            Assert.That(tenancy.ActiveTenant, Is.EqualTo("default"));
        });
        await switcher.Received(1).SwitchTenantAsync(ExplorerTenantId.Default, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_non_operator_opening_the_default_tenant_is_sent_back_to_their_own()
    {
        var (tenancy, _) = Tenancy("acme", isOperator: false, allowSwitch: false);
        await tenancy.RefreshAsync();

        var resolution = await Navigator(tenancy).ResolveAsync(ExplorerAddress.Parse("/t/default/data"));

        Assert.That(resolution.RedirectTo!.Format(), Is.EqualTo("/t/acme/data"));
    }

    [Test]
    public void IsTenantScoped_answers_for_home_tenant_scoped_and_cluster_wide_addresses()
    {
        var navigator = Navigator(new ExplorerTenancy());

        Assert.Multiple(() =>
        {
            Assert.That(navigator.IsTenantScoped(ExplorerAddress.Home), Is.True);
            Assert.That(navigator.IsTenantScoped(ExplorerAddress.Parse("/data/orders")), Is.True);
            Assert.That(navigator.IsTenantScoped(ExplorerAddress.Parse("/unknown")), Is.True);
            Assert.That(navigator.IsTenantScoped(ExplorerAddress.Parse("/cluster")), Is.False);
            Assert.That(() => navigator.IsTenantScoped(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void A_tenant_is_unresolved_only_when_tenancy_is_on_and_none_is_established()
    {
        var unresolved = Substitute.For<IExplorerTenantView>();
        unresolved.IsActive.Returns(true);
        var (established, _) = Tenancy("acme", isOperator: false, allowSwitch: false);

        Assert.Multiple(() =>
        {
            Assert.That(new ExplorerTenancy().IsTenantUnresolved, Is.False, "tenancy off");
            Assert.That(new ExplorerTenancy(unresolved).IsTenantUnresolved, Is.True);
            Assert.That(established.IsTenantUnresolved, Is.False);
        });
    }

    private static (ExplorerTenancy Tenancy, IExplorerTenantSwitcher Switcher) Tenancy(string active, bool isOperator, bool allowSwitch)
    {
        ExplorerTenantId? current = new ExplorerTenantId(active);
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        view.ActiveTenant.Returns(_ => current);
        var switcher = Substitute.For<IExplorerTenantSwitcher>();
        switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(isOperator));
        switcher.SwitchTenantAsync(Arg.Any<ExplorerTenantId>(), Arg.Any<CancellationToken>()).Returns(call =>
        {
            if (allowSwitch)
            {
                current = call.Arg<ExplorerTenantId>();
            }

            return new ValueTask<bool>(allowSwitch);
        });
        return (new ExplorerTenancy(view, switcher), switcher);
    }

    private ExplorerNavigator Navigator(ExplorerTenancy tenancy) =>
        new(
            new TestNavigationManager(),
            new ExplorerAreaDirectory([_data, _cluster], new ExplorerChromeOptions(), new ManualTimeProvider()),
            tenancy);
}
