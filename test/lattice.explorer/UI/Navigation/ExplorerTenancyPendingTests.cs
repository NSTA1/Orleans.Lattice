using NSubstitute;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// Issue #3999: while the tenant a circuit will hold is not known yet, the chrome's view
/// of tenancy reports none active and offers no switch, so nothing is addressed or drawn
/// under the tenant a prerender would have had to guess.
/// </summary>
[TestFixture]
public sealed class ExplorerTenancyPendingTests
{
    [Test]
    public void A_pending_tenant_is_not_reported_active_and_a_settled_one_is()
    {
        var tenancy = Tenancy();

        Assert.Multiple(() =>
        {
            Assert.That(tenancy.IsTenantPending, Is.False, "nothing is pending until the layout says so");
            Assert.That(tenancy.ActiveTenant, Is.EqualTo("acme"));
        });

        tenancy.IsTenantPending = true;
        Assert.Multiple(() =>
        {
            Assert.That(tenancy.ActiveTenant, Is.Null);
            Assert.That(tenancy.IsActive, Is.True, "tenancy stays on; only the tenant is not known");
        });

        tenancy.IsTenantPending = false;
        Assert.That(tenancy.ActiveTenant, Is.EqualTo("acme"));
    }

    [Test]
    public async Task No_switch_is_offered_while_the_tenant_is_pending()
    {
        var tenancy = Tenancy();
        await tenancy.RefreshAsync();

        tenancy.IsTenantPending = true;
        var pending = await tenancy.CanSwitchAsync();
        tenancy.IsTenantPending = false;
        var settled = await tenancy.CanSwitchAsync();

        Assert.Multiple(() =>
        {
            Assert.That(pending, Is.False);
            Assert.That(settled, Is.True);
        });
    }

    [Test]
    public async Task A_pending_tenant_roots_no_address_at_the_guess()
    {
        var tenancy = Tenancy();
        await tenancy.RefreshAsync();
        var navigator = new ExplorerNavigator(
            new TestNavigationManager(),
            new ExplorerAreaDirectory([new FakeArea("data", "Data", 1)], new ExplorerChromeOptions(), new ManualTimeProvider()),
            tenancy);

        tenancy.IsTenantPending = true;

        Assert.Multiple(() =>
        {
            Assert.That(navigator.Canonicalize(ExplorerAddress.Home).Format(), Is.EqualTo("/"));
            Assert.That(navigator.Canonicalize(ExplorerAddress.Parse("/data")).Format(), Is.EqualTo("/data"));
        });
    }

    private static ExplorerTenancy Tenancy()
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        view.ActiveTenant.Returns(new ExplorerTenantId("acme"));
        var switcher = Substitute.For<IExplorerTenantSwitcher>();
        switcher.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));
        return new ExplorerTenancy(view, switcher);
    }
}
