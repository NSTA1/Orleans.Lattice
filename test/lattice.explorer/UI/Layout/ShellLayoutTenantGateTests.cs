using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The layout fails closed when tenancy is on and a signed-in caller's tenant
/// could not be established: a call made then would assert no tenant and be
/// served as the reserved default tenant, so no tenant-scoped page renders, and
/// a cluster-wide page still does.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutTenantGateTests : ShellLayoutTestContext
{
    [Test]
    public void A_signed_in_caller_with_no_tenant_established_sees_no_tenant_scoped_page()
    {
        UnresolvedTenancy(signedIn: true);
        Navigation.NavigateTo("data/orders");

        var cut = RenderLayout();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("#page-body"), Is.Empty);
            Assert.That(cut.Find("main h1").TextContent, Is.EqualTo("Tenant not established"));
            Assert.That(cut.Find("main .lt-empty").TextContent, Does.Contain(ShellLayout.TenantUnresolvedReason));
        });
    }

    [Test]
    public void Home_is_withheld_too()
    {
        UnresolvedTenancy(signedIn: true);

        var cut = RenderLayout();

        cut.WaitUntil(() => Assert.That(cut.Find("main h1").TextContent, Is.EqualTo("Tenant not established")));
    }

    [Test]
    public void A_cluster_wide_page_still_renders()
    {
        UnresolvedTenancy(signedIn: true, new FakeArea("cluster", "Cluster") { IsTenantScoped = false });
        Navigation.NavigateTo("cluster");

        var cut = RenderLayout();

        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void An_anonymous_caller_is_not_gated_so_the_areas_can_ask_them_to_sign_in()
    {
        UnresolvedTenancy(signedIn: false);
        Navigation.NavigateTo("data/orders");

        var cut = RenderLayout();

        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void An_established_tenant_renders_its_pages()
    {
        UseTenancy("acme");
        AddArea(new FakeArea("data", "Data"));
        ((FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>()).SignIn("alice");
        Navigation.NavigateTo("t/acme/data/orders");

        var cut = RenderLayout();

        cut.WaitUntil(() => Assert.That(cut.FindAll("main #page-body"), Has.Count.EqualTo(1)));
    }

    private void UnresolvedTenancy(bool signedIn, params FakeArea[] more)
    {
        var view = Substitute.For<IExplorerTenantView>();
        view.IsActive.Returns(true);
        view.ActiveTenant.Returns((ExplorerTenantId?)null);
        Services.AddSingleton(view);
        AddArea(new FakeArea("data", "Data"));
        foreach (var area in more)
        {
            AddArea(area);
        }

        if (signedIn)
        {
            ((FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>()).SignIn("alice");
        }
    }
}
