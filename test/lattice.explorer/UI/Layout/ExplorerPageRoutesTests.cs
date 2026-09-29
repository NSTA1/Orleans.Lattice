using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Pages;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Whether an address is one of a page type's own routes, which decides whether
/// the page accepts the location the layout cascades.
/// </summary>
[TestFixture]
public sealed class ExplorerPageRoutesTests
{
    [TestCase("/access", true)]
    [TestCase("/access?filter=app", true)]
    [TestCase("/t/acme/access", true)]
    [TestCase("/access/rules", false)]
    [TestCase("/cluster", false)]
    [TestCase("/", false)]
    [TestCase("/t/acme", false)]
    public void A_literal_page_answers_only_its_own_addresses(string address, bool answers)
    {
        Assert.That(ExplorerPageRoutes.For(typeof(LiteralPage)).Answers(ExplorerAddress.Parse(address)), Is.EqualTo(answers));
    }

    [TestCase("/data", false)]
    [TestCase("/data/orders", true)]
    [TestCase("/data/orders/2024", true)]
    [TestCase("/data/orders/2024/q1", true)]
    [TestCase("/data/orders/2024/q1/extra", false)]
    [TestCase("/t/acme/data/orders", true)]
    [TestCase("/t/acme/schema/orders", false)]
    public void Required_and_optional_parameters_bound_the_segment_count(string address, bool answers)
    {
        Assert.That(ExplorerPageRoutes.For(typeof(ParameterPage)).Answers(ExplorerAddress.Parse(address)), Is.EqualTo(answers));
    }

    [TestCase("/", true)]
    [TestCase("/t/acme", true)]
    [TestCase("/data", false)]
    public void Home_answers_the_root_and_a_tenant_root(string address, bool answers)
    {
        Assert.That(ExplorerPageRoutes.For(typeof(HomeProbePage)).Answers(ExplorerAddress.Parse(address)), Is.EqualTo(answers));
    }

    [Test]
    public void A_catch_all_takes_every_remaining_segment()
    {
        var routes = ExplorerPageRoutes.For(typeof(CatchAllPage));

        Assert.Multiple(() =>
        {
            Assert.That(routes.Answers(ExplorerAddress.Parse("/apps")), Is.True);
            Assert.That(routes.Answers(ExplorerAddress.Parse("/apps/a/b/c/d/e/f/g")), Is.True);
            Assert.That(routes.Answers(ExplorerAddress.Parse("/data/a")), Is.False);
        });
    }

    [Test]
    public void A_page_without_a_route_answers_every_address()
    {
        var routes = ExplorerPageRoutes.For(typeof(UnroutedPage));

        Assert.Multiple(() =>
        {
            Assert.That(routes.IsRouted, Is.False);
            Assert.That(routes.Answers(ExplorerAddress.Parse("/anything/at/all")), Is.True);
            Assert.That(ExplorerPageRoutes.For(typeof(LiteralPage)).IsRouted, Is.True);
        });
    }

    [Test]
    public void The_routes_of_a_page_type_are_read_once()
    {
        Assert.That(ExplorerPageRoutes.For(typeof(LiteralPage)), Is.SameAs(ExplorerPageRoutes.For(typeof(LiteralPage))));
    }

    [Test]
    public void The_shipped_pages_answer_their_own_canonical_addresses()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ExplorerPageRoutes.For(typeof(HomePage)).Answers(ExplorerAddress.Parse("/t/default")), Is.True);
            Assert.That(ExplorerPageRoutes.For(typeof(HomePage)).Answers(ExplorerAddress.Parse("/access")), Is.False);
            Assert.That(ExplorerPageRoutes.For(typeof(NotFoundPage)).Answers(ExplorerAddress.Parse("/not-found")), Is.True);
        });
    }

    [Test]
    public void Null_arguments_are_refused()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => ExplorerPageRoutes.For(null!), Throws.ArgumentNullException);
            Assert.That(() => ExplorerPageRoutes.For(typeof(LiteralPage)).Answers(null!), Throws.ArgumentNullException);
        });
    }

    [Route("/access")]
    [Route("/t/{tenant}/access")]
    private sealed class LiteralPage : ComponentBase
    {
    }

    [Route("/data/{p1}/{p2?}/{p3?}")]
    [Route("/t/{tenant}/data/{p1}/{p2?}/{p3?}")]
    private sealed class ParameterPage : ComponentBase
    {
    }

    [Route("/")]
    [Route("/t/{tenant}")]
    private sealed class HomeProbePage : ComponentBase
    {
    }

    [Route("/apps/{*rest}")]
    private sealed class CatchAllPage : ComponentBase
    {
    }

    private sealed class UnroutedPage : ComponentBase
    {
    }
}
