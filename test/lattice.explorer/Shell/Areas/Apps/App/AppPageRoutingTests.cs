using System.Reflection;
using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Routing;
using Orleans.Lattice.Explorer.Shell.Areas.Apps.App;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Apps.App;

/// <summary>
/// The app page's routes, resolved by the real Blazor router beside a stand-in for the
/// catalogue's literal <c>/apps/catalogue</c> routes (A1): a literal segment always wins, so
/// the catalogue never resolves to an app page, while every app section - including a deep
/// in-frame path - does.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppPageRoutingTests : AppPageTestContext
{
    [TestCase("apps/catalogue", nameof(CatalogueRoutesStandIn))]
    [TestCase("apps/catalogue?source=all&filter=installed", nameof(CatalogueRoutesStandIn))]
    [TestCase("apps/catalogue/in-image/crm", nameof(CatalogueRoutesStandIn))]
    [TestCase("t/acme/apps/catalogue", nameof(CatalogueRoutesStandIn))]
    [TestCase("apps/crm", nameof(AppPage))]
    [TestCase("apps/crm/trees", nameof(AppPage))]
    [TestCase("apps/crm/open", nameof(AppPage))]
    [TestCase("apps/crm/open/board/42/cards/7", nameof(AppPage))]
    [TestCase("t/acme/apps/crm/consent", nameof(AppPage))]
    [TestCase("t/acme/apps/crm/open/board", nameof(AppPage))]
    public void The_router_resolves_the_catalogue_to_its_own_page_and_every_app_section_to_the_app_page(string address, string page)
    {
        var cut = Render<Router>(parameters => parameters
            .Add(router => router.AppAssembly, typeof(AppPage).Assembly)
            .Add(router => router.AdditionalAssemblies, new[] { typeof(CatalogueRoutesStandIn).Assembly })
            .Add(router => router.Found, (RouteData route) => builder => builder.AddContent(0, route.PageType.Name)));

        Navigation.NavigateTo(address);

        cut.WaitForAssertion(() => Assert.That(cut.Markup, Is.EqualTo(page)));
    }

    [Test]
    public void Every_app_page_route_is_lower_case_under_the_apps_literal_both_plain_and_tenant_rooted_with_no_catch_all()
    {
        var templates = typeof(AppPage).GetCustomAttributes<RouteAttribute>().Select(route => route.Template).ToArray();

        Assert.That(templates, Is.EquivalentTo(new[]
        {
            "/apps/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}",
            "/t/{tenant}/apps/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}",
        }));
    }

    /// <summary>A stand-in for the catalogue pages' literal routes, which A1 owns.</summary>
    [Route("/apps/catalogue")]
    [Route("/apps/catalogue/{source}/{slug}")]
    [Route("/t/{tenant}/apps/catalogue")]
    [Route("/t/{tenant}/apps/catalogue/{source}/{slug}")]
    public sealed class CatalogueRoutesStandIn : ComponentBase
    {
        /// <summary>The tenant route parameter.</summary>
        [Parameter]
        public string? Tenant { get; set; }

        /// <summary>The source route parameter.</summary>
        [Parameter]
        public string? Source { get; set; }

        /// <summary>The slug route parameter.</summary>
        [Parameter]
        public string? Slug { get; set; }
    }
}
