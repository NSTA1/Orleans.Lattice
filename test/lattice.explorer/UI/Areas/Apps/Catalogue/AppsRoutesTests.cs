using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>The Apps area's address grammar: the query string is the catalogue's state.</summary>
[TestFixture]
public sealed class AppsRoutesTests
{
    [Test]
    public void The_catalogue_address_carries_source_filter_and_text_and_reads_back()
    {
        var view = new AppsCatalogueView("nuget-contoso", AvailableAppFilter.Updates, "board");

        var address = AppsRoutes.Catalogue("acme", view);

        Assert.Multiple(() =>
        {
            Assert.That(address.Format(), Is.EqualTo("/t/acme/apps/catalogue?source=nuget-contoso&filter=updates&q=board"));
            Assert.That(AppsCatalogueView.FromAddress(ExplorerAddress.Parse(address.Format())), Is.EqualTo(view));
        });
    }

    [Test]
    public void Every_source_is_all_and_an_unknown_filter_reads_as_all()
    {
        var address = AppsRoutes.Catalogue(null, AppsCatalogueView.Default);
        var odd = AppsCatalogueView.FromAddress(ExplorerAddress.Parse("/apps/catalogue?filter=bogus&q=%20%20"));

        Assert.Multiple(() =>
        {
            Assert.That(address.Format(), Is.EqualTo("/apps/catalogue?source=all&filter=all"));
            Assert.That(odd, Is.EqualTo(AppsCatalogueView.Default));
        });
    }

    [Test]
    public void The_listing_query_sends_the_text_only_where_a_source_can_search()
    {
        var view = new AppsCatalogueView(null, AvailableAppFilter.Available, "crm");

        Assert.Multiple(() =>
        {
            Assert.That(view.ToQuery(textHonoured: true, "10").Text, Is.EqualTo("crm"));
            Assert.That(view.ToQuery(textHonoured: false).Text, Is.Null);
            Assert.That(view.ToQuery(textHonoured: true, "10").Continuation, Is.EqualTo("10"));
            Assert.That(view.ToQuery(textHonoured: true).Filter, Is.EqualTo(AvailableAppFilter.Available));
        });
    }

    [TestCase(AvailableAppFilter.All, "all")]
    [TestCase(AvailableAppFilter.Installed, "installed")]
    [TestCase(AvailableAppFilter.Available, "available")]
    [TestCase(AvailableAppFilter.Updates, "updates")]
    public void Each_filter_round_trips_through_its_query_text(AvailableAppFilter filter, string text)
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsRoutes.FilterText(filter), Is.EqualTo(text));
            Assert.That(AppsRoutes.ReadFilter(text.ToUpperInvariant()), Is.EqualTo(filter));
        });
    }

    [Test]
    public void The_review_address_names_source_slug_and_optional_version()
    {
        var newest = AppsRoutes.Review(null, "in-image", "task-board");
        var pinned = AppsRoutes.Review("acme", "in-image", "task-board", "1.2.0");

        Assert.Multiple(() =>
        {
            Assert.That(newest.Format(), Is.EqualTo("/apps/catalogue/in-image/task-board"));
            Assert.That(pinned.Path, Is.EqualTo(new[] { "catalogue", "in-image", "task-board@1.2.0" }));
            Assert.That(pinned.Tenant, Is.EqualTo("acme"));
            Assert.That(ExplorerAddress.Parse(pinned.Format()), Is.EqualTo(pinned));
            Assert.That(() => AppsRoutes.Review(null, " ", "x"), Throws.ArgumentException);
        });
    }

    [TestCase("task-board", true, "task-board", null)]
    [TestCase("task-board@2.0.0", true, "task-board", "2.0.0")]
    [TestCase("@2.0.0", false, "", null)]
    [TestCase("task-board@", false, "", null)]
    [TestCase("", false, "", null)]
    public void A_slug_segment_reads_its_optional_version(string segment, bool ok, string slug, string? version)
    {
        var read = AppsRoutes.TryReadSlugSegment(segment, out var readSlug, out var readVersion);

        Assert.That((read, readSlug, readVersion), Is.EqualTo((ok, slug, version)));
    }

    [Test]
    public void App_pages_and_their_ui_are_addressed_under_the_area()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppsRoutes.Landing("acme").Format(), Is.EqualTo("/t/acme/apps"));
            Assert.That(AppsRoutes.App(null, "crm").Format(), Is.EqualTo("/apps/crm"));
            Assert.That(AppsRoutes.Window(null, "crm").Format(), Is.EqualTo("/apps/crm/window"));
            Assert.That(AppsRoutes.Window("acme", "crm").Format(), Is.EqualTo("/t/acme/apps/crm/window"));
        });
    }

    [TestCase("/apps/crm/window", true)]
    [TestCase("/apps/crm/window/board/7", true)]
    [TestCase("/t/acme/apps/crm/window", true)]
    [TestCase("/apps/crm/open", false)]
    [TestCase("/apps/crm", false)]
    [TestCase("/apps/window", false)]
    [TestCase("/apps", false)]
    [TestCase("/apps/catalogue/window", false)]
    [TestCase("/apps/catalogue/window/crm", false)]
    [TestCase("/data/crm/window", false)]
    public void Only_an_apps_window_address_is_a_window(string address, bool expected)
    {
        Assert.That(AppsRoutes.IsWindow(ExplorerAddress.Parse(address)), Is.EqualTo(expected));
    }

    [Test]
    public void IsWindow_rejects_a_null_address()
    {
        Assert.That(() => AppsRoutes.IsWindow(null!), Throws.ArgumentNullException);
    }
}
