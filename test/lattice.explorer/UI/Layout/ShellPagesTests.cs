using Bunit;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Pages;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// Home (the estate overview drawn as a spine), not found (naming the nearest
/// valid ancestor), and the page base every area page inherits.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellPagesTests : ShellChromeTestContext
{
    [Test]
    public void Home_lists_each_area_with_its_status_as_a_spine_never_as_cards()
    {
        var data = new FakeArea("data", "Data", 1) { HomeStatus = _ => ValueTask.FromResult<string?>("12 trees, 2 with dead letters") };
        var apps = new FakeArea("apps", "Apps", 2);
        var backups = new FakeArea("backups", "Backups", 3);
        AddArea(data);

        var cut = Render<HomePage>(parameters => parameters.AddCascadingValue(new ExplorerLocation(
            ExplorerAddress.Home,
            [new(data, AreaAvailability.Visible), new(apps, AreaAvailability.Visible), new(backups, AreaAvailability.Unavailable("Sign in to see backups."))],
            EntriesLoaded: true,
            TenancyActive: false)));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Estate"));
            Assert.That(cut.Find("ul").ClassList, Does.Contain("lt-spine"));
            Assert.That(cut.FindAll(".lt-shell-estate__name").Select(name => name.TextContent), Is.EqualTo(new[] { "Data", "Apps", "Backups" }));
            Assert.That(cut.FindAll(".lt-shell-estate__status").Select(status => status.TextContent),
                Is.EqualTo(new[] { "12 trees, 2 with dead letters", "Sign in to see backups." }));
            Assert.That(cut.FindAll(".lt-shell-estate__link").Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "data", "apps", "backups" }));
            Assert.That(cut.Markup, Does.Not.Contain("card"));
        });
    }

    [Test]
    public void Home_statuses_arrive_independently_and_a_slow_one_is_dropped_at_its_timeout()
    {
        var slow = new TaskCompletionSource<string?>();
        var data = new FakeArea("data", "Data", 1) { HomeStatus = _ => new ValueTask<string?>(slow.Task) };
        var apps = new FakeArea("apps", "Apps", 2) { HomeStatus = _ => ValueTask.FromResult<string?>("4 installed") };

        var cut = Render<HomePage>(parameters => parameters.AddCascadingValue(new ExplorerLocation(
            ExplorerAddress.Home,
            [new(data, AreaAvailability.Visible), new(apps, AreaAvailability.Visible)],
            EntriesLoaded: true,
            TenancyActive: false)));

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-shell-estate__status").Select(status => status.TextContent), Is.EqualTo(new[] { "4 installed" })));

        cut.InvokeAsync(() => Time.Advance(new ExplorerChromeOptions().HomeStatusTimeout));

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-shell-estate__status"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void Home_before_the_directory_answers_shows_a_skeleton_and_with_no_area_says_so()
    {
        var loading = Render<HomePage>(parameters => parameters.AddCascadingValue(ExplorerLocation.Initial));
        var empty = Render<HomePage>(parameters => parameters.AddCascadingValue(
            new ExplorerLocation(ExplorerAddress.Home, [], EntriesLoaded: true, TenancyActive: false)));

        Assert.Multiple(() =>
        {
            Assert.That(loading.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
            Assert.That(empty.Find(".lt-empty h2").TextContent, Is.EqualTo("Nothing to show yet"));
        });
    }

    [Test]
    public void Home_links_are_rooted_at_the_tenant_when_tenancy_is_on()
    {
        UseTenancy("acme");
        var data = new FakeArea("data", "Data");

        var cut = Render<HomePage>(parameters => parameters.AddCascadingValue(new ExplorerLocation(
            ExplorerAddress.Home.WithTenant("acme"),
            [new(data, AreaAvailability.Visible)],
            EntriesLoaded: true,
            TenancyActive: true)));

        Assert.That(cut.Find(".lt-shell-estate__link").GetAttribute("href"), Is.EqualTo("t/acme/data"));
    }

    [Test]
    public void Not_found_names_the_address_and_its_nearest_valid_ancestor()
    {
        AddArea(new FakeArea("data", "Data"));
        Navigation.NavigateTo("data/Missing?key=k");

        var cut = Render<NotFoundPage>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
            Assert.That(cut.Find("code").TextContent, Is.EqualTo("/data/%4Dissing?key=k"));
            Assert.That(cut.Find(".lt-shell-not-found__ancestor a").GetAttribute("href"), Is.EqualTo("data"));
            Assert.That(cut.Find(".lt-shell-not-found__ancestor a").TextContent, Is.EqualTo("/data"));
        });
    }

    [Test]
    public void Not_found_for_a_url_that_is_not_an_address_shows_it_raw_and_offers_home()
    {
        Navigation.NavigateTo("data/%ZZ");

        var cut = Render<NotFoundPage>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("code").TextContent, Is.EqualTo("/data/%ZZ"));
            Assert.That(cut.Find(".lt-shell-not-found__ancestor a").GetAttribute("href"), Is.EqualTo("./"));
        });
    }

    [Test]
    public void A_page_reads_its_address_from_the_layout_or_else_from_the_url()
    {
        Navigation.NavigateTo("data/orders");
        var cascaded = new ExplorerLocation(ExplorerAddress.Parse("/apps"), [], EntriesLoaded: true, TenancyActive: false);

        var fromLayout = Render<AddressProbePage>(parameters => parameters.AddCascadingValue(cascaded));
        var fromUrl = Render<AddressProbePage>();

        Assert.Multiple(() =>
        {
            Assert.That(fromLayout.Find("p").TextContent, Is.EqualTo("/apps"));
            Assert.That(fromUrl.Find("p").TextContent, Is.EqualTo("/data/orders"));
        });
    }

    [Test]
    public void The_route_parameters_bind_without_the_page_declaring_them()
    {
        var cut = Render<AddressProbePage>(parameters => parameters
            .Add(page => page.Tenant, "acme")
            .Add(page => page.P1, "a")
            .Add(page => page.P2, "b")
            .Add(page => page.P3, "c")
            .Add(page => page.P4, "d")
            .Add(page => page.P5, "e")
            .Add(page => page.P6, "f"));

        Assert.That(new[] { cut.Instance.Tenant, cut.Instance.P1, cut.Instance.P2, cut.Instance.P3, cut.Instance.P4, cut.Instance.P5, cut.Instance.P6 },
            Is.EqualTo(new[] { "acme", "a", "b", "c", "d", "e", "f" }));
    }

    /// <summary>An area page that shows the address it was given.</summary>
    public sealed class AddressProbePage : ExplorerPage
    {
        /// <inheritdoc />
        protected override void BuildRenderTree(Microsoft.AspNetCore.Components.Rendering.RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddContent(1, Address.Format());
            builder.CloseElement();
        }
    }
}
