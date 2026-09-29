using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The directory spine: Home then one stop per shown area in directory order,
/// the current stop as the marker node with <c>aria-current</c>, an unavailable
/// area demoted with its reason, badges, and the rail form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DirectorySpineTests : ShellChromeTestContext
{
    private static readonly ExplorerAreaEntry Data =
        new(new FakeArea("data", "Data", 1), AreaAvailability.Visible, "1,204");

    private static readonly ExplorerAreaEntry Backups =
        new(new FakeArea("backups", "Backups", 2), AreaAvailability.Unavailable("Sign in to see backups."));

    [Test]
    public void Home_heads_the_spine_and_the_stops_follow_in_order()
    {
        var cut = RenderSpine(Location("/"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-spine__link").Select(link => link.GetAttribute("data-lt-command")),
                Is.EqualTo(new[] { "go.home", "go.data", "go.backups" }));
            Assert.That(cut.Find("nav").GetAttribute("aria-labelledby"), Is.EqualTo(cut.Find("h2").Id));
            Assert.That(cut.Find("h2").TextContent, Is.EqualTo("Estate"));
        });
    }

    [Test]
    public void The_current_stop_is_the_marker_node_with_aria_current_and_the_others_are_hollow()
    {
        var cut = RenderSpine(Location("/data/a/crm"));

        var current = cut.FindAll("[aria-current='page']").Single();
        Assert.Multiple(() =>
        {
            Assert.That(current.GetAttribute("data-lt-command"), Is.EqualTo("go.data"));
            Assert.That(current.QuerySelector(".lt-node--join"), Is.Not.Null);
            Assert.That(cut.FindAll(".lt-node--join"), Has.Count.EqualTo(1), "one marker on the spine");
            Assert.That(cut.Find("[data-lt-command='go.home'] .lt-node").ClassList, Does.Contain("lt-node--hollow"));
        });
    }

    [Test]
    public void At_home_the_home_stop_is_current()
    {
        var cut = RenderSpine(Location("/"));

        Assert.That(cut.Find("[aria-current='page']").GetAttribute("data-lt-command"), Is.EqualTo("go.home"));
    }

    [Test]
    public void An_unavailable_area_is_demoted_with_its_reason_and_a_badge_is_shown()
    {
        var cut = RenderSpine(Location("/"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-command='go.backups']").ClassList, Does.Contain("lt-shell-directory__link--unavailable"));
            Assert.That(cut.Find("[data-lt-command='go.backups'] .lt-shell-directory__reason").TextContent, Is.EqualTo("Sign in to see backups."));
            Assert.That(cut.Find("[data-lt-command='go.data'] .lt-shell-directory__badge").TextContent, Is.EqualTo("1,204"));
            Assert.That(cut.Find("[data-lt-command='go.data']").GetAttribute("href"), Is.EqualTo("data"));
            Assert.That(cut.Find("[data-lt-command='go.home']").GetAttribute("href"), Is.EqualTo("./"));
        });
    }

    [Test]
    public void As_a_rail_the_labels_stay_and_badges_and_reasons_go()
    {
        var cut = RenderSpine(Location("/"), rail: true);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("nav").ClassList, Does.Contain("lt-shell-directory__nav--rail"));
            Assert.That(cut.FindAll(".lt-shell-directory__badge"), Is.Empty);
            Assert.That(cut.FindAll(".lt-shell-directory__reason"), Is.Empty);
            Assert.That(cut.Find("[data-lt-command='go.backups']").TextContent, Does.Contain("Backups").And.Contain("unavailable"));
        });
    }

    [Test]
    public void While_nothing_is_known_a_skeleton_stands_in()
    {
        var cut = RenderSpine(ExplorerLocation.Initial);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-spine__link"), Has.Count.EqualTo(1), "Home is always there");
        });
    }

    [Test]
    public void With_tenancy_on_every_stop_is_rooted_at_the_tenant()
    {
        UseTenancy("acme");

        var cut = RenderSpine(Location("/t/acme/data"));

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-command='go.home']").GetAttribute("href"), Is.EqualTo("t/acme"));
            Assert.That(cut.Find("[data-lt-command='go.data']").GetAttribute("href"), Is.EqualTo("t/acme/data"));
        });
    }

    [Test]
    public void Following_a_stop_raises_OnNavigate()
    {
        var followed = 0;
        var cut = Render<DirectorySpine>(parameters => parameters
            .AddCascadingValue(Location("/"))
            .Add(spine => spine.OnNavigate, () => followed++));

        cut.Find("[data-lt-command='go.data']").Click();

        Assert.That(followed, Is.EqualTo(1));
    }

    [Test]
    public void Every_go_command_has_its_spine_stop()
    {
        var location = Location("/");
        var cut = RenderSpine(location);

        foreach (var command in ChromeCommands.Build(location, Services.GetRequiredService<Orleans.Lattice.Explorer.UI.Layout.Appearance.ShellAppearance>())
            .Where(command => command.Id.StartsWith("go.", StringComparison.Ordinal)))
        {
            ExplorerCommandControls.AssertVisibleControl(cut, command);
        }
    }

    private static ExplorerLocation Location(string address) =>
        new(ExplorerAddress.Parse(address), [Data, Backups], EntriesLoaded: true, TenancyActive: false);

    private IRenderedComponent<DirectorySpine> RenderSpine(ExplorerLocation location, bool rail = false) =>
        Render<DirectorySpine>(parameters => parameters.AddCascadingValue(location).Add(spine => spine.Rail, rail));
}
