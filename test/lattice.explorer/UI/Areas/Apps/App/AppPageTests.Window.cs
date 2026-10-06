using Bunit;
using Bunit.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Framing;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The app's window, <c>/apps/{slug}/window</c>: the app's UI never runs inside the console.
/// The overview's "Open" control launches the window in a new browser window; the window
/// renders only the app's sandboxed frame, is reachable by its address directly, and keeps
/// the frame's in-app path and the address in step in both directions - a deep link reopens
/// the app where it was (<c>nav.changed</c>), and the app's own navigation becomes the
/// address (<c>nav.sync</c>). An old <c>/apps/{slug}/open</c> address is redirected to it.
/// The frame is a stub host here; the real frame is X1's and is tested there.
/// </summary>
public sealed partial class AppPageTests
{
    [Test]
    public void The_overview_opens_the_app_in_a_new_window_that_shares_nothing_with_the_console()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/overview");
        var open = cut.Find("a[data-lt-open-app]");

        Assert.Multiple(() =>
        {
            Assert.That(open.GetAttribute("href"), Is.EqualTo("apps/crm/window"));
            Assert.That(open.GetAttribute("target"), Is.EqualTo("_blank"));
            Assert.That(open.GetAttribute("rel")!.Split(' '), Is.EquivalentTo(new[] { "noopener", "noreferrer" }));
            Assert.That(open.TextContent, Is.EqualTo("Open CRM (opens in a new window)"));
            Assert.That(cut.FindComponents<Stub<AppFrame>>(), Is.Empty, "the console never hosts the frame itself");
        });
    }

    [Test]
    public void The_window_renders_only_the_frame_for_this_app_with_a_way_back_to_its_overview()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/window");
        var frame = Frame(cut);

        Assert.Multiple(() =>
        {
            Assert.That(frame.Get(parameter => parameter.AppSlug), Is.EqualTo(Slug));
            Assert.That(frame.Get(parameter => parameter.Path), Is.Null, "a bare window address starts the app at its own start");
            Assert.That(frame.Get(parameter => parameter.LeaveHref), Is.EqualTo("apps/crm/overview"));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
            Assert.That(cut.FindAll(".lt-app-head"), Is.Empty);
            Assert.That(cut.Find("h1").ClassList, Does.Contain("lt-visually-hidden"));
            Assert.That(cut.Find(".lt-app-window"), Is.Not.Null);
        });
    }

    [Test]
    public void A_deep_link_hands_the_frame_its_in_app_path_and_query()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/window/board/42?query=view%3Dall");

        Assert.That(Frame(cut).Get(parameter => parameter.Path), Is.EqualTo("/board/42?view=all"));
    }

    [Test]
    public void The_frames_first_report_replaces_the_bare_window_address_and_later_ones_add_history()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/window");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        Sync(cut, "/board/7?filter=mine#top");

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "apps/crm/window/board/7?query=filter%3Dmine"));
            Assert.That(navigation.History.First().Options.ReplaceHistoryEntry, Is.True);
        });

        cut.Render();
        Assert.That(Frame(cut).Get(parameter => parameter.Path), Is.EqualTo("/board/7?filter=mine"), "the frame is handed back what it reported, so nothing echoes");

        Sync(cut, "/board/8");

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "apps/crm/window/board/8"));
            Assert.That(navigation.History.First().Options.ReplaceHistoryEntry, Is.False);
        });
    }

    [Test]
    public void A_deep_in_app_path_is_kept_in_the_address_query_and_handed_back_to_the_frame()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/window/board");

        Sync(cut, "/board/1/cards/2/comments");
        cut.Render();

        Assert.That(Frame(cut).Get(parameter => parameter.Path), Is.EqualTo("/board/1/cards/2/comments"));
    }

    [Test]
    public void A_report_of_where_the_address_already_is_does_not_navigate()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/window/board/7");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();
        var before = navigation.History.Count;

        Sync(cut, "/board/7");
        Sync(cut, "//board//7/");

        Assert.That(navigation.History, Has.Count.EqualTo(before));
    }

    [Test]
    public void The_start_of_the_app_reported_at_the_bare_window_address_does_not_navigate()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/window");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();
        var before = navigation.History.Count;

        Sync(cut, "/");

        Assert.That(navigation.History, Has.Count.EqualTo(before));
    }

    [Test]
    public void With_tenancy_on_the_window_and_its_in_app_path_stay_under_the_tenant()
    {
        UseTenancy("acme");
        Workspace.Grant(Workspace());
        var cut = RenderAt("t/acme/apps/crm/window");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        Sync(cut, "/board/1");

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "t/acme/apps/crm/window/board/1"));
            Assert.That(Frame(cut).Get(parameter => parameter.LeaveHref), Is.EqualTo("t/acme/apps/crm/overview"));
        });
    }

    [TestCase("apps/crm/open", "apps/crm/window")]
    [TestCase("apps/crm/open/board/42", "apps/crm/window/board/42")]
    [TestCase("apps/crm/open/board?query=view%3Dall", "apps/crm/window/board?query=view%3Dall")]
    public void An_old_open_address_is_redirected_in_place_to_the_same_path_in_the_window(string address, string window)
    {
        Workspace.Grant(Workspace());
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        RenderAt(address);

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + window));
            Assert.That(navigation.History.First().Options.ReplaceHistoryEntry, Is.True, "Back does not return to the old address");
        });
    }

    [Test]
    public void With_tenancy_on_an_old_open_address_is_redirected_under_its_tenant()
    {
        UseTenancy("acme");
        Workspace.Grant(Workspace());
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        RenderAt("t/acme/apps/crm/open/board");

        Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "t/acme/apps/crm/window/board"));
    }

    [Test]
    public void An_app_the_caller_holds_a_role_in_but_that_is_not_listed_as_theirs_cannot_be_opened()
    {
        // Described (the caller holds a role) but absent from ListMyAppsAsync: opening the
        // app follows the list, never the description.
        Workspace.Grant(Workspace());
        Workspace.Apps.Clear();

        var overview = RenderAt("apps/crm/overview");
        var window = RenderAt("apps/crm/window");

        Assert.Multiple(() =>
        {
            Assert.That(overview.FindAll("[data-lt-open-app]"), Is.Empty);
            Assert.That(window.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
            Assert.That(window.FindComponents<Stub<AppFrame>>(), Is.Empty);
        });
    }

    private static CapturedParameterView<AppFrame> Frame(IRenderedComponent<Orleans.Lattice.Explorer.UI.Areas.Apps.App.AppPage> cut) =>
        cut.FindComponent<Stub<AppFrame>>().Instance.Parameters;

    private static void Sync(IRenderedComponent<Orleans.Lattice.Explorer.UI.Areas.Apps.App.AppPage> cut, string path)
    {
        var callback = Frame(cut).Get(parameter => parameter.OnNavSync);
        cut.InvokeAsync(() => callback.InvokeAsync(path)).GetAwaiter().GetResult();
    }
}
