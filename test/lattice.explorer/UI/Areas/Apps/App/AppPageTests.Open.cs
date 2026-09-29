using Bunit;
using Bunit.TestDoubles;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Framing;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The open section: it hosts the app's sandboxed frame, and keeps the frame's in-app path
/// and the address in step in both directions - a deep link reopens the app where it was
/// (<c>nav.changed</c>), and the app's own navigation becomes the address (<c>nav.sync</c>).
/// The frame is a stub host here; the real frame is X1's and is tested there.
/// </summary>
public sealed partial class AppPageTests
{
    [Test]
    public void The_open_section_hosts_the_frame_for_this_app_with_a_way_back_to_its_overview()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/open");
        var frame = Frame(cut);

        Assert.Multiple(() =>
        {
            Assert.That(frame.Get(parameter => parameter.AppSlug), Is.EqualTo(Slug));
            Assert.That(frame.Get(parameter => parameter.Path), Is.Null, "a bare open address starts the app at its own start");
            Assert.That(frame.Get(parameter => parameter.LeaveHref), Is.EqualTo("apps/crm/overview"));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Open"));
        });
    }

    [Test]
    public void A_deep_link_hands_the_frame_its_in_app_path_and_query()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/open/board/42?query=view%3Dall");

        Assert.That(Frame(cut).Get(parameter => parameter.Path), Is.EqualTo("/board/42?view=all"));
    }

    [Test]
    public void The_frames_first_report_replaces_the_bare_open_address_and_later_ones_add_history()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/open");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        Sync(cut, "/board/7?filter=mine#top");

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "apps/crm/open/board/7?query=filter%3Dmine"));
            Assert.That(navigation.History.First().Options.ReplaceHistoryEntry, Is.True);
        });

        cut.Render();
        Assert.That(Frame(cut).Get(parameter => parameter.Path), Is.EqualTo("/board/7?filter=mine"), "the frame is handed back what it reported, so nothing echoes");

        Sync(cut, "/board/8");

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "apps/crm/open/board/8"));
            Assert.That(navigation.History.First().Options.ReplaceHistoryEntry, Is.False);
        });
    }

    [Test]
    public void A_deep_in_app_path_is_kept_in_the_address_query_and_handed_back_to_the_frame()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/open/board");

        Sync(cut, "/board/1/cards/2/comments");
        cut.Render();

        Assert.That(Frame(cut).Get(parameter => parameter.Path), Is.EqualTo("/board/1/cards/2/comments"));
    }

    [Test]
    public void A_report_of_where_the_address_already_is_does_not_navigate()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/open/board/7");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();
        var before = navigation.History.Count;

        Sync(cut, "/board/7");
        Sync(cut, "//board//7/");

        Assert.That(navigation.History, Has.Count.EqualTo(before));
    }

    [Test]
    public void The_start_of_the_app_reported_at_the_bare_open_address_does_not_navigate()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/open");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();
        var before = navigation.History.Count;

        Sync(cut, "/");

        Assert.That(navigation.History, Has.Count.EqualTo(before));
    }

    [Test]
    public void With_tenancy_on_the_in_app_path_stays_under_the_tenant()
    {
        UseTenancy("acme");
        Workspace.Grant(Workspace());
        var cut = RenderAt("t/acme/apps/crm/open");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        Sync(cut, "/board/1");

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "t/acme/apps/crm/open/board/1"));
            Assert.That(Frame(cut).Get(parameter => parameter.LeaveHref), Is.EqualTo("t/acme/apps/crm/overview"));
        });
    }

    [Test]
    public void An_app_the_caller_holds_a_role_in_but_that_is_not_listed_as_theirs_cannot_be_opened()
    {
        // Described (the caller holds a role) but absent from ListMyAppsAsync: the Open
        // section follows the list, never the description.
        Workspace.Grant(Workspace());
        Workspace.Apps.Clear();

        var cut = RenderAt("apps/crm/open");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
            Assert.That(cut.FindComponents<Stub<AppFrame>>(), Is.Empty);
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
