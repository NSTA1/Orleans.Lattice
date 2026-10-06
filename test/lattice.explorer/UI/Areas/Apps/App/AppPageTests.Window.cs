using Bunit;
using Bunit.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Framing;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The window section, <c>/apps/{slug}/window</c>: the app's frame alone, for a browser
/// window of its own and reachable by its URL directly. It is offered exactly when the open
/// section is, renders nothing but the frame, keeps its in-app path in its own address, and
/// the open section links to it.
/// </summary>
public sealed partial class AppPageTests
{
    [Test]
    public void The_open_section_offers_the_app_in_a_new_window()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/open");

        Assert.That(Frame(cut).Get(parameter => parameter.WindowHref), Is.EqualTo("apps/crm/window"));
    }

    [Test]
    public void The_window_renders_only_the_frame_for_this_app()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/window");
        var frame = Frame(cut);

        Assert.Multiple(() =>
        {
            Assert.That(frame.Get(parameter => parameter.AppSlug), Is.EqualTo(Slug));
            Assert.That(frame.Get(parameter => parameter.Path), Is.Null);
            Assert.That(() => frame.Get(parameter => parameter.WindowHref), Throws.TypeOf<ParameterNotFoundException>(), "a window does not offer another window");
            Assert.That(frame.Get(parameter => parameter.LeaveHref), Is.EqualTo("apps/crm/overview"));
            Assert.That(cut.FindAll("[role=tab]"), Is.Empty);
            Assert.That(cut.FindAll(".lt-app-head"), Is.Empty);
            Assert.That(cut.Find("h1").ClassList, Does.Contain("lt-visually-hidden"));
            Assert.That(cut.Find(".lt-app-window"), Is.Not.Null);
        });
    }

    [Test]
    public void A_deep_link_to_a_window_hands_the_frame_its_in_app_path()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/window/board/42?query=view%3Dall");

        Assert.That(Frame(cut).Get(parameter => parameter.Path), Is.EqualTo("/board/42?view=all"));
    }

    [Test]
    public void The_apps_own_navigation_stays_in_the_window()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/window");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        Sync(cut, "/board/7");

        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "apps/crm/window/board/7"));
            Assert.That(navigation.History.First().Options.ReplaceHistoryEntry, Is.True);
        });
    }

    [Test]
    public void With_tenancy_on_the_window_stays_under_the_tenant()
    {
        UseTenancy("acme");
        Workspace.Grant(Workspace());
        var cut = RenderAt("t/acme/apps/crm/window");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        Sync(cut, "/board/1");

        Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "t/acme/apps/crm/window/board/1"));
    }

    [Test]
    public void An_app_without_a_ui_has_no_window()
    {
        Workspace.Grant(Workspace(ui: false));

        var cut = RenderAt("apps/crm/window");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
            Assert.That(cut.FindComponents<Stub<AppFrame>>(), Is.Empty);
        });
    }

    [Test]
    public void An_enabled_app_the_caller_cannot_open_yet_says_so_in_its_window()
    {
        Control.Administer(Admin(ui: true, state: AppLifecycleState.Enabled), CoveringConsent());

        var cut = RenderAt("apps/crm/window");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM is not open to you yet"));
            Assert.That(cut.FindComponents<Stub<AppFrame>>(), Is.Empty);
        });
    }
}
