using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// "Your apps" (<c>/apps</c>): every signed-in user's own apps, the Catalogue view
/// and install control only for an <c>AppInstall</c> holder, failed activations
/// called out, tenancy on and off, and the compact form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class AppsPageTests : AppsTestContext
{
    [Test]
    public void A_restricted_identity_sees_its_apps_and_no_catalogue_and_no_error()
    {
        Restrict();
        Workspace.Apps.Add(AppsTestData.Mine("crm", hasUi: true, name: "CRM", "viewer", "editor"));
        Workspace.Apps.Add(AppsTestData.Mine("notes", hasUi: false));

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(2)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Apps"));
            Assert.That(cut.FindAll(".lt-apps-tabs__link").Select(link => link.TextContent), Is.EqualTo(new[] { "Your apps" }));
            Assert.That(cut.Find(".lt-apps-tabs__link").GetAttribute("aria-current"), Is.EqualTo("page"));
            Assert.That(cut.FindAll($"[data-lt-command='{AppsArea.InstallCommandId}']"), Is.Empty);
            Assert.That(cut.FindAll("[role=alert]"), Is.Empty);
            Assert.That(cut.Find("tbody tr").TextContent, Does.Contain("CRM").And.Contain("viewer, editor"));
            Assert.That(cut.FindAll("a[href='apps/crm/open']"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("a[href='apps/notes/open']"), Is.Empty, "an app without a UI has no Open control");
            Assert.That(cut.FindAll("a[href='apps/notes']"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("link[rel=stylesheet]").GetAttribute("href"), Is.EqualTo(AppsCatalogueAssets.Stylesheet));
        });
    }

    [Test]
    public void With_no_app_it_says_so_plainly()
    {
        Restrict();

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("No apps yet")));
    }

    [Test]
    public void Before_the_probe_answers_it_shows_a_skeleton()
    {
        Workspace.ListGate = new TaskCompletionSource();

        var cut = RenderAt<AppsPage>("/apps");

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));
        cut.InvokeAsync(() => Workspace.ListGate.SetResult());
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Is.Empty));
    }

    [Test]
    public void An_app_install_holder_gets_the_catalogue_view_and_the_install_control()
    {
        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-apps-tabs__link"), Has.Count.EqualTo(2)));
        var install = cut.Find($"[data-lt-command='{AppsArea.InstallCommandId}']");
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-tabs__link")[1].GetAttribute("href"), Is.EqualTo("apps/catalogue?source=all&filter=all"));
            Assert.That(install.GetAttribute("href"), Is.EqualTo("apps/catalogue?source=all&filter=available"));
            Assert.That(install.TextContent, Is.EqualTo("Install app..."));
        });

        var area = Services.GetServices<IExplorerArea>().OfType<AppsArea>().Single();
        ExplorerCommandControls.AssertVisibleControl(cut, area.Commands.Single(command => command.Id == AppsArea.InstallCommandId));
    }

    [Test]
    public void A_failed_activation_is_called_out_with_its_reconsent_action()
    {
        Control.Install(AppsTestData.TaskBoard(), AppLifecycleState.Failed);

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-apps-attention"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-apps-attention .lt-pill").GetAttribute("data-lt-state"), Is.EqualTo(LtStateRoles.Key(LtStateRole.Failed)));
            Assert.That(cut.Find(".lt-apps-attention a").GetAttribute("href"), Is.EqualTo("apps/catalogue/in-image/task-board%401.0.0"));
            Assert.That(cut.Find(".lt-apps-attention a").TextContent, Is.EqualTo("Review and re-consent"));
        });
    }

    [Test]
    public void With_tenancy_on_the_page_names_the_tenant_and_roots_every_link()
    {
        UseTenancy("acme");
        Workspace.Apps.Add(AppsTestData.Mine("crm"));

        var cut = RenderAt<AppsPage>("/t/acme/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("in tenant acme"));
            Assert.That(cut.FindAll("a").Select(link => link.GetAttribute("href")).Where(href => href is not null), Is.All.StartsWith("t/acme/apps"));
        });
    }

    [Test]
    public void With_tenancy_off_no_tenant_is_named()
    {
        Workspace.Apps.Add(AppsTestData.Mine("crm"));

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Not.Contain("tenant acme"));
    }

    [Test]
    public void Hostile_presentation_text_renders_literally_and_icons_are_images()
    {
        Restrict();
        var hostile = AppsTestData.Mine("crm") with
        {
            Presentation = new AppPresentationDescriptor
            {
                DisplayName = "<img src=x onerror=alert(1)>",
                Summary = "<script>alert(2)</script>",
                Icon = new AppIconDescriptor { Path = "i.svg", Sha256 = AppsTestData.Digest() },
            },
        };
        Workspace.Apps.Add(hostile);
        Workspace.Icons["crm"] = AppsTestData.Icon;

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.FindAll("img.lt-apps-icon"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-apps-app__name").TextContent, Is.EqualTo("<img src=x onerror=alert(1)>"));
            Assert.That(cut.Find(".lt-apps-app__summary").TextContent, Is.EqualTo("<script>alert(2)</script>"));
            Assert.That(cut.FindAll("script"), Is.Empty);
            Assert.That(cut.FindAll("img").Select(image => image.GetAttribute("src")), Is.All.StartsWith("data:image/svg+xml;base64,"));
            Assert.That(cut.FindAll("svg"), Is.Empty, "an icon is never inlined");
        });
    }

    [Test]
    public void Below_768px_each_app_is_a_two_line_row_that_opens_a_detail_sheet_with_its_actions()
    {
        Restrict();
        Workspace.Apps.Add(AppsTestData.Mine("crm", hasUi: true, name: "CRM", "viewer"));

        var cut = RenderAt<AppsPage>("/apps", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(1)));
        Assert.That(cut.Find(".lt-compact-row").TextContent, Does.Contain("CRM").And.Contain("v1.0.0 - viewer"));

        cut.Find(".lt-table-list__open").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog]").TextContent, Does.Contain("Open").And.Contain("Details")));
    }

    [Test]
    public void When_the_workspace_cannot_be_read_an_app_install_holder_is_told_so()
    {
        Workspace.Failure = new TimeoutException();

        var cut = RenderAt<AppsPage>("/apps");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-hint").TextContent, Does.Contain("could not be read")));
    }
}
