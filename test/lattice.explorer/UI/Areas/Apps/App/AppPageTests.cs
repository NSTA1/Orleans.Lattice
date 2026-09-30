using Bunit;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Bunit.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.App;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App.AppPageTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The app page's gating and presentation (A2, issue #3819): who sees the page at all, which
/// sections they get, the overview, untrusted presentation text rendered literally, and the
/// loading, unavailable and not-found states.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class AppPageTests : AppPageTestContext
{
    private static readonly string[] RoleHolderTabs = ["Overview", "Trees", "Roles", "Tools", "Subscriptions", "Replication", "Open"];

    [Test]
    public void A_role_holder_sees_the_overview_and_every_section_but_consent()
    {
        Workspace.Grant(Workspace()).Icons[Slug] = Icon();

        var cut = RenderAt("apps/crm/overview");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Is.EqualTo("Accounts, orders and contact history"));
            Assert.That(TabLabels(cut), Is.EqualTo(RoleHolderTabs));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Overview"));
            Assert.That(Definition(cut, "Version"), Is.EqualTo("2.1.0"));
            Assert.That(Definition(cut, "Source"), Is.EqualTo("in-image"));
            Assert.That(Definition(cut, "State"), Is.EqualTo("Enabled"));
            Assert.That(Definition(cut, "Your roles"), Is.EqualTo("viewer"));
            Assert.That(Definition(cut, "User interface"), Is.EqualTo("This app ships a UI."));
            Assert.That(Definition(cut, "Publisher"), Is.EqualTo("Contoso"));
            Assert.That(Definition(cut, "Categories"), Is.EqualTo("sales, records"));
            Assert.That(cut.FindAll(".lt-dl__term").Select(term => term.TextContent), Does.Not.Contain("Provenance"));
            Assert.That(cut.Find(".lt-app-description").TextContent, Is.EqualTo("Keeps accounts and orders.\nOne line per fact."));
            Assert.That(cut.Find(".lt-app-head code").TextContent, Is.EqualTo("a/crm"));
            Assert.That(cut.Find("a.lt-btn").GetAttribute("href"), Is.EqualTo("apps/crm/open"));
            Assert.That(cut.Find("link[rel=stylesheet]").GetAttribute("href"), Is.EqualTo(AppPagesAssets.Stylesheet));
            Assert.That(Control.DescribeCalls, Is.Zero, "a caller the probe says lacks AppInstall is never described");
        });
    }

    [Test]
    public void The_icon_is_an_img_with_a_data_uri_and_never_inline_svg()
    {
        Workspace.Grant(Workspace()).Icons[Slug] = Icon();

        var cut = RenderAt("apps/crm/overview");
        var icon = cut.Find("img.lt-app-head__icon");

        Assert.Multiple(() =>
        {
            Assert.That(icon.GetAttribute("src"), Is.EqualTo("data:image/svg+xml;base64," + Convert.ToBase64String(Icon().Bytes.Span)));
            Assert.That(icon.GetAttribute("alt"), Is.Empty);
            Assert.That(cut.FindAll("svg"), Is.Empty);
        });
    }

    [Test]
    public void A_non_image_icon_is_not_shown_and_the_head_falls_back_to_the_apps_monogram()
    {
        Workspace.Grant(Workspace()).Icons[Slug] = Icon("text/html");

        var cut = RenderAt("apps/crm/overview");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("img"), Is.Empty);
            Assert.That(cut.FindAll(".lt-app-head .lt-node"), Is.Empty, "never an empty circle where the icon belongs");
            Assert.That(cut.Find(".lt-app-head .lt-app-head__monogram").TextContent, Is.EqualTo("cr"));
        });
    }

    [Test]
    public void An_operator_without_a_role_sees_the_installed_versions_icon()
    {
        Control.Administer(Admin(), CoveringConsent());
        Catalog.Icons[("in-image", Slug)] = Icon();

        var cut = RenderAt("apps/crm/overview");

        cut.WaitUntil(() => Assert.That(
            cut.Find("img.lt-app-head__icon").GetAttribute("src"),
            Is.EqualTo("data:image/svg+xml;base64," + Convert.ToBase64String(Icon().Bytes.Span))));
    }

    [Test]
    public void The_bare_app_address_shows_the_overview_where_it_is_and_never_navigates()
    {
        // Issue #4093: the page used to rename the bare address to .../overview with a
        // server-side replace once its load settled. When the browser had already moved on
        // but the server had not yet heard, that replace landed last and dragged the user back.
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm");
        var navigation = Services.GetRequiredService<BunitNavigationManager>();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM"));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent.Trim(), Is.EqualTo("Overview"));
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "apps/crm"));
            Assert.That(navigation.History.Where(entry => entry.Uri.Contains("overview", StringComparison.Ordinal)), Is.Empty, "the page issued no navigation of its own");
        });
    }

    [TestCase("apps/crm")]
    [TestCase("apps/crm/overview")]
    [TestCase("apps/crm/trees")]
    [TestCase("apps/crm/consent")]
    [TestCase("apps/crm/open")]
    [TestCase("apps/crm/open/board/1")]
    public void A_caller_with_neither_a_role_nor_app_install_sees_not_found_for_the_whole_subtree(string address)
    {
        // The app exists and the control would describe it - to a holder of AppInstall.
        Control.Descriptions[Slug] = Admin();

        var cut = RenderAt(address);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
            Assert.That(cut.Markup, Does.Not.Contain("CRM"));
            Assert.That(Control.DescribeCalls, Is.Zero);
        });
    }

    [Test]
    public void A_refused_administrative_read_fails_closed_to_not_found()
    {
        Control.Capabilities = new LatticeAppsCapabilities { CanDescribe = true, CanGetConsent = true };
        Control.DescribeThrows = new UnauthorizedAccessException("denied");

        var cut = RenderAt("apps/crm/overview");

        Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
    }

    [Test]
    public void An_app_that_is_not_installed_is_not_found_even_to_an_app_install_holder()
    {
        Control.Capabilities = new LatticeAppsCapabilities { CanDescribe = true, CanGetConsent = true };

        var cut = RenderAt("apps/ghost/overview");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
            Assert.That(Control.DescribeCalls, Is.EqualTo(1));
            Assert.That(Control.ConsentCalls, Is.Zero);
        });
    }

    [Test]
    public void An_app_install_holder_without_a_role_sees_the_administrative_sections_but_cannot_open_it()
    {
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/overview");

        Assert.Multiple(() =>
        {
            Assert.That(TabLabels(cut), Is.EqualTo(new[] { "Overview", "Trees", "Roles", "Tools", "Subscriptions", "Replication", "Consent" }));
            Assert.That(Definition(cut, "Your roles"), Is.EqualTo("You hold no role in this app."));
            Assert.That(Definition(cut, "Provenance"), Is.EqualTo("in-image / Contoso / Contoso.Crm/2.1.0"));
            Assert.That(cut.FindAll("a.lt-btn"), Is.Empty, "there is no Open control without a role");
            Assert.That(cut.FindAll("img"), Is.Empty, "the workspace icon needs a role");
            Assert.That(cut.Markup, Does.Not.Contain(AdoptedTreeId));
        });
    }

    [Test]
    public void A_role_holder_who_also_holds_app_install_sees_both_the_open_and_consent_sections()
    {
        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());

        var cut = RenderAt("apps/crm/overview");

        Assert.That(TabLabels(cut), Is.EqualTo(new[] { "Overview", "Trees", "Roles", "Tools", "Subscriptions", "Replication", "Consent", "Open" }));
    }

    [Test]
    public void An_app_without_a_ui_has_no_open_section_at_all()
    {
        Workspace.Grant(Workspace(ui: false));

        var overview = RenderAt("apps/crm/overview");

        Assert.Multiple(() =>
        {
            Assert.That(TabLabels(overview), Does.Not.Contain("Open"));
            Assert.That(Definition(overview, "User interface"), Is.EqualTo("This app ships no UI."));
            Assert.That(overview.FindAll("a.lt-btn"), Is.Empty);
        });

        var open = RenderAt("apps/crm/open");
        Assert.That(open.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
    }

    [TestCase("apps/crm/consent")]
    [TestCase("apps/crm/settings")]
    [TestCase("apps/crm/trees/orders")]
    public void A_section_the_caller_does_not_have_is_not_found(string address)
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt(address);

        Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Nothing lives at this address"));
    }

    [Test]
    public void Hostile_presentation_text_renders_literally_as_text()
    {
        var presentation = Presentation(description: Hostile, documentation: "javascript:steal()") with
        {
            DisplayName = Hostile,
            Summary = Hostile,
            PublisherDisplayName = Hostile,
            Categories = [Hostile],
        };
        Workspace.Grant(Workspace(presentation: presentation));

        var cut = RenderAt("apps/crm/overview");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo(Hostile));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Is.EqualTo(Hostile));
            Assert.That(cut.Find(".lt-app-description").TextContent, Is.EqualTo(Hostile));
            Assert.That(Definition(cut, "Publisher"), Is.EqualTo(Hostile));
            Assert.That(Definition(cut, "Documentation"), Is.EqualTo("javascript:steal()"));
            Assert.That(cut.FindAll("script"), Is.Empty);
            Assert.That(cut.FindAll("b"), Is.Empty);
            Assert.That(cut.FindAll("[onmouseover]"), Is.Empty);
            Assert.That(cut.FindAll("a[href^='javascript']"), Is.Empty);
        });
    }

    [Test]
    public void A_web_documentation_url_is_a_link_that_passes_no_referrer_or_opener()
    {
        Workspace.Grant(Workspace());

        var cut = RenderAt("apps/crm/overview");
        var link = cut.Find(".lt-dl a");

        Assert.Multiple(() =>
        {
            Assert.That(link.GetAttribute("href"), Is.EqualTo("https://example.test/crm"));
            Assert.That(link.GetAttribute("rel"), Is.EqualTo("noopener noreferrer nofollow"));
        });
    }

    [Test]
    public void The_page_shows_a_skeleton_until_the_workspace_answers()
    {
        Workspace.Grant(Workspace());
        Workspace.Gate = new TaskCompletionSource();

        var cut = RenderAt("apps/crm/overview");

        Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1));

        cut.InvokeAsync(() => Workspace.Gate.SetResult());

        cut.WaitForAssertion(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM")));
    }

    [Test]
    public void A_workspace_fault_with_no_other_answer_is_unavailable_and_can_be_retried()
    {
        Workspace.Grant(Workspace());
        Workspace.Throw = new InvalidOperationException("the channel is down");

        var cut = RenderAt("apps/crm/overview");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("This app could not be loaded"));
            Assert.That(cut.Find("[role=alert] h2").TextContent, Is.EqualTo("The cluster did not answer"));
            Assert.That(cut.Markup, Does.Not.Contain("the channel is down"), "raw exception text is never shown");
        });

        Workspace.Throw = null;
        cut.Find("[role=alert] button").Click();

        cut.WaitForAssertion(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("CRM")));
    }

    [Test]
    public void A_failing_capability_probe_hides_the_administrative_sections()
    {
        Workspace.Grant(Workspace());
        Control.Administer(Admin(), CoveringConsent());
        Control.ProbeThrows = new InvalidOperationException("probe down");

        var cut = RenderAt("apps/crm/overview");

        Assert.Multiple(() =>
        {
            Assert.That(TabLabels(cut), Is.EqualTo(RoleHolderTabs));
            Assert.That(Control.DescribeCalls, Is.Zero);
        });
    }

    [Test]
    public void Choosing_a_section_navigates_to_its_address()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/overview");

        cut.FindAll("[role=tab]").Single(tab => tab.TextContent == "Trees").Click();

        var navigation = Services.GetRequiredService<BunitNavigationManager>();
        Assert.Multiple(() =>
        {
            Assert.That(navigation.Uri, Is.EqualTo(navigation.BaseUri + "apps/crm/trees"));
            Assert.That(cut.Find("[role=tab][aria-selected=true]").TextContent, Is.EqualTo("Trees"));
            Assert.That(cut.Find("[role=tabpanel] h2").TextContent, Is.EqualTo("Trees"));
        });
    }

    [Test]
    public void With_tenancy_on_every_link_is_rooted_at_the_tenant()
    {
        UseTenancy("acme");
        Workspace.Grant(Workspace());

        var overview = RenderAt("t/acme/apps/crm/overview");
        var trees = RenderAt("t/acme/apps/crm/trees");

        Assert.Multiple(() =>
        {
            Assert.That(overview.Find("a.lt-btn").GetAttribute("href"), Is.EqualTo("t/acme/apps/crm/open"));
            Assert.That(trees.Find("tbody a").GetAttribute("href"), Is.EqualTo("t/acme/data/a/crm/orders"));
        });
    }

    [Test]
    public void Moving_to_another_app_loads_it_afresh()
    {
        Workspace.Grant(Workspace());
        Workspace.Grant(Workspace() with { Slug = "hr", Presentation = Presentation() with { DisplayName = "People" } });
        var cut = RenderAt("apps/crm/overview");

        Navigation.NavigateTo("apps/hr/overview");
        cut.Render();

        cut.WaitForAssertion(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("People")));
        Assert.That(Workspace.Described, Is.EqualTo(new[] { "crm", "hr" }));
    }

    [Test]
    public void Switching_sections_within_an_app_reads_it_again_and_keeps_the_page_on_screen()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/overview");
        Workspace.Gate = new TaskCompletionSource();

        Navigation.NavigateTo("apps/crm/roles");
        cut.Render();
        var whileReading = cut.Find("h1").TextContent;
        Workspace.Gate.SetResult();

        Assert.Multiple(() =>
        {
            Assert.That(whileReading, Is.EqualTo("CRM"), "the page stays while the section's read runs");
            Assert.That(Workspace.Described, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void A_move_within_the_open_apps_own_frame_reads_nothing()
    {
        Workspace.Grant(Workspace());
        var cut = RenderAt("apps/crm/open");

        Navigation.NavigateTo("apps/crm/open/board");
        cut.Render();

        Assert.That(Workspace.Described, Has.Count.EqualTo(1), "re-reading would tear the running frame down");
    }

    [Test]
    public void The_route_parameters_bind_without_the_page_reading_them()
    {
        var cut = Render<AppPage>(parameters => parameters.Add(page => page.P1, "crm").Add(page => page.P2, "open").Add(page => page.P3, "board"));

        Assert.That(new[] { cut.Instance.P1, cut.Instance.P2, cut.Instance.P3 }, Is.EqualTo(new[] { "crm", "open", "board" }));
    }

    private static string Definition(IRenderedComponent<AppPage> cut, string term) =>
        cut.FindAll(".lt-dl__row")
            .Single(row => row.QuerySelector(".lt-dl__term")?.TextContent == term)
            .QuerySelector("dd")!.TextContent.Trim();
}
