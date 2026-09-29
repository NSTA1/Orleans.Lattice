using Bunit;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The pre-install review (<c>/apps/catalogue/{source}/{slug}[@version]</c>), the
/// signature moment: identity and provenance, what the app creates under
/// <c>a/{slug}/</c>, what it asks for with every exception marked, its UI's bridge
/// operations in plain language, and its integrations - all as text only.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed partial class AppReviewPageTests : AppsTestContext
{
    private const string Review = "/apps/catalogue/in-image/task-board";

    [Test]
    public void The_review_shows_identity_and_provenance()
    {
        Offer(AppsTestData.TaskBoard());

        var cut = RenderReady(Review);

        var identity = cut.Find("section[aria-labelledby=lt-apps-identity]");
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Task board"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("v1.0.0 from In-image apps").And.Contain("nothing is installed until you approve it"));
            Assert.That(identity.TextContent, Does.Contain("task-board").And.Contain("1.0.0").And.Contain("In-image apps (in-image, Static)").And.Contain("Contoso").And.Contain("pkg:task-board"));
            Assert.That(identity.TextContent, Does.Contain("Shipped in the silo image"));
            Assert.That(identity.QuerySelector(".lt-apps-description")!.TextContent, Is.EqualTo("Track work.\nMove cards between columns."));
            Assert.That(cut.FindAll(".lt-apps-steps__step").Select(step => step.TextContent.Trim()), Is.EqualTo(new[] { "Review", "Bind roles", "Confirm ceiling", "Install", "Enable" }));
            Assert.That(cut.Find(".lt-apps-steps__step[aria-current]").TextContent.Trim(), Is.EqualTo("Review"));
        });
    }

    [Test]
    public void What_it_creates_is_drawn_as_a_chain_under_its_namespace_with_adopted_trees_as_exceptions()
    {
        Offer(AppsTestData.TaskBoard(adopted: true));

        var cut = RenderReady(Review);

        var creates = cut.Find("section[aria-labelledby=lt-apps-creates]");
        Assert.Multiple(() =>
        {
            Assert.That(creates.QuerySelectorAll(".lt-apps-chain__path").Select(path => path.TextContent), Is.EqualTo(new[] { "a/task-board/", "a/task-board/tasks" }));
            Assert.That(creates.TextContent, Does.Contain("kept 7 days after uninstall"));
            Assert.That(creates.TextContent, Does.Contain("Adopted trees - exceptions outside a/task-board/"));
            Assert.That(creates.TextContent, Does.Contain("legacy-archive").And.Contain("needs an approved scope, and survives uninstall"));
        });
    }

    [Test]
    public void What_it_asks_for_marks_anything_outside_its_namespace()
    {
        Offer(AppsTestData.TaskBoard(crossApp: true, adopted: true));

        var cut = RenderReady(Review);

        var asks = cut.Find("section[aria-labelledby=lt-apps-asks]");
        Assert.Multiple(() =>
        {
            Assert.That(asks.TextContent, Does.Contain("The capability ceiling it needs: read, write, delete and range read."));
            Assert.That(asks.QuerySelectorAll("tbody tr").Select(row => row.QuerySelector("th")!.TextContent), Is.EqualTo(new[] { "viewer", "editor" }));
            Assert.That(asks.TextContent, Does.Contain("a/crm/contacts, keys starting \"eu-\"").And.Contain("outside a/task-board/"));
            Assert.That(asks.QuerySelectorAll("h3 + ul li").Select(item => item.TextContent), Has.Some.Contains("legacy-archive (adopted)"));
        });
    }

    [Test]
    public void The_ui_and_its_bridge_operations_read_in_plain_language()
    {
        Offer(AppsTestData.TaskBoard(bridge: [new AppUiBridgeGrantDescriptor { Operation = "data.read" }, new AppUiBridgeGrantDescriptor { Operation = "context.user" }, new AppUiBridgeGrantDescriptor { Operation = "net.fetch" }]));

        var cut = RenderReady(Review);

        var ui = cut.Find("section[aria-labelledby=lt-apps-ui]");
        Assert.Multiple(() =>
        {
            Assert.That(ui.TextContent, Does.Contain("It ships a UI that runs in a sandboxed frame"));
            var items = ui.QuerySelectorAll("li").Select(item => item.TextContent).ToArray();
            Assert.That(items, Has.Length.EqualTo(3));
            Assert.That(items[0], Does.Contain("read its own trees"));
            Assert.That(items[1], Does.Contain("see your display name"));
            Assert.That(items[2], Does.Contain("use the unrecognised operation \"net.fetch\"").And.Contain("not recognised by this Explorer"));
            Assert.That(items.Take(2), Has.None.Contains("not recognised"));
        });
    }

    [Test]
    public void An_app_without_a_ui_says_so()
    {
        Offer(AppsTestData.TaskBoard() with { Ui = null });

        var cut = RenderReady(Review);

        Assert.That(cut.Find("section[aria-labelledby=lt-apps-ui]").TextContent, Does.Contain("It ships no UI"));
    }

    [Test]
    public void Replication_schema_change_feeds_and_tools_are_listed_with_other_apps_flagged()
    {
        Offer(AppsTestData.TaskBoard(crossApp: true));

        var cut = RenderReady(Review);

        var integration = System.Text.RegularExpressions.Regex.Replace(cut.Find("section[aria-labelledby=lt-apps-integration]").TextContent, @"\s+", " ");
        Assert.Multiple(() =>
        {
            Assert.That(integration, Does.Contain("a/task-board/tasks as LwwRegister"));
            Assert.That(integration, Does.Contain("task v2, strict ingest"));
            Assert.That(integration, Does.Contain("contacts-feed observes a/crm/contacts").And.Contain("another app: crm"));
            Assert.That(integration, Does.Contain("task-board_list_tasks").And.Contain("Lists tasks"));
        });
    }

    [Test]
    public void Hostile_presentation_text_renders_literally()
    {
        Offer(AppsTestData.TaskBoard(presentation: new AppPresentationDescriptor
        {
            DisplayName = "<h2>Board</h2>",
            Description = "<script>alert(document.cookie)</script><a href=\"javascript:x\">click</a>",
            PublisherDisplayName = "<i>Evil</i>",
            DocumentationUrl = "javascript:alert(1)",
        }));

        var cut = RenderReady(Review);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("<h2>Board</h2>"));
            Assert.That(cut.Find(".lt-apps-description").TextContent, Is.EqualTo("<script>alert(document.cookie)</script><a href=\"javascript:x\">click</a>"));
            Assert.That(cut.FindAll("script, i"), Is.Empty);
            Assert.That(cut.FindAll("a").Select(link => link.GetAttribute("href")), Has.None.StartsWith("javascript"));
            Assert.That(cut.Markup, Does.Contain("&lt;script&gt;"));
        });
    }

    [Test]
    public void A_verified_icon_is_drawn_through_an_image_data_url()
    {
        Offer(AppsTestData.TaskBoard(withIcon: true));
        Catalog.Icons[("in-image", "task-board")] = AppsTestData.Icon;

        var cut = RenderReady(Review);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("img.lt-apps-icon--large").GetAttribute("src"), Does.StartWith("data:image/svg+xml;base64,"));
            Assert.That(cut.FindAll("svg:not(.lt-mark)"), Is.Empty);
        });
    }

    [Test]
    public void An_app_the_source_does_not_offer_is_not_offered()
    {
        Catalog.Sources.Add(AppsTestData.InImage);

        var cut = RenderAt<AppReviewPage>("/apps/catalogue/in-image/nope%402.0.0");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Not offered")));
        Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain("In-image apps does not offer nope v2.0.0."));
    }

    [Test]
    public void A_restricted_identity_gets_not_found_and_nothing_is_described()
    {
        Restrict();
        Offer(AppsTestData.TaskBoard());

        var cut = RenderAt<AppReviewPage>(Review);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("h1, section, [role=alert]"), Is.Empty);
            Assert.That(Catalog.Describes, Is.Empty);
        });
    }

    [Test]
    public void A_malformed_review_address_is_not_found()
    {
        Offer(AppsTestData.TaskBoard());

        var cut = RenderAt<AppReviewPage>("/apps/catalogue/in-image/%40");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("h1"), Is.Empty);
            Assert.That(Catalog.Describes, Is.Empty);
        });
    }

    [Test]
    public void With_tenancy_on_the_review_names_the_tenant_and_roots_its_links()
    {
        UseTenancy("acme");
        Offer(AppsTestData.TaskBoard());

        var cut = RenderReady("/t/acme/apps/catalogue/in-image/task-board");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("in tenant acme"));
            Assert.That(cut.FindAll("a.lt-apps-link").Select(link => link.GetAttribute("href")), Is.All.StartsWith("t/acme/apps"));
        });
    }

    [Test]
    public void Below_768px_the_roles_become_two_line_rows()
    {
        Offer(AppsTestData.TaskBoard());

        var cut = RenderReady(Review, LtBreakpoint.Compact);

        Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-asks] .lt-table-list__row"), Has.Count.EqualTo(2));
    }

    private void Offer(AppDescriptor app, AppSourceSummary? source = null)
    {
        source ??= AppsTestData.InImage;
        if (!Catalog.Sources.Contains(source))
        {
            Catalog.Sources.Add(source);
        }

        Catalog.Descriptions[(source.Key, app.Slug, app.Version)] = app with { SourceKey = source.Key };
        Catalog.Offers.Add(AppsTestData.Offer(app, source.Key));
    }

    private IRenderedComponent<AppReviewPage> RenderReady(string address, LtBreakpoint breakpoint = LtBreakpoint.Expanded)
    {
        var cut = RenderAt<AppReviewPage>(address, breakpoint);
        cut.WaitUntil(() => Assert.That(cut.FindAll("section[aria-labelledby=lt-apps-identity], .lt-apps-status, .lt-apps-error"), Is.Not.Empty));
        return cut;
    }
}
