using Bunit;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The source catalogue: one source and many, the source selector and its search
/// gating, the filters as query-string state, one row per (source, slug) with the
/// ambiguity called out, state pills, paging, failure, denial, hostile text and
/// the compact form.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppsCataloguePageTests : AppsTestContext
{
    [Test]
    public void With_one_static_source_the_catalogue_reads_correctly_and_search_is_disabled()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image", "2.1.0", "2.1.0", AppLifecycleState.Enabled, name: "CRM"));
        Catalog.Offers.Add(AppsTestData.Offer("task-board", "in-image", name: "Task board"));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(2)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-tabs__link")[1].GetAttribute("aria-current"), Is.EqualTo("page"));
            Assert.That(cut.FindAll("select")[0].QuerySelectorAll("option").Select(option => option.TextContent),
                Is.EqualTo(new[] { "All sources", "In-image apps (Static)" }));
            Assert.That(cut.Find(".lt-toolbar").TextContent, Does.Contain("In-image apps: shipped with the cluster, one version of each app"));
            Assert.That(cut.Find(".lt-toolbar").TextContent, Does.Not.Contain("in-image: Static"));
            var search = cut.Find("input[type=search]");
            Assert.That(search.HasAttribute("disabled"), Is.True);
            Assert.That(search.GetAttribute("placeholder"), Is.EqualTo("Search is not available"));
            Assert.That(cut.Find("#" + search.GetAttribute("aria-describedby")).TextContent, Is.EqualTo("No configured source supports search."), "the reason is the field's hint");
            Assert.That(cut.FindAll("td .lt-apps-source").Select(cell => cell.TextContent), Is.EqualTo(new[] { "in-image", "in-image" }), "a source key is one unbroken token");
            Assert.That(cut.Find("p.lt-apps-hint").TextContent, Is.EqualTo("No configured source supports search."));
            Assert.That(cut.Find(".lt-apps-count").TextContent, Is.EqualTo("1 source, 2 apps"));
            Assert.That(cut.FindAll("thead th").Select(header => header.TextContent.Trim()), Is.EqualTo(new[] { "App", "Source", "Version", "State", "Actions" }));
            Assert.That(Catalog.Queries.Last().Text, Is.Null);
        });
    }

    [Test]
    public void With_several_sources_each_one_is_listed_with_its_kind_and_capabilities()
    {
        Catalog.Sources.AddRange([AppsTestData.InImage, AppsTestData.Feed, AppsTestData.Blob]);

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue?source=nuget-contoso&filter=all");

        cut.WaitUntil(() => Assert.That(cut.FindAll("select"), Is.Not.Empty));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("select")[0].QuerySelectorAll("option").Select(option => option.TextContent),
                Is.EqualTo(new[] { "All sources", "In-image apps (Static)", "Contoso feed (Dynamic)", "Ops blob store (Dynamic)" }));
            Assert.That(cut.Find(".lt-toolbar").TextContent, Does.Contain("Contoso feed: fetched from a live source; searchable, several versions of each app, downloaded and verified before review"));
            Assert.That(cut.Find("input[type=search]").HasAttribute("disabled"), Is.False);
            Assert.That(cut.Find("input[type=search]").HasAttribute("aria-describedby"), Is.False);
            Assert.That(Catalog.Queries.Last().SourceKey, Is.EqualTo("nuget-contoso"));
        });
    }

    [Test]
    public void The_text_filter_is_enabled_for_all_sources_when_any_can_search_and_is_sent()
    {
        Catalog.Sources.AddRange([AppsTestData.InImage, AppsTestData.Blob]);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "blob-ops"));
        Catalog.Offers.Add(AppsTestData.Offer("notes", "blob-ops"));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue?source=all&filter=all&q=crm");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(Catalog.Queries.Last().Text, Is.EqualTo("crm"));
            Assert.That(cut.Find("input[type=search]").GetAttribute("value"), Is.EqualTo("crm"));
        });
    }

    [Test]
    public void The_selectors_write_their_state_to_the_address()
    {
        Catalog.Sources.AddRange([AppsTestData.InImage, AppsTestData.Blob]);
        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-apps-filter button"), Has.Count.EqualTo(4)));

        cut.FindAll("select")[0].Change("blob-ops");
        Assert.That(Navigation.Uri, Does.EndWith("apps/catalogue?source=blob-ops&filter=all"));

        cut.FindAll(".lt-apps-filter button")[3].Click();
        Assert.That(Navigation.Uri, Does.EndWith("apps/catalogue?source=all&filter=updates"));

        var search = cut.Find("input[type=search]");
        search.Input("board");
        search.KeyDown("Enter");
        Assert.That(Navigation.Uri, Does.EndWith("apps/catalogue?source=all&filter=all&q=board"));
    }

    [Test]
    public void The_current_filter_is_pressed_and_filters_the_listing()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image", "2.0.0", "1.0.0", AppLifecycleState.Enabled));
        Catalog.Offers.Add(AppsTestData.Offer("notes", "in-image"));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue?source=all&filter=updates");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-filter button").Select(button => button.GetAttribute("aria-pressed")), Is.EqualTo(new[] { "false", "false", "false", "true" }));
            Assert.That(Catalog.Queries.Last().Filter, Is.EqualTo(AvailableAppFilter.Updates));
            Assert.That(cut.Find("tbody .lt-pill").TextContent, Is.EqualTo("Update available"));
            Assert.That(cut.Find("tbody a.lt-btn").TextContent, Is.EqualTo("Upgrade"));
            Assert.That(cut.Find("tbody a.lt-btn").GetAttribute("href"), Is.EqualTo("apps/catalogue/in-image/crm%402.0.0"));
        });
    }

    [Test]
    public void A_slug_offered_by_several_sources_is_one_row_per_source_with_the_ambiguity_called_out()
    {
        Catalog.Sources.AddRange([AppsTestData.Feed, AppsTestData.Blob]);
        Catalog.Offers.Add(AppsTestData.Offer("quality-gates", "nuget-contoso", "1.2.0", name: "Quality gates"));
        Catalog.Offers.Add(AppsTestData.Offer("quality-gates", "blob-ops", "1.1.4", name: "Quality gates"));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(2)));
        var rows = cut.FindAll("tbody tr.lt-table__row");
        Assert.Multiple(() =>
        {
            Assert.That(rows.Select(row => row.QuerySelectorAll("td")[0].TextContent), Is.EqualTo(new[] { "blob-ops", "nuget-contoso" }));
            Assert.That(cut.FindAll(".lt-apps-app__note").Select(note => note.TextContent), Is.All.EqualTo("Offered by 2 sources: install from the one you trust"));
            Assert.That(cut.FindAll("tbody .lt-pill").Select(pill => pill.TextContent), Is.All.EqualTo("Offered by 2 sources"));
            Assert.That(cut.FindAll("tbody a.lt-btn").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "apps/catalogue/blob-ops/quality-gates%401.1.4", "apps/catalogue/nuget-contoso/quality-gates%401.2.0" }));
        });
    }

    [TestCase(null, null, "Available", "Review")]
    [TestCase("1.0.0", AppLifecycleState.Installed, "Installed v1.0.0", "Manage")]
    [TestCase("1.0.0", AppLifecycleState.Enabled, "Enabled", "Manage")]
    [TestCase("1.0.0", AppLifecycleState.Disabled, "Disabled", "Manage")]
    [TestCase("1.0.0", AppLifecycleState.Failed, "Activation failed", "Re-consent")]
    public void Each_row_shows_its_state_pill_and_its_one_action(string? installed, AppLifecycleState? state, string pill, string action)
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image", "1.0.0", installed, state));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("tbody .lt-pill").TextContent, Is.EqualTo(pill));
            Assert.That(cut.Find("tbody a.lt-btn:not(.lt-btn--quiet)").TextContent, Is.EqualTo(action));
        });
    }

    [Test]
    public void An_enabled_app_the_caller_holds_a_role_in_can_be_opened_from_its_row()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image", "1.0.0", "1.0.0", AppLifecycleState.Enabled));
        Workspace.Apps.Add(AppsTestData.Mine("crm", hasUi: true));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody a[href='apps/crm/window'][target='_blank'][rel='noopener noreferrer']"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void The_listing_pages_with_its_continuation()
    {
        Catalog.PageSize = 2;
        Catalog.Sources.Add(AppsTestData.InImage);
        for (var i = 0; i < 5; i++)
        {
            Catalog.Offers.Add(AppsTestData.Offer($"app-{i}", "in-image"));
        }

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(2)));
        Assert.That(cut.Find(".lt-apps-count").TextContent, Is.EqualTo("1 source, 2+ apps"));

        cut.Find(".lt-apps-more button").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(4)));
        cut.Find(".lt-apps-more button").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(5)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-more"), Is.Empty);
            Assert.That(Catalog.Queries.Where(query => query.PageSize == AvailableAppQuery.DefaultPageSize).Select(query => query.Continuation), Is.EqualTo(new[] { null, "2", "4" }));
        });
    }

    [Test]
    public void Thousands_of_apps_are_virtualised_once_loaded_past_the_threshold()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        for (var i = 0; i < 150; i++)
        {
            Catalog.Offers.Add(AppsTestData.Offer($"app-{i:D3}", "in-image"));
        }

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(50)));
        cut.Find(".lt-apps-more button").Click();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-count").TextContent, Is.EqualTo("1 source, 100+ apps")));
        cut.Find(".lt-apps-more button").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-apps-count").TextContent, Is.EqualTo("1 source, 150 apps")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.LessThan(150));
            Assert.That(cut.FindAll("tbody > tr:not(.lt-table__row)"), Is.Not.Empty, "virtualisation reserves the unrendered rows with spacers");
        });
    }

    [Test]
    public void No_source_and_no_match_are_empty_states()
    {
        var none = RenderAt<AppsCataloguePage>("/apps/catalogue");
        none.WaitUntil(() => Assert.That(none.Find(".lt-empty h2").TextContent, Is.EqualTo("No app sources")));

        Catalog.Sources.Add(AppsTestData.InImage);
        var empty = RenderAt<AppsCataloguePage>("/apps/catalogue?source=all&filter=installed");
        empty.WaitUntil(() => Assert.That(empty.Find(".lt-empty h2").TextContent, Is.EqualTo("No apps match")));
        Assert.That(empty.Find(".lt-empty").TextContent, Does.Contain("Nothing from this selection is installed here."));
    }

    [Test]
    public void A_failing_listing_shows_a_human_error_and_retries()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.AvailableFailure = new TimeoutException("rpc failed at 10.0.0.7 with a/crm/x@t-1");

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-apps-error"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-apps-error").TextContent, Does.Contain("could not be reached").And.Not.Contain("10.0.0.7"));
            Assert.That(cut.Find(".lt-apps-error").GetAttribute("role"), Is.EqualTo("alert"));
            Assert.That(cut.FindAll("table"), Is.Empty);
        });

        Catalog.AvailableFailure = null;
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image", "1.0.0", "1.0.0", AppLifecycleState.Enabled));
        cut.Find(".lt-apps-error button").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.That(cut.FindAll(".lt-apps-error"), Is.Empty);
    }

    [Test]
    public void A_restricted_identity_gets_not_found_and_learns_nothing()
    {
        Restrict();
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image"));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("h1"), Is.Empty);
            Assert.That(cut.FindAll("table, select, [role=alert]"), Is.Empty);
            Assert.That(cut.Markup, Does.Not.Contain("crm").And.Not.Contain("in-image"));
            Assert.That(Catalog.Queries, Is.Empty, "nothing is listed for a caller without AppInstall");
        });
    }

    [Test]
    public void Hostile_presentation_text_renders_literally_and_icons_are_data_images()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        var app = AppsTestData.TaskBoard(withIcon: true) with
        {
            Presentation = new AppPresentationDescriptor
            {
                DisplayName = "<b onclick=alert(1)>Board</b>",
                Summary = "<script>alert(1)</script>",
                Icon = new AppIconDescriptor { Path = "i.svg", Sha256 = AppsTestData.Digest('b') },
            },
        };
        Catalog.Offers.Add(AppsTestData.Offer(app, "in-image"));
        Catalog.Icons[("in-image", app.Slug)] = AppsTestData.Icon;

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue");

        cut.WaitUntil(() => Assert.That(cut.FindAll("img.lt-apps-icon"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-apps-app__name").TextContent, Is.EqualTo("<b onclick=alert(1)>Board</b>"));
            Assert.That(cut.FindAll("b, script"), Is.Empty);
            Assert.That(cut.Find("img.lt-apps-icon").GetAttribute("src"), Does.StartWith("data:image/svg+xml;base64,"));
            Assert.That(cut.Find("img.lt-apps-icon").GetAttribute("alt"), Is.Empty);
        });
    }

    [Test]
    public void Below_768px_the_toolbar_uses_a_filter_select_and_rows_become_two_line_rows()
    {
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image", "1.0.0", "1.0.0", AppLifecycleState.Enabled, name: "CRM"));

        var cut = RenderAt<AppsCataloguePage>("/apps/catalogue", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-apps-filter"), Is.Empty);
            Assert.That(cut.FindAll(".lt-toolbar select"), Has.Count.EqualTo(2), "more than three filters become a select");
            Assert.That(cut.Find(".lt-compact-row").TextContent, Does.Contain("CRM").And.Contain("in-image").And.Contain("Enabled"));
        });

        cut.FindAll(".lt-toolbar select")[1].Change("available");
        Assert.That(Navigation.Uri, Does.EndWith("filter=available"));

        cut.Find(".lt-table-list__open").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog] a.lt-btn").TextContent, Is.EqualTo("Manage")));
    }

    [Test]
    public void With_tenancy_on_the_lede_names_the_tenant_and_links_are_rooted_at_it()
    {
        UseTenancy("acme");
        Catalog.Sources.Add(AppsTestData.InImage);
        Catalog.Offers.Add(AppsTestData.Offer("crm", "in-image"));

        var cut = RenderAt<AppsCataloguePage>("/t/acme/apps/catalogue");

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr.lt-table__row"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("for tenant acme"));
            Assert.That(cut.Find("tbody a.lt-btn").GetAttribute("href"), Is.EqualTo("t/acme/apps/catalogue/in-image/crm%401.0.0"));
        });
    }
}
