using Bunit;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using static Orleans.Lattice.Explorer.Tests.UI.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// <c>/replication</c>: the order diagram of the estate, the sortable link table
/// beneath it, the health / region / app filters, the loading, empty and fault states,
/// the refresh command, the compact presentation and tenancy.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationEstatePageTests : ReplicationTestContext
{
    [Test]
    public void It_shows_a_skeleton_until_the_status_report_arrives()
    {
        UseEstate();
        Status.Gate = new TaskCompletionSource();

        var cut = RenderAt<ReplicationEstatePage>("replication");
        var loading = cut.FindAll(".lt-skeleton").Count;
        cut.InvokeAsync(() => Status.Gate.SetResult());

        cut.WaitUntil(() =>
        {
            Assert.That(loading, Is.EqualTo(1));
            Assert.That(cut.FindAll(".lt-replication-map"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Replication"));
        });
    }

    [Test]
    public void The_diagram_marks_this_region_and_draws_each_peer_worst_first()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() =>
        {
            var local = cut.Find(".lt-replication-map__local");
            Assert.That(local.QuerySelector(".lt-node--join"), Is.Not.Null, "this region is the marker node");
            Assert.That(local.TextContent, Does.Contain("eu-west").And.Contain("This region"), "the marker is never alone");
            Assert.That(cut.FindAll(".lt-replication-map__peer .lt-replication-map__peer-link .lt-replication-map__region").Select(node => node.TextContent),
                Is.EqualTo(new[] { "ap-south", "sa-east", "us-east" }));
            Assert.That(cut.Find(".lt-replication-map__caption").TextContent, Is.EqualTo("Replication links between eu-west and 3 peer regions"));
        });
    }

    [Test]
    public void Every_health_state_is_drawn_with_a_glyph_a_word_and_a_line_style()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() =>
        {
            var edges = cut.FindAll(".lt-replication-map__edge");
            Assert.That(edges.Select(edge => edge.GetAttribute("data-health")), Is.EqualTo(new[] { "stalled", "lagging", "unknown", "none", "healthy", "healthy" }));
            Assert.That(edges.Select(edge => edge.QuerySelector(".lt-pill__text")?.TextContent ?? edge.QuerySelector(".lt-replication-map__idle")!.TextContent),
                Is.EqualTo(new[] { "Stalled", "Lagging", "Unknown", "No links", "Healthy", "Healthy" }));
            Assert.That(edges[0].QuerySelector(".lt-pill")!.GetAttribute("data-lt-state"), Is.EqualTo(LtStateRoles.Key(LtStateRole.Stalled)));
            Assert.That(edges[0].QuerySelector(".lt-pill__glyph")!.GetAttribute("aria-hidden"), Is.EqualTo("true"));
            Assert.That(edges[0].QuerySelector(".lt-replication-map__direction")!.TextContent, Does.Contain("To ap-south"));
            Assert.That(edges[1].QuerySelector(".lt-replication-map__direction")!.TextContent, Does.Contain("From ap-south"));
        });
    }

    [Test]
    public void A_stalled_edge_carries_its_backlog_tree_count_and_the_stalled_link_count()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() =>
        {
            var stalled = cut.Find(".lt-replication-map__edge[data-health=\"stalled\"]");
            Assert.That(stalled.QuerySelector(".lt-replication-map__backlog")!.TextContent, Is.EqualTo("1,204 entries, 3.2 MB behind"));
            Assert.That(stalled.QuerySelector(".lt-replication-map__count")!.TextContent, Is.EqualTo("2 trees"));
            Assert.That(stalled.QuerySelector(".lt-replication-map__alert")!.TextContent, Is.EqualTo("1 link stalled"));
            Assert.That(cut.Find(".lt-replication-map__edge[data-health=\"healthy\"] .lt-replication-map__backlog").TextContent, Is.EqualTo("Caught up"));
        });
    }

    [Test]
    public void The_table_lists_every_link_worst_first_with_the_stalled_link_on_top()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(6));
            var cells = rows[0].Children.Select(cell => cell.TextContent.Trim()).ToArray();
            Assert.That(cells, Is.EqualTo(new[] { "a/crm/contacts", "ap-south", "Outbound", "Stalled", "Re-seed required", "1,204", "3.2 MB", "7", "14 min ago", "0" }));
            Assert.That(rows[0].QuerySelector("th a")!.GetAttribute("href"), Is.EqualTo("replication/trees/a/crm/contacts"));
            Assert.That(rows.Select(row => row.Children[8].TextContent.Trim()), Does.Contain("Never"));
        });
    }

    [TestCase("Errors", "a/crm/contacts")]
    [TestCase("Entries behind", "a/crm/contacts")]
    [TestCase("Last contact", "a/billing/invoices")]
    [TestCase("Health", "a/crm/contacts")]
    public void The_table_sorts_by_health_backlog_errors_and_contact(string column, string topWhenDescending)
    {
        UseEstate();
        var cut = RenderAt<ReplicationEstatePage>("replication");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(6)));

        var sort = cut.FindAll("th button.lt-table__sort").Single(button => button.TextContent.Trim() == column);
        sort.Click();
        cut.FindAll("th button.lt-table__sort").Single(button => button.TextContent.Trim() == column).Click();

        Assert.That(cut.FindAll("tbody tr")[0].Children[0].TextContent.Trim(), Is.EqualTo(topWhenDescending));
    }

    [Test]
    public void A_health_filter_narrows_the_diagram_the_table_and_the_status_line()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication?health=stalled");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lt-replication-map__peer"), Has.Count.EqualTo(1));
            Assert.That(cut.Find(".lt-replication-status").TextContent, Does.Contain("1 of 6 links match."));
            Assert.That(cut.FindAll("select")[0].QuerySelector("option[selected]")!.GetAttribute("value"), Is.EqualTo("stalled"));
        });
    }

    [Test]
    public void A_region_filter_marks_that_peer_as_current_and_its_link_clears_the_filter()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication?region=us-east");

        cut.WaitUntil(() =>
        {
            var peer = cut.Find(".lt-replication-map__peer-link");
            Assert.That(peer.GetAttribute("aria-current"), Is.EqualTo("true"));
            Assert.That(peer.GetAttribute("href"), Is.EqualTo("replication"));
            Assert.That(peer.TextContent, Does.Contain("filtered"));
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()), Is.EqualTo(new[] { "orders", "orders" }));
        });
    }

    [Test]
    public void An_app_filter_from_the_apps_area_shows_only_that_apps_trees()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication?app=billing");

        cut.WaitUntil(() => Assert.That(
            cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()).Distinct(),
            Is.EqualTo(new[] { "a/billing/invoices" })));
    }

    [Test]
    public void Choosing_a_filter_rewrites_the_address_in_place()
    {
        UseEstate();
        var cut = RenderAt<ReplicationEstatePage>("replication?app=crm");
        cut.WaitUntil(() => Assert.That(cut.FindAll("select"), Has.Count.EqualTo(3)));

        cut.FindAll("select")[0].Change("lagging");
        var afterHealth = Navigation.Uri;
        cut.FindAll("select")[1].Change("ap-south");

        Assert.Multiple(() =>
        {
            Assert.That(afterHealth, Does.EndWith("replication?app=crm&health=lagging"));
            Assert.That(Navigation.Uri, Does.EndWith("replication?app=crm&region=ap-south"),
                "each select writes one key onto the page's own address");
        });
    }

    [Test]
    public void Clear_filters_appears_only_when_a_filter_is_set_and_removes_them_all()
    {
        UseEstate();
        var unfiltered = RenderAt<ReplicationEstatePage>("replication");
        unfiltered.WaitUntil(() => Assert.That(unfiltered.FindAll(".lt-replication-map"), Has.Count.EqualTo(1)));
        var filtered = RenderAt<ReplicationEstatePage>("replication?health=stalled&region=ap-south&app=crm");
        filtered.WaitUntil(() => Assert.That(filtered.FindAll(".lt-replication-map"), Has.Count.EqualTo(1)));

        Assert.That(unfiltered.FindAll("button").Select(button => button.TextContent.Trim()), Does.Not.Contain("Clear filters"));
        filtered.FindAll("button").Single(button => button.TextContent.Trim() == "Clear filters").Click();

        Assert.That(Navigation.Uri, Does.EndWith("/replication"));
    }

    [Test]
    public void A_filter_that_names_nothing_present_stays_selectable_and_empties_the_page_with_a_reason()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication?region=mars");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("select")[1].QuerySelectorAll("option").Select(option => option.GetAttribute("value")), Does.Contain("mars"));
            Assert.That(cut.Find(".lt-replication-map__empty").TextContent, Is.EqualTo("No peer region is linked to this region under these filters."));
            Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("No replication links match these filters."));
        });
    }

    [Test]
    public void An_estate_with_no_links_says_so()
    {
        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-replication-map__empty").TextContent, Is.EqualTo("No peer region is linked to this region."));
            Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("This region has no replication links yet."));
        });
    }

    [Test]
    public void A_truncated_report_says_how_much_is_shown()
    {
        Status.Links.AddRange(Estate());
        Status.StuckToken = "loop";

        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-replication-note").TextContent, Does.Contain("only the first 6 links are shown")));
    }

    [Test]
    public void A_denied_identity_is_told_so_and_pointed_at_the_trees_it_may_manage()
    {
        Status.Failure = new LatticeAuthorizationDeniedException();

        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Replication status is not open to you"));
            Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain("You are not allowed to see replication status on this cluster."));
            Assert.That(cut.FindAll("button").Select(button => button.TextContent.Trim()), Does.Not.Contain("Try again"));
            Assert.That(cut.FindAll(".lt-empty a").Single().GetAttribute("href"), Is.EqualTo("replication/trees"));
            Assert.That(cut.FindAll(".lt-replication-map"), Is.Empty);
        });
    }

    [Test]
    public void An_unserved_status_facade_is_named_as_such()
    {
        Status.Failure = new NotSupportedException();

        var cut = RenderAt<ReplicationEstatePage>("replication");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Replication status is not served here")));
    }

    [Test]
    public void A_failed_read_offers_try_again_which_reads_afresh()
    {
        Status.Failure = new TimeoutException();
        var cut = RenderAt<ReplicationEstatePage>("replication");
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("Replication status could not be read")));

        Status.Failure = null;
        UseEstate();
        cut.FindAll("button").Single(button => button.TextContent.Trim() == "Try again").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-replication-map__peer"), Has.Count.EqualTo(3)));
    }

    [Test]
    public void The_refresh_command_has_a_visible_control_and_re_reads_the_estate()
    {
        UseEstate();
        var cut = RenderAt<ReplicationEstatePage>("replication");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-replication-map"), Has.Count.EqualTo(1)));
        var command = Area.Commands.Single(candidate => candidate.Id == ReplicationArea.RefreshCommandId);
        var before = Status.Calls;

        ExplorerCommandControls.AssertVisibleControl(cut, command);
        cut.InvokeAsync(async () => await command.InvokeAsync!(CancellationToken.None));
        cut.WaitUntil(() => Assert.That(Status.Calls, Is.EqualTo(before + 1)));

        Status.Links.Add(Link("stock", "us-east", ReplicationLinkHealth.Lagging));
        cut.Find($"[data-lt-command=\"{ReplicationArea.RefreshCommandId}\"]").Click();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(7)));
    }

    [Test]
    public void The_sections_mark_the_estate_and_carry_the_filters_to_the_trees()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication?app=crm");

        cut.WaitUntil(() =>
        {
            var links = cut.FindAll(".lt-replication-sections__link");
            Assert.That(links.Select(link => link.TextContent), Is.EqualTo(new[] { "Estate", "Enrolled trees" }));
            Assert.That(links[0].GetAttribute("aria-current"), Is.EqualTo("page"));
            Assert.That(links[1].GetAttribute("aria-current"), Is.Null);
            Assert.That(links[0].GetAttribute("href"), Is.EqualTo("replication?app=crm"));
            Assert.That(links[1].GetAttribute("href"), Is.EqualTo("replication/trees?app=crm"));
            Assert.That(cut.Find("link[rel=\"stylesheet\"]").GetAttribute("href"), Is.EqualTo(ReplicationAssets.Stylesheet));
        });
    }

    [Test]
    public void Below_the_compact_width_links_are_two_line_rows_that_open_a_detail_sheet()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__open"), Has.Count.EqualTo(6)));

        var first = cut.FindAll(".lt-table-list__open")[0];
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty, "no booktabs table at the compact width");
            Assert.That(first.QuerySelector(".lt-compact-row")!.TextContent, Does.Contain("a/crm/contacts").And.Contain("Stalled").And.Contain("To ap-south - 1,204 entries, 3.2 MB behind - Re-seed required"));
            Assert.That(cut.FindAll(".lt-table-list__sort select"), Has.Count.EqualTo(1), "sorting becomes a select");
        });

        first.Click();

        cut.WaitUntil(() =>
        {
            var sheet = cut.Find(".lt-dialog");
            Assert.That(sheet.TextContent, Does.Contain("a/crm/contacts: to ap-south").And.Contain("Stall reason").And.Contain("Re-seed required").And.Contain("Consecutive errors").And.Contain("14 min ago"));
            Assert.That(sheet.QuerySelector("a.lt-btn")!.GetAttribute("href"), Is.EqualTo("replication/trees/a/crm/contacts"));
        });
    }

    [Test]
    public void The_toolbar_stacks_in_the_shared_toolbar_frame()
    {
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("replication", LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            var toolbar = cut.Find(".lt-toolbar");
            Assert.That(toolbar.QuerySelectorAll("select").Length, Is.EqualTo(3), "five health options are a select, never a segmented row");
        });
    }

    [Test]
    public void With_tenancy_on_every_link_is_rooted_at_the_active_tenant()
    {
        UseTenancy("acme");
        UseEstate();

        var cut = RenderAt<ReplicationEstatePage>("t/acme/replication", tenancy: true);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("tbody th a").GetAttribute("href"), Is.EqualTo("t/acme/replication/trees/a/crm/contacts"));
            Assert.That(cut.FindAll(".lt-replication-sections__link")[1].GetAttribute("href"), Is.EqualTo("t/acme/replication/trees"));
            Assert.That(cut.Find(".lt-replication-map__peer-link").GetAttribute("href"), Is.EqualTo("t/acme/replication?region=ap-south"));
        });
    }

    [Test]
    public void Signing_in_as_someone_else_re_reads_the_estate()
    {
        UseEstate();
        var cut = RenderAt<ReplicationEstatePage>("replication");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-replication-map"), Has.Count.EqualTo(1)));
        var before = Status.Calls;

        cut.InvokeAsync(() => Auth.SignIn("dana"));

        cut.WaitUntil(() => Assert.That(Status.Calls, Is.EqualTo(before + 1)));
    }
}
