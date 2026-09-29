using Bunit;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.Shell.Areas.Replication;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using static Orleans.Lattice.Explorer.Tests.Shell.Areas.Replication.ReplicationTestData;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Replication;

/// <summary>
/// <c>/replication/trees/{tree-path}</c>: one tree's per-peer links, its enrolment and
/// owner, a refresh cadence that stops while the page is hidden, and not-found for a
/// tree nothing the caller may see names.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationTreePageTests : ReplicationTestContext
{
    private static readonly TimeSpan Interval = new ReplicationOptions().RefreshInterval;

    [Test]
    public void It_shows_the_trees_links_per_peer_and_direction_worst_first()
    {
        UseEstate();
        Control.Trees.Add(Tree("a/crm/contacts"));

        var cut = RenderAt<ReplicationTreePage>("replication/trees/a/crm/contacts");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("a/crm/contacts"));
            Assert.That(cut.Find("h1").ClassList, Does.Contain("lt-shell-mono"));
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("Replicated - LWW register - Runtime enrolment - owned by app crm"));
            Assert.That(cut.Find(".lt-shell-page-lede a").GetAttribute("href"), Is.EqualTo("apps/crm/replication"));
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].Children[0].LocalName, Is.EqualTo("th"), "without a tree column the peer is the row header");
            Assert.That(rows.Select(row => (row.Children[0].TextContent.Trim(), row.Children[1].TextContent.Trim(), row.Children[2].TextContent.Trim())),
                Is.EqualTo(new[] { ("ap-south", "Outbound", "Stalled"), ("ap-south", "Inbound", "Lagging") }));
            Assert.That(cut.FindAll("thead th").Select(header => header.TextContent.Trim()), Does.Not.Contain("Tree"));
            Assert.That(cut.FindAll(".lt-replication-sections__link")[1].GetAttribute("aria-current"), Is.EqualTo("location"));
            Assert.That(Status.Queries.Last().TreeId, Is.EqualTo("a/crm/contacts"));
        });
    }

    [Test]
    public void A_tree_outside_the_callers_enrolment_still_shows_its_links()
    {
        UseEstate();

        var cut = RenderAt<ReplicationTreePage>("replication/trees/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent.Trim(), Is.EqualTo("Not in the enrolment you may manage."));
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void It_refreshes_on_its_cadence_while_the_page_is_visible()
    {
        UseEstate();
        var cut = RenderAt<ReplicationTreePage>("replication/trees/orders");
        cut.WaitUntil(() => Assert.That(cut.Instance.Loop?.IsRunning, Is.True));
        var before = Status.Calls;

        Status.Links.Add(Link("orders", "sa-east", ReplicationLinkHealth.Stalled));
        cut.InvokeAsync(() => Time.Advance(Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(Status.Calls, Is.EqualTo(before + 1));
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3));
            Assert.That(cut.Find("tbody tr").Children[2].TextContent.Trim(), Is.EqualTo("Stalled"), "a newly stalled link rises to the top");
            Assert.That(cut.Find(".lt-replication-status").TextContent, Does.Contain("refreshing every 5 s"));
        });
    }

    [Test]
    public void The_cadence_stops_while_the_page_is_hidden_and_resumes_with_an_immediate_read()
    {
        UseEstate();
        var cut = RenderAt<ReplicationTreePage>("replication/trees/orders");
        cut.WaitUntil(() => Assert.That(cut.Instance.Loop?.IsRunning, Is.True));
        var before = Status.Calls;

        cut.InvokeAsync(() => Visibility.Set(false));
        cut.InvokeAsync(() => Time.Advance(Interval * 6));
        var whileHidden = Status.Calls;
        cut.Render();
        var pausedLine = cut.Find(".lt-replication-status").TextContent;

        cut.InvokeAsync(() => Visibility.Set(true));

        cut.WaitUntil(() =>
        {
            Assert.That(whileHidden, Is.EqualTo(before), "no read while hidden");
            Assert.That(pausedLine, Does.Contain("paused while the page is hidden"));
            Assert.That(Status.Calls, Is.EqualTo(before + 1), "showing the page reads at once");
            Assert.That(cut.Instance.Loop!.IsRunning, Is.True);
        });
    }

    [Test]
    public async Task Leaving_the_page_stops_the_cadence()
    {
        UseEstate();
        var cut = RenderAt<ReplicationTreePage>("replication/trees/orders");
        cut.WaitUntil(() => Assert.That(cut.Instance.Loop?.IsRunning, Is.True));
        var loop = cut.Instance.Loop!;

        await DisposeComponentsAsync();
        var before = Status.Calls;
        Time.Advance(Interval * 3);

        Assert.Multiple(() =>
        {
            Assert.That(loop.IsRunning, Is.False);
            Assert.That(Status.Calls, Is.EqualTo(before));
            Assert.That(Visibility.Subscribers, Is.Zero);
        });
    }

    [Test]
    public void A_failed_refresh_keeps_the_last_read_and_says_so()
    {
        UseEstate();
        var cut = RenderAt<ReplicationTreePage>("replication/trees/orders");
        cut.WaitUntil(() => Assert.That(cut.Instance.Loop?.IsRunning, Is.True));

        Status.Failure = new TimeoutException();
        cut.InvokeAsync(() => Time.Advance(Interval));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(2));
            Assert.That(cut.Find(".lt-replication-note").TextContent, Does.Contain("The last refresh failed").And.Contain("previous read"));
        });

        Status.Failure = null;
        cut.InvokeAsync(() => Time.Advance(Interval));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-replication-note"), Is.Empty));
    }

    [Test]
    public void A_tree_whose_links_cannot_be_read_says_why()
    {
        Control.Trees.Add(Tree("orders"));
        Status.Failure = new LatticeAuthorizationDeniedException();

        var cut = RenderAt<ReplicationTreePage>("replication/trees/orders");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty h2").TextContent, Is.EqualTo("This tree's links could not be read"));
            Assert.That(cut.Find(".lt-empty").TextContent, Does.Contain("You are not allowed to see replication status"));
        });
    }

    [Test]
    public void A_tree_nothing_the_caller_may_see_names_is_not_found()
    {
        UseEstate();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;

        RenderAt<ReplicationTreePage>("replication/trees/missing");

        Assert.That(notFound, Is.EqualTo(1));
    }

    [Test]
    public void An_enrolled_tree_with_no_links_yet_says_so()
    {
        Control.Trees.Add(Tree("fresh", enabled: false, mode: null));

        var cut = RenderAt<ReplicationTreePage>("replication/trees/fresh");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-shell-page-lede").TextContent, Does.Contain("Not replicated - None - Runtime enrolment"));
            Assert.That(cut.Find(".lt-table__empty").TextContent.Trim(), Is.EqualTo("This tree has no replication links yet."));
        });
    }

    [Test]
    public void Below_the_compact_width_each_link_is_a_row_named_by_its_peer()
    {
        UseEstate();

        var cut = RenderAt<ReplicationTreePage>("replication/trees/a/crm/contacts", LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll(".lt-compact-row");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].QuerySelector(".lt-compact-row__primary")!.TextContent, Is.EqualTo("ap-south"));
        });
    }

    [Test]
    public void With_tenancy_on_the_app_link_is_rooted_at_the_active_tenant()
    {
        UseTenancy("acme");
        UseEstate();

        var cut = RenderAt<ReplicationTreePage>("t/acme/replication/trees/a/crm/contacts", tenancy: true);

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-shell-page-lede a").GetAttribute("href"), Is.EqualTo("t/acme/apps/crm/replication")));
    }
}
