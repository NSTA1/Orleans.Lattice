using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Areas.Cluster.Pages;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>
/// <c>/cluster</c>: the estate's identity and storage, the region picture drawn
/// as an order diagram (this region the marker node, a fully stalled peer dashed
/// and labelled), and the stops that administer trees, WAL and orphans.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ClusterOverviewTests : ClusterTestContext
{
    [Test]
    public void It_shows_the_cluster_identity_and_storage_use()
    {
        Admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>())
            .Returns(new ClusterStorageUsageSummary { TreeCount = 12, TotalBytes = 2048, WalRetainedBytes = 1024, Partial = true });

        var cut = RenderAt("/cluster");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Cluster"));
            Assert.That(cut.Markup, Does.Contain("lattice-prod"));
            Assert.That(cut.Markup, Does.Contain("2.0 KiB"));
            Assert.That(cut.Markup, Does.Contain("From the cached WAL poll"));
            Assert.That(cut.Markup, Does.Contain(", partial"));
        });
    }

    [Test]
    public void Re_measuring_every_shard_is_expensive_so_it_asks_for_the_cluster_id()
    {
        // A head that serves no storage-usage operations (#4126) re-measures with the blocking deep read.
        Services.AddKeyedSingleton<ILatticeStorageUsageOperations>(ShellFacades.Key, (_, _) => null!);
        Admin.GetStorageUsageAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(call => new ClusterStorageUsageSummary { TreeCount = 12, Deep = call.Arg<bool>() });
        var cut = RenderAt("/cluster");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Re-measure every shard..."), Is.True));

        Button(cut, "Re-measure every shard...").Click();
        cut.Find(".lt-confirm input").Input("lattice");
        Assert.That(cut.Find(".lt-confirm button[type=submit]").HasAttribute("disabled"), Is.True, "a near miss does not enable it");
        ConfirmTyping(cut, "lattice-prod");

        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("Re-measured by a leaf walk")));
        Admin.Received(1).GetStorageUsageAsync(true, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Refresh_reads_the_cached_summary_again()
    {
        Admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>()).Returns(new ClusterStorageUsageSummary { TreeCount = 1 });
        var cut = RenderAt("/cluster");
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Refresh"), Is.True));

        Button(cut, "Refresh").Click();

        cut.WaitUntil(() => Admin.Received(2).GetStorageUsageAsync(false, Arg.Any<CancellationToken>()));
    }

    [Test]
    public void Failures_read_as_sentences_and_a_restricted_identity_sees_why()
    {
        Admin.GetStorageUsageAsync(false, Arg.Any<CancellationToken>()).ThrowsAsync(new LatticeAuthorizationDeniedException("denied"));
        Explorer.Connection.GetClusterInfoAsync(Arg.Any<ClusterInfoRequest>(), Arg.Any<CancellationToken>()).ThrowsAsync(new TimeoutException());

        var cut = RenderAt("/cluster");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-cluster-error").Select(error => error.TextContent),
            Is.EqualTo(new[] { "The cluster did not answer in time.", "You do not have permission to do this." })));
    }

    [Test]
    public void The_administer_stops_are_a_spine_of_links()
    {
        var cut = RenderAt("/cluster");

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-cluster-stops").ClassList, Does.Contain("lt-spine"));
            Assert.That(cut.FindAll(".lt-cluster-stops__link").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "cluster/trees", "cluster/wal", "cluster/orphans" }));
            Assert.That(cut.Markup, Does.Not.Contain("card"));
        });
    }

    [Test]
    public void The_region_picture_marks_this_region_and_draws_a_fully_stalled_peer_dashed_and_labelled()
    {
        Status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>()).Returns(new ReplicationPeerStatusPage("eu-west",
        [
            Link("orders", "us-east", ReplicationLinkHealth.Stalled),
            Link("invoices", "us-east", ReplicationLinkHealth.Stalled),
            Link("orders", "ap-south", ReplicationLinkHealth.Healthy),
            Link("invoices", "ap-south", ReplicationLinkHealth.Stalled),
        ], null));

        var cut = RenderAt("/cluster");

        cut.WaitUntil(() =>
        {
            var local = cut.Find(".lt-cluster-regions__local");
            var peers = cut.FindAll(".lt-cluster-regions__peer");
            Assert.That(local.QuerySelector(".lt-node--join"), Is.Not.Null, "this region is the marker node");
            Assert.That(local.TextContent, Does.Contain("eu-west").And.Contain("this region"), "the marker is never alone");
            Assert.That(peers.Select(peer => peer.QuerySelector(".lt-cluster-regions__name")!.TextContent), Is.EqualTo(new[] { "ap-south", "us-east" }));
            Assert.That(peers[1].ClassList, Does.Contain("lt-cluster-regions__peer--stalled"));
            Assert.That(peers[1].TextContent, Does.Contain("Stalled"), "stalled is a word, not only a colour");
            Assert.That(peers[1].QuerySelector(".lt-node--hollow"), Is.Not.Null);
            Assert.That(peers[0].ClassList, Does.Not.Contain("lt-cluster-regions__peer--stalled"), "one live link keeps a peer connected");
            Assert.That(peers[0].TextContent, Does.Contain("Lagging").And.Contain("2 links across 2 trees, 1 stalled"));
            Assert.That(cut.Find(".lt-cluster-regions__caption").TextContent, Is.EqualTo("This region, eu-west, and 2 peer regions, 1 stalled."));
            Assert.That(cut.Find(".lt-cluster-regions").GetAttribute("aria-labelledby"), Is.EqualTo(cut.Find(".lt-cluster-regions__caption").Id));
        });
    }

    [Test]
    public void A_single_region_estate_is_this_region_alone()
    {
        var cut = RenderAt("/cluster");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-cluster-regions__peer"), Is.Empty);
            Assert.That(cut.Find(".lt-cluster-regions__alone").TextContent, Is.EqualTo("No peer region: this cluster replicates with no other region."));
            Assert.That(cut.Find(".lt-cluster-regions__caption").TextContent, Is.EqualTo("This region, eu-west, and no peer region."));
        });
    }

    [Test]
    public void The_region_picture_follows_every_page_of_the_report()
    {
        Status.GetPeerStatusAsync(Arg.Is<ReplicationPeerStatusQuery>(query => query.ContinuationToken == null), Arg.Any<CancellationToken>())
            .Returns(new ReplicationPeerStatusPage("eu-west", [Link("orders", "us-east", ReplicationLinkHealth.Healthy)], "page-2"));
        Status.GetPeerStatusAsync(Arg.Is<ReplicationPeerStatusQuery>(query => query.ContinuationToken == "page-2"), Arg.Any<CancellationToken>())
            .Returns(new ReplicationPeerStatusPage("eu-west", [Link("orders", "ap-south", ReplicationLinkHealth.Healthy)], null));

        var cut = RenderAt("/cluster");

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-cluster-regions__peer"), Has.Count.EqualTo(2)));
    }

    [Test]
    public void Without_the_peer_report_it_says_so_and_still_links_to_replication()
    {
        Services.RemoveAllKeyed<ILatticeReplicationStatus>(ShellFacades.Key);

        var cut = RenderAt("/cluster");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-cluster-regions"), Is.Empty);
            Assert.That(cut.Markup, Does.Contain("This Explorer does not read the replication peer report, so no region is drawn."));
            Assert.That(cut.Find("a.lt-cluster-link").GetAttribute("href"), Is.EqualTo("replication"));
        });
    }

    [Test]
    public void The_region_picture_alone_renders_as_a_component()
    {
        var cut = Render<ClusterRegionDiagram>();

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-cluster-regions__local"), Has.Count.EqualTo(1)));
    }

    private static ReplicationPeerStatusEntry Link(string tree, string peer, ReplicationLinkHealth health) =>
        new(tree, peer, ReplicationLinkDirection.Outbound, 0, 0, 0, TimeSpan.FromSeconds(1), 0, health);
}
