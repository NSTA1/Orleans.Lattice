using NSubstitute;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Cluster;

/// <summary>Peer-report pagination, bounded reads and cross-page region aggregation.</summary>
[TestFixture]
public sealed class ClusterRegionPictureTests
{
    [Test]
    public async Task ReadAsync_empty_report_keeps_the_local_region()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(ReplicationPeerStatusPage.Empty("local"));

        var picture = await ClusterRegionPicture.ReadAsync(status, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(picture.LocalRegionId, Is.EqualTo("local"));
            Assert.That(picture.Peers, Is.Empty);
            Assert.That(picture.Truncated, Is.False);
        });
        await status.Received(1).GetPeerStatusAsync(
            Arg.Is<ReplicationPeerStatusQuery>(query => query.ContinuationToken == null), CancellationToken.None);
    }

    [Test]
    public async Task ReadAsync_pages_through_empty_pages_and_rolls_up_both_directions()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        var tokens = new List<string?>();
        using var cancellation = new CancellationTokenSource();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), cancellation.Token)
            .Returns(call =>
            {
                var token = call.Arg<ReplicationPeerStatusQuery>().ContinuationToken;
                tokens.Add(token);
                return token switch
                {
                    null => new ReplicationPeerStatusPage("local",
                        [Link("orders", "west", ReplicationLinkHealth.Healthy)], "empty"),
                    "empty" => new ReplicationPeerStatusPage("local", [], "last"),
                    "last" => new ReplicationPeerStatusPage("local",
                    [
                        Link("orders", "west", ReplicationLinkHealth.Stalled, ReplicationLinkDirection.Inbound),
                        Link("invoices", "west", ReplicationLinkHealth.Lagging),
                        Link("orders", "east", ReplicationLinkHealth.Stalled),
                    ], null),
                    _ => throw new AssertionException("Unexpected continuation token."),
                };
            });

        var picture = await ClusterRegionPicture.ReadAsync(status, cancellation.Token);

        Assert.Multiple(() =>
        {
            Assert.That(tokens, Is.EqualTo(new string?[] { null, "empty", "last" }));
            Assert.That(picture.LocalRegionId, Is.EqualTo("local"));
            Assert.That(picture.Truncated, Is.False);
            Assert.That(picture.Peers, Is.EqualTo(new[]
            {
                new ClusterRegionPeer("east", 1, 0, 0, 1, 1),
                new ClusterRegionPeer("west", 3, 1, 1, 1, 2),
            }));
            Assert.That(picture.Peers[0].Health, Is.EqualTo(ReplicationLinkHealth.Stalled));
            Assert.That(picture.Peers[1].Health, Is.EqualTo(ReplicationLinkHealth.Lagging));
        });
        await status.Received(3).GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), cancellation.Token);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ReadAsync_page_limit_reports_truncation_only_when_more_remains(bool more)
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        var calls = 0;
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var token = call.Arg<ReplicationPeerStatusQuery>().ContinuationToken;
                Assert.That(token, Is.EqualTo(calls == 0 ? null : $"page-{calls}"));
                calls++;
                Assert.That(calls, Is.LessThanOrEqualTo(ClusterRegionPicture.MaximumPages));
                return new ReplicationPeerStatusPage("local",
                    [Link($"tree-{calls}", "west", ReplicationLinkHealth.Healthy)],
                    calls < ClusterRegionPicture.MaximumPages || more ? $"page-{calls}" : null);
            });

        var picture = await ClusterRegionPicture.ReadAsync(status, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(ClusterRegionPicture.MaximumPages));
            Assert.That(picture.Truncated, Is.EqualTo(more));
            Assert.That(picture.Peers, Has.Count.EqualTo(1));
            Assert.That(picture.Peers[0].Trees, Is.EqualTo(ClusterRegionPicture.MaximumPages));
            Assert.That(picture.Peers[0].Links, Is.EqualTo(ClusterRegionPicture.MaximumPages));
            Assert.That(picture.Peers[0].Health, Is.EqualTo(ReplicationLinkHealth.Healthy));
        });
    }

    [Test]
    public void ReadAsync_null_facade_is_rejected() =>
        Assert.That(async () => await ClusterRegionPicture.ReadAsync(null!, CancellationToken.None), Throws.ArgumentNullException);

    [Test]
    public void ReadAsync_cancelled_request_propagates_the_callers_token()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), cancellation.Token)
            .Returns(Task.FromCanceled<ReplicationPeerStatusPage>(cancellation.Token));

        var fault = Assert.ThrowsAsync<TaskCanceledException>(
            async () => await ClusterRegionPicture.ReadAsync(status, cancellation.Token));

        Assert.That(fault!.CancellationToken, Is.EqualTo(cancellation.Token));
    }

    [Test]
    public void ReadAsync_later_page_failure_is_not_a_success_shaped_partial_picture()
    {
        var status = Substitute.For<ILatticeReplicationStatus>();
        var fault = new IOException("peer report unavailable");
        status.GetPeerStatusAsync(Arg.Any<ReplicationPeerStatusQuery>(), Arg.Any<CancellationToken>())
            .Returns(
                Task.FromResult(new ReplicationPeerStatusPage("local",
                    [Link("orders", "west", ReplicationLinkHealth.Healthy)], "next")),
                Task.FromException<ReplicationPeerStatusPage>(fault));

        Assert.That(
            Assert.ThrowsAsync<IOException>(async () => await ClusterRegionPicture.ReadAsync(status, CancellationToken.None)),
            Is.SameAs(fault));
    }

    private static ReplicationPeerStatusEntry Link(
        string tree, string peer, ReplicationLinkHealth health,
        ReplicationLinkDirection direction = ReplicationLinkDirection.Outbound) =>
        new(tree, peer, direction, 0, 0, 0, TimeSpan.FromSeconds(1), 0, health);
}
