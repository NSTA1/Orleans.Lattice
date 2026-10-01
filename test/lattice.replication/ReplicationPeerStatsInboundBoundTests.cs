namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The inbound half of <see cref="ReplicationPeerStats"/> is keyed by values a remote peer
/// supplies, so it is bounded (issue #4021): once the cap is reached a new inbound pair is not
/// recorded, existing pairs keep updating, and outbound rows are unaffected.
/// </summary>
[TestFixture]
public class ReplicationPeerStatsInboundBoundTests
{
    [Test]
    public void New_inbound_pairs_beyond_the_cap_are_not_recorded()
    {
        var stats = new ReplicationPeerStats(maxInboundRows: 3);

        for (var i = 0; i < 10; i++)
        {
            stats.RecordInboundSuccess("tree", "origin-" + i);
            stats.RecordInboundError("tree-" + i, "origin");
        }

        Assert.Multiple(() =>
        {
            Assert.That(stats.InboundRowCount, Is.EqualTo(3));
            Assert.That(stats.Snapshot().Count(s => s.Direction == ReplicationContactDirection.Inbound), Is.EqualTo(3));
        });
    }

    [Test]
    public void A_pair_already_held_keeps_updating_at_the_cap()
    {
        var stats = new ReplicationPeerStats(maxInboundRows: 1);
        stats.RecordInboundError("tree", "site-b");
        stats.RecordInboundSuccess("tree", "planted");

        stats.RecordInboundError("tree", "site-b");

        var row = stats.Snapshot().Single();
        Assert.Multiple(() =>
        {
            Assert.That(row.Peer, Is.EqualTo("site-b"));
            Assert.That(row.ConsecutiveErrors, Is.EqualTo(2));
        });
    }

    [Test]
    public void The_cap_does_not_limit_outbound_rows()
    {
        var stats = new ReplicationPeerStats(maxInboundRows: 0);

        stats.RecordInboundSuccess("tree", "site-b");
        stats.RecordSuccess("tree", "site-b");
        stats.RecordError("tree", "site-c");

        Assert.Multiple(() =>
        {
            Assert.That(stats.InboundRowCount, Is.Zero);
            Assert.That(stats.Snapshot().Select(s => s.Direction), Is.All.EqualTo(ReplicationContactDirection.Outbound));
            Assert.That(stats.Snapshot(), Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void Concurrent_new_pairs_never_overshoot_the_cap()
    {
        var stats = new ReplicationPeerStats(maxInboundRows: 64);

        Parallel.For(0, 4_000, i => stats.RecordInboundSuccess("tree", "origin-" + (i % 1_000)));

        Assert.Multiple(() =>
        {
            Assert.That(stats.InboundRowCount, Is.EqualTo(64));
            Assert.That(stats.Snapshot(), Has.Count.EqualTo(64));
        });
    }

    [Test]
    public void The_default_instance_is_bounded_by_the_default_cap()
    {
        var stats = new ReplicationPeerStats();

        for (var i = 0; i < ReplicationPeerStats.DefaultMaxInboundRows + 10; i++)
        {
            stats.RecordInboundSuccess("tree", "origin-" + i);
        }

        Assert.That(stats.InboundRowCount, Is.EqualTo(ReplicationPeerStats.DefaultMaxInboundRows));
    }

    [Test]
    public void A_negative_cap_is_rejected() =>
        Assert.That(() => new ReplicationPeerStats(maxInboundRows: -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
}
