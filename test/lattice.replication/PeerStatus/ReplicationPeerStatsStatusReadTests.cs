namespace Orleans.Lattice.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="ReplicationPeerStats.ReadStatusPage"/>: field
/// fidelity, the read order, filters, the exclusive cursor, the bounded limit,
/// tenant rendering, and complete, duplicate-free paging.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatsStatusReadTests
{
    private static ReplicationPeerStatusReadRequest Read(
        int limit = ReplicationPeerStatusReadRequest.MaxLimit,
        string? tree = null,
        string? peer = null,
        string? strip = null,
        ReplicationPeerStatusCursor? after = null) =>
        new() { Limit = limit, TreeId = tree, Peer = peer, StripPrefix = strip, After = after };

    [Test]
    public void ReadStatusPage_carries_every_recorded_field()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordBacklog("orders", "east", entriesBehind: 12, bytesBehind: 3400);
        stats.RecordInFlight("orders", "east", depth: 2);
        stats.RecordSuccess("orders", "east");
        stats.RecordError("orders", "east");
        stats.Advance(TimeSpan.FromSeconds(7));

        var rows = stats.ReadStatusPage(Read());

        Assert.That(rows, Has.Length.EqualTo(1));
        var row = rows[0];
        Assert.Multiple(() =>
        {
            Assert.That(row.Tree, Is.EqualTo("orders"));
            Assert.That(row.Peer, Is.EqualTo("east"));
            Assert.That(row.Direction, Is.EqualTo(ReplicationContactDirection.Outbound));
            Assert.That(row.EntriesBehind, Is.EqualTo(12));
            Assert.That(row.BytesBehind, Is.EqualTo(3400));
            Assert.That(row.InFlight, Is.EqualTo(2));
            Assert.That(row.ConsecutiveErrors, Is.EqualTo(1));
            Assert.That(row.LastContactSeconds, Is.EqualTo(7d));
        });
    }

    [Test]
    public void ReadStatusPage_reports_NaN_for_a_link_never_contacted()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordInboundError("orders", "west");

        var row = stats.ReadStatusPage(Read()).Single();

        Assert.Multiple(() =>
        {
            Assert.That(row.Direction, Is.EqualTo(ReplicationContactDirection.Inbound));
            Assert.That(double.IsNaN(row.LastContactSeconds), Is.True);
            Assert.That(row.ConsecutiveErrors, Is.EqualTo(1));
        });
    }

    [Test]
    public void ReadStatusPage_orders_by_tree_then_peer_then_direction()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordInboundSuccess("orders", "east");
        stats.RecordSuccess("orders", "west");
        stats.RecordSuccess("audit", "west");
        stats.RecordSuccess("orders", "east");

        var keys = stats.ReadStatusPage(Read()).Select(r => (r.Tree, r.Peer, r.Direction)).ToArray();

        Assert.That(keys, Is.EqualTo(new[]
        {
            ("audit", "west", ReplicationContactDirection.Outbound),
            ("orders", "east", ReplicationContactDirection.Outbound),
            ("orders", "east", ReplicationContactDirection.Inbound),
            ("orders", "west", ReplicationContactDirection.Outbound),
        }));
    }

    [Test]
    public void ReadStatusPage_filters_by_tree_and_peer()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordSuccess("orders", "east");
        stats.RecordSuccess("orders", "west");
        stats.RecordSuccess("audit", "east");

        var byTree = stats.ReadStatusPage(Read(tree: "orders")).Select(r => r.Peer).ToArray();
        var byPeer = stats.ReadStatusPage(Read(peer: "east")).Select(r => r.Tree).ToArray();
        var byBoth = stats.ReadStatusPage(Read(tree: "audit", peer: "west"));

        Assert.Multiple(() =>
        {
            Assert.That(byTree, Is.EqualTo(new[] { "east", "west" }));
            Assert.That(byPeer, Is.EqualTo(new[] { "audit", "orders" }));
            Assert.That(byBoth, Is.Empty);
        });
    }

    [Test]
    public void ReadStatusPage_returns_the_first_rows_in_order_up_to_the_limit()
    {
        var stats = new ManualClockPeerStats();
        foreach (var tree in new[] { "e", "c", "a", "d", "b" })
        {
            stats.RecordSuccess(tree, "peer");
        }

        var trees = stats.ReadStatusPage(Read(limit: 3)).Select(r => r.Tree).ToArray();

        Assert.That(trees, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void ReadStatusPage_resumes_strictly_after_the_cursor()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordSuccess("a", "p");
        stats.RecordSuccess("b", "p");
        stats.RecordInboundSuccess("b", "p");
        stats.RecordSuccess("c", "p");

        var after = new ReplicationPeerStatusCursor("b", Stripped: false, "p", ReplicationContactDirection.Outbound);
        var keys = stats.ReadStatusPage(Read(after: after)).Select(r => (r.Tree, r.Direction)).ToArray();

        Assert.That(keys, Is.EqualTo(new[]
        {
            ("b", ReplicationContactDirection.Inbound),
            ("c", ReplicationContactDirection.Outbound),
        }));
    }

    [Test]
    public void ReadStatusPage_paging_visits_every_row_exactly_once()
    {
        var stats = new ManualClockPeerStats();
        var expected = new List<(string, string, ReplicationContactDirection)>();
        foreach (var tree in new[] { "t3", "t1", "t2" })
        {
            foreach (var peer in new[] { "y", "x" })
            {
                stats.RecordSuccess(tree, peer);
                expected.Add((tree, peer, ReplicationContactDirection.Outbound));
            }
        }

        stats.RecordInboundSuccess("t1", "x");
        expected.Add(("t1", "x", ReplicationContactDirection.Inbound));

        var seen = new List<(string, string, ReplicationContactDirection)>();
        ReplicationPeerStatusCursor? after = null;
        while (true)
        {
            var page = stats.ReadStatusPage(Read(limit: 2, after: after));
            seen.AddRange(page.Select(r => (r.Tree, r.Peer, r.Direction)));
            if (page.Length < 2)
            {
                break;
            }

            after = ReplicationPeerStatusOrder.CursorAfter(page[^1], stripPrefix: null);
        }

        Assert.That(seen, Is.EquivalentTo(expected));
        Assert.That(seen, Is.Unique);
    }

    [Test]
    public void ReadStatusPage_orders_on_the_display_id_the_caller_is_shown()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordSuccess("t/acme/a/crm/contacts", "east");
        stats.RecordSuccess("b-global", "east");
        stats.RecordSuccess("t/other/zeta", "east");

        var trees = stats.ReadStatusPage(Read(strip: "t/acme/")).Select(r => r.Tree).ToArray();

        // Effective ids come back unchanged; only their order follows the display
        // id ("a/crm/contacts" < "b-global" < "t/other/zeta").
        Assert.That(trees, Is.EqualTo(new[] { "t/acme/a/crm/contacts", "b-global", "t/other/zeta" }));
    }

    [Test]
    public void ReadStatusPage_orders_a_tenants_own_tree_before_a_bare_tree_of_the_same_name()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordSuccess("orders", "east");
        stats.RecordSuccess("t/acme/orders", "east");

        var trees = stats.ReadStatusPage(Read(strip: "t/acme/")).Select(r => r.Tree).ToArray();
        var afterOwn = stats.ReadStatusPage(Read(
            strip: "t/acme/",
            after: new ReplicationPeerStatusCursor("orders", Stripped: true, "east", ReplicationContactDirection.Outbound)));

        Assert.Multiple(() =>
        {
            Assert.That(trees, Is.EqualTo(new[] { "t/acme/orders", "orders" }));
            Assert.That(afterOwn.Select(r => r.Tree), Is.EqualTo(new[] { "orders" }));
        });
    }

    [Test]
    public void ReadStatusPage_clamps_a_non_positive_limit_to_one_row()
    {
        var stats = new ManualClockPeerStats();
        stats.RecordSuccess("a", "p");
        stats.RecordSuccess("b", "p");

        Assert.That(stats.ReadStatusPage(Read(limit: 0)), Has.Length.EqualTo(1));
    }

    [Test]
    public void ReadStatusPage_on_empty_state_returns_no_rows()
    {
        Assert.That(new ManualClockPeerStats().ReadStatusPage(Read()), Is.Empty);
    }

    [Test]
    public void ReadStatusPage_null_request_throws()
    {
        Assert.That(() => new ManualClockPeerStats().ReadStatusPage(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void EffectiveLimit_clamps_to_one_and_the_maximum()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new ReplicationPeerStatusReadRequest { Limit = -5 }.EffectiveLimit, Is.EqualTo(1));
            Assert.That(new ReplicationPeerStatusReadRequest { Limit = 42 }.EffectiveLimit, Is.EqualTo(42));
            Assert.That(
                new ReplicationPeerStatusReadRequest { Limit = int.MaxValue }.EffectiveLimit,
                Is.EqualTo(ReplicationPeerStatusReadRequest.MaxLimit));
        });
    }
}
