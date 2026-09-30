namespace Orleans.Lattice.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="ReplicationPeerStatusMerge"/>: cross-silo
/// de-duplication (which copy of a key survives), ordering, and truncation.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatusMergeTests
{
    private static ReplicationPeerStatusRow Row(
        string tree,
        double lastContactSeconds,
        long errors = 0,
        long entries = 0,
        string peer = "east",
        ReplicationContactDirection direction = ReplicationContactDirection.Outbound) =>
        new(tree, peer, direction, entries, entries * 10, errors, lastContactSeconds, InFlight: 0);

    [Test]
    public void Merge_keeps_the_copy_with_the_most_recent_contact_whole()
    {
        var live = Row("orders", lastContactSeconds: 2, errors: 0, entries: 5);
        var stale = Row("orders", lastContactSeconds: 400, errors: 9, entries: 900);

        var merged = ReplicationPeerStatusMerge.Merge(new[] { new[] { stale }, new[] { live } }, 10);

        Assert.That(merged, Is.EqualTo(new[] { live }));
    }

    [Test]
    public void Prefer_a_contacted_copy_over_one_never_contacted()
    {
        var contacted = Row("orders", lastContactSeconds: 1_000);
        var never = Row("orders", double.NaN, errors: 3);

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationPeerStatusMerge.Prefer(contacted, never), Is.EqualTo(contacted));
            Assert.That(ReplicationPeerStatusMerge.Prefer(never, contacted), Is.EqualTo(contacted));
        });
    }

    [Test]
    public void Prefer_among_copies_never_contacted_the_longest_error_streak_then_backlog()
    {
        var quiet = Row("orders", double.NaN, errors: 1, entries: 50);
        var failing = Row("orders", double.NaN, errors: 4, entries: 0);
        var backlogged = Row("orders", double.NaN, errors: 1, entries: 80);

        Assert.Multiple(() =>
        {
            Assert.That(ReplicationPeerStatusMerge.Prefer(quiet, failing), Is.EqualTo(failing));
            Assert.That(ReplicationPeerStatusMerge.Prefer(quiet, backlogged), Is.EqualTo(backlogged));
        });
    }

    [Test]
    public void Merge_keeps_distinct_directions_and_peers_apart()
    {
        var outbound = Row("orders", 1);
        var inbound = Row("orders", 1, direction: ReplicationContactDirection.Inbound);
        var otherPeer = Row("orders", 1, peer: "west");

        var merged = ReplicationPeerStatusMerge.Merge(
            new[] { new[] { outbound, inbound }, new[] { otherPeer, inbound } }, 10);

        Assert.That(merged, Is.EqualTo(new[] { outbound, inbound, otherPeer }));
    }

    [Test]
    public void Merge_orders_the_union_and_truncates_to_the_limit()
    {
        var merged = ReplicationPeerStatusMerge.Merge(
            new[]
            {
                new[] { Row("a", 1), Row("c", 1), Row("e", 1) },
                new[] { Row("b", 1), Row("d", 1) },
            },
            limit: 3);

        Assert.That(merged.Select(r => r.Tree), Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void Merge_orders_on_the_effective_tree_id()
    {
        var merged = ReplicationPeerStatusMerge.Merge(
            new[] { new[] { Row("t/acme/a", 1) }, new[] { Row("b", 1) } },
            limit: 10);

        Assert.That(merged.Select(r => r.Tree), Is.EqualTo(new[] { "b", "t/acme/a" }));
    }

    [Test]
    public void Merge_skips_a_null_answer_and_clamps_the_limit_to_one()
    {
        var merged = ReplicationPeerStatusMerge.Merge(
            new IReadOnlyList<ReplicationPeerStatusRow>[] { null!, new[] { Row("a", 1), Row("b", 1) } },
            limit: 0);

        Assert.That(merged.Select(r => r.Tree), Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public void Merge_null_answers_throws()
    {
        Assert.That(() => ReplicationPeerStatusMerge.Merge(null!, 10), Throws.ArgumentNullException);
    }
}
