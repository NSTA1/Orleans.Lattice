namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Unit tests for <see cref="OrSetDotCompaction"/>, the shared dot-history
/// machinery every observed-remove primitive compacts through (issue #1932).
/// <para>
/// These pin the helper's own contract in isolation - the per-primitive
/// fixtures then pin that each CRDT's convergence survives being built on it.
/// </para>
/// </summary>
[TestFixture]
public class OrSetDotCompactionTests
{
    private static OrSetDot Dot(string replicaId, long counter)
        => new() { ReplicaId = replicaId, Counter = counter };

    private static List<OrSetDot> Dots(params (string Replica, long Counter)[] dots)
    {
        var list = new List<OrSetDot>(dots.Length);
        foreach (var (replica, counter) in dots)
        {
            list.Add(Dot(replica, counter));
        }

        return list;
    }

    [Test]
    public void Covers_matches_an_equal_or_higher_counter_from_the_same_replica()
    {
        var cover = Dots(("A", 5));

        Assert.Multiple(() =>
        {
            Assert.That(OrSetDotCompaction.Covers(cover, Dot("A", 5)), Is.True, "equal counter cancels");
            Assert.That(OrSetDotCompaction.Covers(cover, Dot("A", 3)), Is.True,
                "a lower counter from the same replica is cancelled too - this is what lets a superseded "
                + "dot be compacted away without a peer's copy escaping cancellation");
            Assert.That(OrSetDotCompaction.Covers(cover, Dot("A", 6)), Is.False,
                "a newer assertion outranks the cancellation, so the slot comes back");
        });
    }

    [Test]
    public void Covers_never_crosses_replicas()
    {
        var cover = Dots(("A", 99));

        Assert.That(OrSetDotCompaction.Covers(cover, Dot("B", 1)), Is.False,
            "Cancelling one replica's assertion must never cancel another's - that is exactly the "
            + "concurrency the dot context exists to preserve.");
    }

    [Test]
    public void Covers_is_false_against_an_empty_cover()
    {
        Assert.That(OrSetDotCompaction.Covers(new List<OrSetDot>(), Dot("A", 1)), Is.False);
    }

    [Test]
    public void CompactMaxPerReplica_keeps_the_highest_counter_per_replica()
    {
        var dots = Dots(("A", 1), ("B", 7), ("A", 4), ("B", 2), ("A", 3));

        var changed = OrSetDotCompaction.CompactMaxPerReplica(dots);

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.True);
            Assert.That(dots, Has.Count.EqualTo(2));
            Assert.That(dots.Single(d => d.ReplicaId == "A").Counter, Is.EqualTo(4));
            Assert.That(dots.Single(d => d.ReplicaId == "B").Counter, Is.EqualTo(7));
        });
    }

    [Test]
    public void CompactMaxPerReplica_preserves_first_seen_replica_order()
    {
        var dots = Dots(("B", 1), ("A", 1), ("B", 9));

        OrSetDotCompaction.CompactMaxPerReplica(dots);

        Assert.That(dots.Select(d => d.ReplicaId), Is.EqualTo(new[] { "B", "A" }),
            "Stable order keeps serialised state deterministic for a given history.");
    }

    [Test]
    public void CompactMaxPerReplica_is_idempotent_and_reports_no_change_when_already_normal()
    {
        var dots = Dots(("A", 4), ("B", 7));

        Assert.Multiple(() =>
        {
            Assert.That(OrSetDotCompaction.CompactMaxPerReplica(dots), Is.False,
                "An already-normalised list reports no change, so callers can skip follow-up work.");
            Assert.That(dots, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public void CompactMaxPerReplica_handles_empty_and_single_dot_lists()
    {
        var empty = new List<OrSetDot>();
        var single = Dots(("A", 1));

        Assert.Multiple(() =>
        {
            Assert.That(OrSetDotCompaction.CompactMaxPerReplica(empty), Is.False);
            Assert.That(empty, Is.Empty);
            Assert.That(OrSetDotCompaction.CompactMaxPerReplica(single), Is.False);
            Assert.That(single, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void CompactMaxPerReplica_collapses_a_long_single_replica_history()
    {
        // The exact shape that livelocked the repocontext membership tree.
        var dots = new List<OrSetDot>();
        for (var i = 1; i <= 5000; i++)
        {
            dots.Add(Dot("local", i));
        }

        OrSetDotCompaction.CompactMaxPerReplica(dots);

        Assert.Multiple(() =>
        {
            Assert.That(dots, Has.Count.EqualTo(1));
            Assert.That(dots[0].Counter, Is.EqualTo(5000));
        });
    }

    [Test]
    public void CompactMaxPerReplica_crosses_into_the_dictionary_path_above_the_scan_threshold()
    {
        // More distinct replicas than the in-place scan handles cheaply, each
        // asserting several times, so the dictionary tail is exercised.
        var dots = new List<OrSetDot>();
        for (var round = 1; round <= 4; round++)
        {
            for (var replica = 0; replica < 40; replica++)
            {
                dots.Add(Dot($"replica-{replica}", round));
            }
        }

        OrSetDotCompaction.CompactMaxPerReplica(dots);

        Assert.Multiple(() =>
        {
            Assert.That(dots, Has.Count.EqualTo(40), "one dot per replica regardless of assertion count");
            Assert.That(dots.All(d => d.Counter == 4), Is.True, "and it is each replica's newest");
            Assert.That(dots.Select(d => d.ReplicaId).Distinct().Count(), Is.EqualTo(40));
        });
    }

    [Test]
    public void CountLive_and_AnyLive_agree_with_the_coverage_predicate()
    {
        var dots = Dots(("A", 3), ("B", 2), ("C", 9));
        var cover = Dots(("A", 5), ("B", 1));

        Assert.Multiple(() =>
        {
            // A:3 covered by A:5. B:2 not covered by B:1. C:9 uncovered.
            Assert.That(OrSetDotCompaction.CountLive(dots, cover), Is.EqualTo(2));
            Assert.That(OrSetDotCompaction.AnyLive(dots, cover), Is.True);
        });
    }

    [Test]
    public void CountLive_and_AnyLive_report_nothing_live_when_every_dot_is_covered()
    {
        var dots = Dots(("A", 1), ("A", 2));
        var cover = Dots(("A", 2));

        Assert.Multiple(() =>
        {
            Assert.That(OrSetDotCompaction.CountLive(dots, cover), Is.Zero);
            Assert.That(OrSetDotCompaction.AnyLive(dots, cover), Is.False);
        });
    }

    [Test]
    public void CountLive_and_AnyLive_short_circuit_the_empty_cases()
    {
        var dots = Dots(("A", 1));
        var empty = new List<OrSetDot>();

        Assert.Multiple(() =>
        {
            Assert.That(OrSetDotCompaction.CountLive(dots, empty), Is.EqualTo(1),
                "Nothing cancelling means every dot is live.");
            Assert.That(OrSetDotCompaction.AnyLive(dots, empty), Is.True);
            Assert.That(OrSetDotCompaction.CountLive(empty, dots), Is.Zero);
            Assert.That(OrSetDotCompaction.AnyLive(empty, dots), Is.False);
        });
    }
}
