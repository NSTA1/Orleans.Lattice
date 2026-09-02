namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence and bounded-state guards for dot-history compaction as applied to <see cref="RwFlag"/>.
/// </summary>
[TestFixture]
public class RwFlagCompactionTests
{
    private static void EnableAsAccessorWould(RwFlag flag, string replicaId)
        => flag.Enable(replicaId, NextCounter(flag, replicaId));

    private static void DisableAsAccessorWould(RwFlag flag, string replicaId)
        => flag.Disable(replicaId, NextCounter(flag, replicaId));

    [Test]
    public void Repeated_enable_and_disable_from_one_replica_keep_exactly_one_dot_per_side()
    {
        var enables = new RwFlag();
        for (var i = 0; i < 1000; i++) EnableAsAccessorWould(enables, "local");

        var disables = new RwFlag();
        for (var i = 0; i < 1000; i++) DisableAsAccessorWould(disables, "local");

        Assert.Multiple(() =>
        {
            Assert.That(enables.Enables, Has.Count.EqualTo(1),
                "A replica re-enabling the same flag holds one enable dot, not one per assertion.");
            Assert.That(enables.Enables[0].Counter, Is.EqualTo(1000),
                "The surviving enable dot is the newest from that replica.");
            Assert.That(disables.Disables, Has.Count.EqualTo(1),
                "A replica re-disabling the same flag holds one disable dot, not one per assertion.");
            Assert.That(disables.Disables[0].Counter, Is.EqualTo(1000),
                "The surviving disable dot is the newest from that replica.");
        });
    }

    [Test]
    public void State_stays_bounded_by_replica_count_not_assertion_count()
    {
        var flag = new RwFlag();
        for (var round = 0; round < 200; round++)
        {
            EnableAsAccessorWould(flag, "A");
            EnableAsAccessorWould(flag, "B");
            EnableAsAccessorWould(flag, "C");
        }

        Assert.That(flag.Enables, Has.Count.EqualTo(3),
            "600 assertions across 3 replicas cost 3 dots: state is O(replicas), not O(assertions).");
    }

    [Test]
    public void Enable_concurrent_with_disable_elsewhere_still_loses()
    {
        var remover = new RwFlag();
        EnableAsAccessorWould(remover, "A");
        DisableAsAccessorWould(remover, "A");

        var adder = new RwFlag();
        EnableAsAccessorWould(adder, "B");

        Assert.Multiple(() =>
        {
            Assert.That(RwFlag.Merge(remover, adder).IsEnabled, Is.False,
                "A disable concurrent with an enable it never observed wins: remove-wins is preserved.");
            Assert.That(RwFlag.Merge(adder, remover).IsEnabled, Is.False,
                "The remove-wins tie-break is order-independent.");
        });
    }

    [Test]
    public void Enable_after_a_compacted_disable_still_tombstones_peer_old_disable()
    {
        var author = new RwFlag();
        DisableAsAccessorWould(author, "A");
        var peerHoldingOldDisable = author.Clone();

        DisableAsAccessorWould(author, "A");
        EnableAsAccessorWould(author, "A");

        var merged = peerHoldingOldDisable.Clone();
        merged.MergeFrom(author);

        Assert.Multiple(() =>
        {
            Assert.That(author.IsEnabled, Is.True);
            Assert.That(merged.IsEnabled, Is.True,
                "The enable tombstone covers the peer's older disable from the same replica, so no stale disable suppresses it.");
        });
    }

    [Test]
    public void Merge_is_commutative_associative_and_idempotent()
    {
        var a = new RwFlag();
        EnableAsAccessorWould(a, "A");
        EnableAsAccessorWould(a, "A");

        var b = new RwFlag();
        EnableAsAccessorWould(b, "B");
        DisableAsAccessorWould(b, "B");

        var c = new RwFlag();
        EnableAsAccessorWould(c, "C");

        var left = RwFlag.Merge(RwFlag.Merge(a, b), c);
        var right = RwFlag.Merge(a, RwFlag.Merge(b, c));
        var once = RwFlag.Merge(a, b);
        var twice = RwFlag.Merge(once, b);

        Assert.Multiple(() =>
        {
            Assert.That(left.IsEnabled, Is.EqualTo(right.IsEnabled), "associative");
            Assert.That(RwFlag.Merge(a, b).IsEnabled, Is.EqualTo(RwFlag.Merge(b, a).IsEnabled), "commutative");
            Assert.That(twice.IsEnabled, Is.EqualTo(once.IsEnabled), "idempotent");
            Assert.That(twice.Enables, Has.Count.EqualTo(once.Enables.Count),
                "Re-merging the same state adds no enable dots.");
            Assert.That(twice.Disables, Has.Count.EqualTo(once.Disables.Count),
                "Re-merging the same state adds no disable dots.");
        });
    }

    [Test]
    public void Folding_a_replicated_delta_repeatedly_does_not_grow_state()
    {
        var flag = new RwFlag();
        var delta = new RwFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "peer", Counter = 7 } },
            Disables = new[] { new OrSetDot { ReplicaId = "peer", Counter = 8 } },
            Tombstones = Array.Empty<OrSetDot>(),
        };

        for (var i = 0; i < 50; i++) flag.MergeDelta(delta);

        Assert.Multiple(() =>
        {
            Assert.That(flag.Enables, Has.Count.EqualTo(1));
            Assert.That(flag.Disables, Has.Count.EqualTo(1));
            Assert.That(flag.IsEnabled, Is.False, "The live disable still wins after duplicate delta folds.");
        });
    }

    [Test]
    public void An_already_bloated_history_heals_on_the_next_merge()
    {
        var legacy = new RwFlag();
        for (var i = 1; i <= 500; i++)
        {
            legacy.Enables.Add(new OrSetDot { ReplicaId = "local", Counter = i });
        }

        Assume.That(legacy.Enables, Has.Count.EqualTo(500), "arranged as a pre-fix row");

        legacy.MergeFrom(new RwFlag());

        Assert.Multiple(() =>
        {
            Assert.That(legacy.Enables, Has.Count.EqualTo(1),
                "A merge normalises an inherited unbounded history without an operator step.");
            Assert.That(legacy.Enables[0].Counter, Is.EqualTo(500));
            Assert.That(legacy.IsEnabled, Is.True, "Healing never changes the observable value.");
        });
    }

    private static long NextCounter(RwFlag flag, string replicaId)
    {
        long max = 0;
        Max(flag.Enables, replicaId, ref max);
        Max(flag.Disables, replicaId, ref max);
        Max(flag.Tombstones, replicaId, ref max);
        return max + 1;
    }

    private static void Max(List<OrSetDot> dots, string replicaId, ref long max)
    {
        foreach (var dot in dots)
        {
            if (dot.ReplicaId == replicaId && dot.Counter > max) max = dot.Counter;
        }
    }
}
