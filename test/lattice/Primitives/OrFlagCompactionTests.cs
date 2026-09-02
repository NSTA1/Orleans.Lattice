namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence and bounded-state guards for the shared dot-history compaction
/// (<see cref="OrSetDotCompaction"/>) as applied to <see cref="OrFlag"/>.
/// <para>
/// The defect these guard (issue #1932) was that re-asserting a slot appended a
/// dot forever, so a presence flag re-enabled on a schedule grew without bound
/// until reads, merges, and serialisation of that one row blew a grain response
/// deadline. Compaction is only sound because cancellation became
/// coverage-based, so these fixtures pin both halves: that state stays bounded,
/// <b>and</b> that add-wins convergence is unchanged.
/// </para>
/// </summary>
[TestFixture]
public class OrFlagCompactionTests
{
    /// <summary>
    /// Mirrors how <c>OrFlagAccessor</c> mints a real enable: the counter is one
    /// past the highest this replica has used anywhere in the flag's history.
    /// Tests that hand-pick counters would not exercise the accessor's contract.
    /// </summary>
    private static void EnableAsAccessorWould(OrFlag flag, string replicaId)
    {
        long max = 0;
        foreach (var dot in flag.Enables)
        {
            if (dot.ReplicaId == replicaId && dot.Counter > max) max = dot.Counter;
        }

        foreach (var dot in flag.Tombstones)
        {
            if (dot.ReplicaId == replicaId && dot.Counter > max) max = dot.Counter;
        }

        flag.Enable(replicaId, max + 1);
    }

    [Test]
    public void Repeated_enable_from_one_replica_keeps_exactly_one_dot()
    {
        var flag = new OrFlag();
        for (var i = 0; i < 1000; i++)
        {
            EnableAsAccessorWould(flag, "local");
        }

        Assert.Multiple(() =>
        {
            Assert.That(flag.Enables, Has.Count.EqualTo(1),
                "A replica re-asserting the same flag holds one dot, not one per assertion. "
                + "This is the exact growth that livelocked the repocontext membership tree.");
            Assert.That(flag.Enables[0].Counter, Is.EqualTo(1000),
                "The surviving dot is the newest, so a later disable tombstones the whole history.");
            Assert.That(flag.IsEnabled, Is.True);
        });
    }

    [Test]
    public void State_stays_bounded_by_replica_count_not_assertion_count()
    {
        var flag = new OrFlag();
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
    public void Enable_concurrent_with_a_disable_elsewhere_still_wins()
    {
        // The load-bearing add-wins case, and the one a naive "drop the old dot"
        // compaction would silently break.
        var authored = new OrFlag();
        EnableAsAccessorWould(authored, "A");

        // A peer replicates that state, then disables based on what it observed.
        var peer = Clone(authored);
        peer.Disable();

        // Concurrently, and without observing the disable, A re-enables.
        EnableAsAccessorWould(authored, "A");

        var merged = Clone(authored);
        merged.MergeFrom(peer);

        Assert.Multiple(() =>
        {
            Assert.That(merged.IsEnabled, Is.True,
                "An enable concurrent with a disable it never observed wins: add-wins is preserved "
                + "even though A's superseded dot was compacted away.");
            Assert.That(Clone(peer) is var reversed && Merge(peer, authored).IsEnabled, Is.True,
                "And the merge is order-independent.");
        });
    }

    [Test]
    public void Disable_after_a_compacted_reenable_still_disables_every_replica_copy()
    {
        // The case that proves coverage-based cancellation is required. A peer
        // still holds the dot the author compacted away; the author's disable
        // must cancel it, or the flag would resurrect on merge.
        var author = new OrFlag();
        EnableAsAccessorWould(author, "A");
        var peerHoldingOldDot = Clone(author);

        EnableAsAccessorWould(author, "A");
        author.Disable();

        var merged = Clone(peerHoldingOldDot);
        merged.MergeFrom(author);

        Assert.Multiple(() =>
        {
            Assert.That(author.IsEnabled, Is.False);
            Assert.That(merged.IsEnabled, Is.False,
                "The disable covers the peer's older dot from the same replica, so the flag stays off. "
                + "With exact-match cancellation the stale dot would survive and wrongly re-enable it.");
        });
    }

    [Test]
    public void Merge_is_commutative_associative_and_idempotent()
    {
        var a = new OrFlag();
        EnableAsAccessorWould(a, "A");
        EnableAsAccessorWould(a, "A");

        var b = new OrFlag();
        EnableAsAccessorWould(b, "B");
        b.Disable();

        var c = new OrFlag();
        EnableAsAccessorWould(c, "C");

        Assert.Multiple(() =>
        {
            Assert.That(Merge(Merge(a, b), c).IsEnabled, Is.EqualTo(Merge(a, Merge(b, c)).IsEnabled),
                "associative");
            Assert.That(Merge(a, b).IsEnabled, Is.EqualTo(Merge(b, a).IsEnabled), "commutative");

            var once = Merge(a, b);
            var twice = Merge(once, b);
            Assert.That(twice.IsEnabled, Is.EqualTo(once.IsEnabled), "idempotent");
            Assert.That(twice.Enables, Has.Count.EqualTo(once.Enables.Count),
                "Re-merging the same state adds no dots.");
        });
    }

    [Test]
    public void Folding_a_replicated_delta_repeatedly_does_not_grow_state()
    {
        // Duplicate delivery is normal on the replication path, and a delta fold
        // is also how write-ahead-log replay rebuilds a row.
        var flag = new OrFlag();
        var delta = new OrFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "peer", Counter = 7 } },
            Disables = Array.Empty<OrSetDot>(),
        };

        for (var i = 0; i < 50; i++)
        {
            flag.MergeDelta(delta);
        }

        Assert.Multiple(() =>
        {
            Assert.That(flag.Enables, Has.Count.EqualTo(1));
            Assert.That(flag.IsEnabled, Is.True);
        });
    }

    [Test]
    public void An_already_bloated_history_heals_on_the_next_merge()
    {
        // Self-healing: state written by a build without compaction collapses
        // the first time anything merges into it, with no migration step. This
        // is what lets the fix ship as a patch release and repair a live volume.
        var legacy = new OrFlag();
        for (var i = 1; i <= 500; i++)
        {
            legacy.Enables.Add(new OrSetDot { ReplicaId = "local", Counter = i });
        }

        Assume.That(legacy.Enables, Has.Count.EqualTo(500), "arranged as a pre-fix row");

        legacy.MergeFrom(new OrFlag());

        Assert.Multiple(() =>
        {
            Assert.That(legacy.Enables, Has.Count.EqualTo(1),
                "A merge normalises an inherited unbounded history without an operator step.");
            Assert.That(legacy.Enables[0].Counter, Is.EqualTo(500));
            Assert.That(legacy.IsEnabled, Is.True, "Healing never changes the observable value.");
        });
    }

    [Test]
    public void A_disabled_flag_keeps_one_dot_per_side_so_history_stays_readable()
    {
        var flag = new OrFlag();
        EnableAsAccessorWould(flag, "A");
        flag.Disable();

        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.False);
            Assert.That(flag.Tombstones, Has.Count.EqualTo(1),
                "The tombstone is retained: a peer still holding the enable needs it to converge.");
            Assert.That(flag.Enables, Has.Count.EqualTo(1),
                "The cancelled enable dot is retained too, so the durable per-key history view can "
                + "still decode the enable-then-disable pair as two events rather than one.");
        });
    }

    [Test]
    public void Reenabling_after_a_disable_turns_the_flag_back_on()
    {
        var flag = new OrFlag();
        EnableAsAccessorWould(flag, "A");
        flag.Disable();
        Assume.That(flag.IsEnabled, Is.False);

        EnableAsAccessorWould(flag, "A");

        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.True,
                "The new dot outranks the tombstone, so the flag is live again.");
            Assert.That(flag.Enables, Has.Count.EqualTo(1));
        });
    }

    private static OrFlag Clone(OrFlag source) => source.Clone();

    private static OrFlag Merge(OrFlag left, OrFlag right) => OrFlag.Merge(left, right);
}
