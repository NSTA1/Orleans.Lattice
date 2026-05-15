using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

public class LwwValueTests
{
    [Test]
    public void Merge_keeps_value_with_higher_timestamp()
    {
        var older = LwwValue<string>.Create("old", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var newer = LwwValue<string>.Create("new", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        Assert.That(LwwValue<string>.Merge(older, newer).Value, Is.EqualTo("new"));
        Assert.That(LwwValue<string>.Merge(newer, older).Value, Is.EqualTo("new"));
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = LwwValue<int>.Create(1, new HybridLogicalClock { WallClockTicks = 10, Counter = 0 });
        var b = LwwValue<int>.Create(2, new HybridLogicalClock { WallClockTicks = 20, Counter = 0 });

        Assert.That(LwwValue<int>.Merge(b, a), Is.EqualTo(LwwValue<int>.Merge(a, b)));
    }

    [Test]
    public void Merge_is_commutative_when_timestamps_tie_across_distinct_origins()
    {
        // Two replicas authoring concurrent writes at the same HLC. This is the
        // common case at startup when both replicas Tick() from
        // HybridLogicalClock.Zero in the same UTC tick. A CRDT-correct LWW must
        // resolve the tie deterministically so both replicas converge regardless
        // of which order the writes arrive in.
        var tie = new HybridLogicalClock { WallClockTicks = 12345, Counter = 7 };
        var fromA = LwwValue<string>.Create("from-A", tie) with { OriginClusterId = "cluster-a" };
        var fromB = LwwValue<string>.Create("from-B", tie) with { OriginClusterId = "cluster-b" };

        var leftFirst = LwwValue<string>.Merge(fromA, fromB);
        var rightFirst = LwwValue<string>.Merge(fromB, fromA);

        Assert.That(leftFirst, Is.EqualTo(rightFirst),
            "Merge must be commutative on HLC ties; otherwise replicas diverge silently.");
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 5, Counter = 0 });
        Assert.That(LwwValue<string>.Merge(v, v), Is.EqualTo(v));
    }

    [Test]
    public void Tombstone_wins_when_timestamp_is_higher()
    {
        var live = LwwValue<string>.Create("alive", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var dead = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        var result = LwwValue<string>.Merge(live, dead);
        Assert.That(result.IsTombstone, Is.True);
    }

    [Test]
    public void Live_value_wins_when_timestamp_is_higher_than_tombstone()
    {
        var dead = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var live = LwwValue<string>.Create("resurrected", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        var result = LwwValue<string>.Merge(dead, live);
        Assert.That(result.IsTombstone, Is.False);
        Assert.That(result.Value, Is.EqualTo("resurrected"));
    }

    // --- TTL / ExpiresAtTicks ---

    [Test]
    public void Create_defaults_ExpiresAtTicks_to_zero()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(v.ExpiresAtTicks, Is.EqualTo(0L));
    }

    [Test]
    public void CreateWithExpiry_sets_ExpiresAtTicks()
    {
        var v = LwwValue<string>.CreateWithExpiry(
            "x",
            new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            expiresAtTicks: 12345L);
        Assert.That(v.ExpiresAtTicks, Is.EqualTo(12345L));
        Assert.That(v.Value, Is.EqualTo("x"));
        Assert.That(v.IsTombstone, Is.False);
    }

    [Test]
    public void IsExpired_returns_false_for_non_expiring_entry()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(v.IsExpired(long.MaxValue), Is.False);
    }

    [Test]
    public void IsExpired_returns_true_when_now_passes_expiry()
    {
        var v = LwwValue<string>.CreateWithExpiry(
            "x",
            new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            expiresAtTicks: 1000L);
        Assert.That(v.IsExpired(999L), Is.False);
        Assert.That(v.IsExpired(1000L), Is.True);
        Assert.That(v.IsExpired(1001L), Is.True);
    }

    [Test]
    public void IsExpired_returns_false_for_tombstones_even_with_expiry_set()
    {
        // Tombstones are already "deleted" - expiry is only meaningful for live values.
        var tomb = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(tomb.IsExpired(long.MaxValue), Is.False);
    }

    [Test]
    public void Merge_preserves_expiry_of_winning_value()
    {
        var older = LwwValue<string>.Create("old", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var newerWithExpiry = LwwValue<string>.CreateWithExpiry(
            "new",
            new HybridLogicalClock { WallClockTicks = 2, Counter = 0 },
            expiresAtTicks: 999L);

        var winner = LwwValue<string>.Merge(older, newerWithExpiry);
        Assert.That(winner.Value, Is.EqualTo("new"));
        Assert.That(winner.ExpiresAtTicks, Is.EqualTo(999L));
    }

    // --- OriginClusterId ---

    [Test]
    public void Create_defaults_OriginClusterId_to_null()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(v.OriginClusterId, Is.Null);
    }

    [Test]
    public void With_expression_sets_OriginClusterId()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { OriginClusterId = "cluster-east" };
        Assert.That(v.OriginClusterId, Is.EqualTo("cluster-east"));
        Assert.That(v.Value, Is.EqualTo("x"));
        Assert.That(v.IsTombstone, Is.False);
    }

    [Test]
    public void Tombstone_can_carry_OriginClusterId()
    {
        var t = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { OriginClusterId = "peer-a" };
        Assert.That(t.IsTombstone, Is.True);
        Assert.That(t.OriginClusterId, Is.EqualTo("peer-a"));
    }

    [Test]
    public void Merge_preserves_OriginClusterId_of_winning_value()
    {
        var older = LwwValue<string>.Create("old", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { OriginClusterId = "cluster-old" };
        var newer = LwwValue<string>.Create("new", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 })
            with { OriginClusterId = "cluster-new" };

        var winner = LwwValue<string>.Merge(older, newer);
        Assert.That(winner.Value, Is.EqualTo("new"));
        Assert.That(winner.OriginClusterId, Is.EqualTo("cluster-new"));
    }

    // --- VectorClock ---

    [Test]
    public void Create_defaults_VectorClock_to_null()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(v.VectorClock, Is.Null);
    }

    [Test]
    public void With_expression_sets_VectorClock()
    {
        var vc = new VersionVector();
        vc.Tick("east");
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { VectorClock = vc };
        Assert.That(v.VectorClock, Is.SameAs(vc));
    }

    [Test]
    public void Tombstone_can_carry_VectorClock()
    {
        var vc = new VersionVector();
        vc.Tick("east");
        var t = LwwValue<string>.Tombstone(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { VectorClock = vc };
        Assert.That(t.IsTombstone, Is.True);
        Assert.That(t.VectorClock, Is.SameAs(vc));
    }

    [Test]
    public void Merge_preserves_VectorClock_of_winning_value()
    {
        var olderVc = new VersionVector();
        olderVc.Tick("a");
        var newerVc = new VersionVector();
        newerVc.Tick("b");

        var older = LwwValue<string>.Create("old", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { VectorClock = olderVc };
        var newer = LwwValue<string>.Create("new", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 })
            with { VectorClock = newerVc };

        var winner = LwwValue<string>.Merge(older, newer);
        Assert.That(winner.Value, Is.EqualTo("new"));
        Assert.That(winner.VectorClock, Is.SameAs(newerVc));
    }

    [Test]
    public void IsMigrated_defaults_to_false()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        Assert.That(v.IsMigrated, Is.False);
    }

    [Test]
    public void With_expression_sets_IsMigrated()
    {
        var v = LwwValue<string>.Create("x", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { IsMigrated = true };
        Assert.That(v.IsMigrated, Is.True);
    }

    [Test]
    public void Merge_preserves_IsMigrated_flag_of_winning_value_when_winner_is_migrated()
    {
        // Migrated entry has the higher HLC (the realistic shape - a cross-leaf
        // migration imports the source's high cumulative HLC and wins the merge
        // against a local low-HLC entry).
        var local = LwwValue<string>.Create("local", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });
        var migrated = LwwValue<string>.Create("migrated", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 })
            with { IsMigrated = true };

        var winner = LwwValue<string>.Merge(local, migrated);
        Assert.That(winner.Value, Is.EqualTo("migrated"));
        Assert.That(winner.IsMigrated, Is.True, "Winner is the migrated value; its IsMigrated flag must ride through.");
    }

    [Test]
    public void Merge_preserves_IsMigrated_flag_of_winning_value_when_winner_is_local()
    {
        // Foreground commit at a higher HLC than the existing migrated entry -
        // the foreground value wins and its IsMigrated=false naturally clears
        // the marker (this is the mechanism by which non-migration writes
        // "clear" stale migration provenance under Option A).
        var migrated = LwwValue<string>.Create("migrated", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { IsMigrated = true };
        var local = LwwValue<string>.Create("local", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 });

        var winner = LwwValue<string>.Merge(migrated, local);
        Assert.That(winner.Value, Is.EqualTo("local"));
        Assert.That(winner.IsMigrated, Is.False, "Winner is the foreground value; its default IsMigrated=false replaces the prior marker.");
    }

    [Test]
    public void Merge_is_commutative_with_respect_to_IsMigrated()
    {
        // Order-independence: feeding (a,b) and (b,a) must produce the same
        // winner including the IsMigrated bit, otherwise replicas could diverge
        // on the provenance signal.
        var migrated = LwwValue<string>.Create("migrated", new HybridLogicalClock { WallClockTicks = 2, Counter = 0 })
            with { IsMigrated = true };
        var local = LwwValue<string>.Create("local", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 });

        var leftFirst = LwwValue<string>.Merge(migrated, local);
        var rightFirst = LwwValue<string>.Merge(local, migrated);

        Assert.That(leftFirst, Is.EqualTo(rightFirst));
        Assert.That(leftFirst.IsMigrated, Is.True);
        Assert.That(rightFirst.IsMigrated, Is.True);
    }

    [Test]
    public void Merge_is_idempotent_with_respect_to_IsMigrated()
    {
        var migrated = LwwValue<string>.Create("migrated", new HybridLogicalClock { WallClockTicks = 1, Counter = 0 })
            with { IsMigrated = true };

        var self = LwwValue<string>.Merge(migrated, migrated);
        Assert.That(self, Is.EqualTo(migrated));
        Assert.That(self.IsMigrated, Is.True);
    }
}
