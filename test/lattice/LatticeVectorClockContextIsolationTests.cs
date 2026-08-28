using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regressions for the ambient vector-clock frontier's ownership boundary.
/// <para>
/// A <see cref="VersionVector"/> is a mutable CRDT, and the frontier established
/// on <see cref="LatticeVectorClockContext"/> is stamped directly onto the
/// persisted <c>LwwValue&lt;T&gt;.VectorClock</c> of every entry written inside
/// the scope. On the inbound replication path the instance handed in arrives
/// inside an <c>[Immutable]</c> carrier, whose same-silo deep copy Orleans
/// <em>elides</em> - so without a defensive copy it is the co-located sender's
/// own object that becomes the durable state of many entries at once, and a
/// later mutation on either side silently rewrites committed frontiers. The
/// elision only happens under co-location, so no cross-silo test would show it.
/// </para>
/// <para>
/// The boundary is enforced at two seams and both are pinned here: the context
/// setter copies on the way in, and <see cref="LwwEntry"/> copies on the way out.
/// </para>
/// </summary>
[TestFixture]
public class LatticeVectorClockContextIsolationTests
{
    private static VersionVector Vector(string replicaId, long ticks) =>
        new() { Entries = { [replicaId] = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 } } };

    [TearDown]
    public void TearDown() => LatticeVectorClockContext.Current = null;

    [Test]
    public void Setting_the_frontier_copies_so_a_later_caller_mutation_does_not_leak_in()
    {
        var caller = Vector("A", 100);
        LatticeVectorClockContext.Current = caller;

        caller.Entries["B"] = new HybridLogicalClock { WallClockTicks = 999, Counter = 0 };

        Assert.That(LatticeVectorClockContext.Current!.Entries.ContainsKey("B"), Is.False,
            "the ambient frontier must not track mutations made to the caller's instance after the set");
    }

    [Test]
    public void Setting_the_frontier_does_not_store_the_caller_instance()
    {
        var caller = Vector("A", 100);
        LatticeVectorClockContext.Current = caller;

        Assert.That(ReferenceEquals(caller, LatticeVectorClockContext.Current), Is.False,
            "the stored frontier must be platform-owned, not the caller's object");
    }

    [Test]
    public void With_copies_so_the_caller_keeps_sole_ownership()
    {
        var caller = Vector("A", 100);

        using (LatticeVectorClockContext.With(caller))
        {
            caller.Entries["C"] = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
            Assert.That(LatticeVectorClockContext.Current!.Entries.ContainsKey("C"), Is.False);
        }

        Assert.That(LatticeVectorClockContext.Current, Is.Null);
    }

    [Test]
    public void The_stored_frontier_preserves_the_supplied_entries_by_value()
    {
        LatticeVectorClockContext.Current = Vector("A", 100);

        var stored = LatticeVectorClockContext.Current!;

        Assert.Multiple(() =>
        {
            Assert.That(stored.Entries.ContainsKey("A"), Is.True);
            Assert.That(stored.Entries["A"].WallClockTicks, Is.EqualTo(100));
        });
    }

    [Test]
    public void A_null_frontier_costs_nothing_and_clears_the_context()
    {
        LatticeVectorClockContext.Current = Vector("A", 1);
        LatticeVectorClockContext.Current = null;

        Assert.That(LatticeVectorClockContext.Current, Is.Null);
    }

    [Test]
    public void Nested_scopes_restore_the_outer_frontier()
    {
        using (LatticeVectorClockContext.With(Vector("outer", 1)))
        {
            using (LatticeVectorClockContext.With(Vector("inner", 2)))
            {
                Assert.That(LatticeVectorClockContext.Current!.Entries.ContainsKey("inner"), Is.True);
            }

            Assert.That(LatticeVectorClockContext.Current!.Entries.ContainsKey("outer"), Is.True,
                "disposing the inner scope must restore the outer frontier intact");
        }
    }

    [Test]
    public void LwwEntry_copies_the_frontier_on_egress_so_a_caller_cannot_write_into_stored_state()
    {
        var stored = new LwwValue<byte[]>
        {
            Value = [1],
            Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            VectorClock = Vector("A", 100),
        };

        var entry = new LwwEntry("k", stored);
        entry.VectorClock!.Entries["injected"] = new HybridLogicalClock { WallClockTicks = 7, Counter = 0 };

        Assert.That(stored.VectorClock!.Entries.ContainsKey("injected"), Is.False,
            "a caller mutating a returned entry's frontier must not reach the stored value");
    }

    [Test]
    public void LwwEntry_leaves_a_null_frontier_null_so_the_local_write_path_allocates_nothing()
    {
        var stored = new LwwValue<byte[]>
        {
            Value = [1],
            Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            VectorClock = null,
        };

        Assert.That(new LwwEntry("k", stored).VectorClock, Is.Null);
    }
}
