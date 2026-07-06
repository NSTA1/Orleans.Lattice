using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Tests for the per-replica counter cache (<see cref="OrMap{TKey, TValue}.Context"/>)
/// that lets <c>NextCounter</c> mint a fresh dot in O(1) instead of rescanning
/// every dot on every write. The cache must stay consistent across every
/// mutator and rebuild itself from the dots when a legacy payload deserializes
/// it as empty, without altering convergence semantics.
/// </summary>
[TestFixture]
public class OrMapContextTests
{
    private static long CounterFor(OrMap<string, PnCounter> map, string key, string replicaId)
    {
        var entry = map.Adds[key].Single(e => e.ReplicaId == replicaId);
        return entry.Counter;
    }

    [Test]
    public void Repeated_sets_on_one_replica_mint_distinct_monotonic_counters()
    {
        var m = new OrMap<string, PnCounter>();
        for (var i = 0; i < 50; i++)
        {
            m.Set($"key-{i}", "r1", new PnCounter());
        }

        var counters = m.Adds.Values.SelectMany(static es => es).Select(static e => e.Counter).ToArray();
        Assert.That(counters, Is.Unique);
        Assert.That(counters, Is.EquivalentTo(Enumerable.Range(1, 50).Select(static i => (long)i)));
        Assert.That(m.Context["r1"], Is.EqualTo(50));
    }

    [Test]
    public void Context_tracks_per_replica_maximum()
    {
        var m = new OrMap<string, PnCounter>();
        m.Set("a", "r1", new PnCounter());
        m.Set("b", "r1", new PnCounter());
        m.Set("c", "r2", new PnCounter());

        Assert.That(m.Context["r1"], Is.EqualTo(2));
        Assert.That(m.Context["r2"], Is.EqualTo(1));
    }

    [Test]
    public void Legacy_payload_with_empty_context_rebuilds_on_first_write()
    {
        // Simulate a payload persisted before the Context field existed:
        // dots are present but the cache deserialized empty.
        var m = new OrMap<string, PnCounter>();
        m.Set("a", "r1", new PnCounter());
        m.Set("b", "r1", new PnCounter());
        m.Set("c", "r1", new PnCounter());
        m.Context.Clear();

        m.Set("d", "r1", new PnCounter());

        // The new dot must not collide with any existing r1 dot (1..3) -
        // it must be 4, proving the cache was rebuilt from the dots.
        Assert.That(CounterFor(m, "d", "r1"), Is.EqualTo(4));
        Assert.That(m.Adds.Values.SelectMany(static es => es).Select(static e => e.Counter), Is.Unique);
    }

    [Test]
    public void Legacy_payload_rebuild_accounts_for_tombstoned_dots()
    {
        var m = new OrMap<string, PnCounter>();
        m.Set("a", "r1", new PnCounter()); // counter 1
        m.Set("b", "r1", new PnCounter()); // counter 2
        m.Remove("b");                     // tombstones dot (r1, 2)
        m.Context.Clear();

        m.Set("c", "r1", new PnCounter());

        // Highest observed counter is the tombstoned dot 2, so the next
        // dot must be 3 - the rebuild must scan tombstones as well.
        Assert.That(CounterFor(m, "c", "r1"), Is.EqualTo(3));
    }

    [Test]
    public void MergeFrom_folds_context_so_later_writes_do_not_collide()
    {
        var a = new OrMap<string, PnCounter>();
        a.Set("x", "r1", new PnCounter());

        var b = new OrMap<string, PnCounter>();
        b.Set("y", "r2", new PnCounter());
        b.Set("z", "r2", new PnCounter()); // r2 max is 2

        a.MergeFrom(b);

        Assert.That(a.Context["r1"], Is.EqualTo(1));
        Assert.That(a.Context["r2"], Is.EqualTo(2));

        // A subsequent r2 write on the receiver must continue r2's run.
        a.Set("w", "r2", new PnCounter());
        Assert.That(CounterFor(a, "w", "r2"), Is.EqualTo(3));
    }

    [Test]
    public void MergeFrom_from_legacy_other_derives_context_from_dots()
    {
        var a = new OrMap<string, PnCounter>();

        var b = new OrMap<string, PnCounter>();
        b.Set("y", "r2", new PnCounter());
        b.Set("z", "r2", new PnCounter());
        b.Context.Clear(); // legacy other: dots present, cache empty

        a.MergeFrom(b);

        Assert.That(a.Context["r2"], Is.EqualTo(2));
        a.Set("w", "r2", new PnCounter());
        Assert.That(CounterFor(a, "w", "r2"), Is.EqualTo(3));
    }

    [Test]
    public void MergeDelta_maintains_context()
    {
        var m = new OrMap<string, PnCounter>();
        var delta = new OrMapDelta<string, PnCounter>
        {
            Adds =
            [
                new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r9", Counter = 7, Value = new PnCounter() },
            ],
            Tombstones = [],
        };

        m.MergeDelta(delta);

        Assert.That(m.Context["r9"], Is.EqualTo(7));
        m.Set("k2", "r9", new PnCounter());
        Assert.That(CounterFor(m, "k2", "r9"), Is.EqualTo(8));
    }

    [Test]
    public void Clone_copies_context()
    {
        var m = new OrMap<string, PnCounter>();
        m.Set("a", "r1", new PnCounter());
        m.Set("b", "r1", new PnCounter());

        var clone = m.Clone();
        Assert.That(clone.Context, Is.EquivalentTo(m.Context));

        // The clone must continue r1's counter run independently.
        clone.Set("c", "r1", new PnCounter());
        Assert.That(CounterFor(clone, "c", "r1"), Is.EqualTo(3));
    }

    [Test]
    public void Context_survives_orleans_serialization_round_trip()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<OrMap<string, PnCounter>>>();

        var m = new OrMap<string, PnCounter>();
        m.Set("a", "r1", new PnCounter());
        m.Set("b", "r1", new PnCounter());

        var roundTripped = serializer.Deserialize(serializer.SerializeToArray(m));

        Assert.That(roundTripped.Context["r1"], Is.EqualTo(2));

        // The next write on the rehydrated map continues r1's run without a
        // full dot rescan and without colliding with the deserialized dots.
        roundTripped.Set("c", "r1", new PnCounter());
        Assert.That(CounterFor(roundTripped, "c", "r1"), Is.EqualTo(3));
    }
}
