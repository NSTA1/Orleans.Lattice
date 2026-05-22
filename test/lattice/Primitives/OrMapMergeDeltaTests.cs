using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class OrMapMergeDeltaTests
{
    [Test]
    public void MergeDelta_adds_entries_to_empty_map()
    {
        var m = new OrMap<string, PnCounter>();
        var pc = new PnCounter();
        pc.Increment("r1", 3);
        var delta = new OrMapDelta<string, PnCounter>
        {
            Adds = new[]
            {
                new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = pc },
            },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };

        m.MergeDelta(delta);

        Assert.That(m.Get("k")!.Value, Is.EqualTo(3));
    }

    [Test]
    public void MergeDelta_tombstone_cancels_matching_add()
    {
        var m = new OrMap<string, PnCounter>();
        var pc = new PnCounter(); pc.Increment("r1", 1);
        m.MergeDelta(new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = pc } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        });

        m.MergeDelta(new OrMapDelta<string, PnCounter>
        {
            Adds = Array.Empty<OrMapDeltaEntry<string, PnCounter>>(),
            Tombstones = new[] { new OrMapDeltaTombstone<string> { Key = "k", ReplicaId = "r1", Counter = 1 } },
        });

        Assert.That(m.Get("k"), Is.Null);
    }

    [Test]
    public void MergeDelta_is_idempotent_under_duplicate_delivery()
    {
        var m = new OrMap<string, PnCounter>();
        var pc = new PnCounter(); pc.Increment("r1", 2);
        var delta = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = pc } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };
        m.MergeDelta(delta);
        m.MergeDelta(delta);

        Assert.That(m.Get("k")!.Value, Is.EqualTo(2));
    }

    [Test]
    public void MergeDelta_with_null_collections_is_a_noop()
    {
        var m = new OrMap<string, PnCounter>();
        m.MergeDelta(default);
        Assert.That(m.IsEmpty, Is.True);
    }

    [Test]
    public void MergeDelta_concurrent_same_dot_values_are_merged_recursively()
    {
        var m = new OrMap<string, PnCounter>();
        var pcA = new PnCounter(); pcA.Increment("a", 1);
        var pcB = new PnCounter(); pcB.Increment("b", 4);
        m.MergeDelta(new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = pcA } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        });
        m.MergeDelta(new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = pcB } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        });

        // Same dot collapsed via MergeFrom: the value-CRDT merge unions both contributions.
        Assert.That(m.Get("k")!.Value, Is.EqualTo(5));
    }
}