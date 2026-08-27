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

    [Test]
    public void MergeDelta_when_a_second_delta_carries_the_same_dot_does_not_mutate_the_first_delta()
    {
        // The applier stored the delta's own TValue object under the new dot,
        // so the next same-dot apply folded into it in place - rewriting a
        // delta the caller may still hold for retry or fan-out.
        var v1 = new PnCounter(); v1.Increment("a", 5);
        var v2 = new PnCounter(); v2.Increment("a", 9);
        var d1 = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = v1 } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };
        var d2 = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = v2 } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };

        var replica = new OrMap<string, PnCounter>();
        replica.MergeDelta(d1);
        replica.MergeDelta(d2);

        Assert.That(v1.Value, Is.EqualTo(5),
            "MergeDelta must not retain the delta's value object: the next same-dot apply folded through the alias");
    }

    [Test]
    public void MergeDelta_when_redelivered_to_a_second_replica_yields_the_authored_value()
    {
        // The convergence proof: a late replica that applies only d1 must see
        // exactly what d1 authored, whatever another replica did with it.
        var v1 = new PnCounter(); v1.Increment("a", 5);
        var v2 = new PnCounter(); v2.Increment("a", 9);
        var d1 = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = v1 } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };
        var d2 = new OrMapDelta<string, PnCounter>
        {
            Adds = new[] { new OrMapDeltaEntry<string, PnCounter> { Key = "k", ReplicaId = "r1", Counter = 1, Value = v2 } },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        };

        var replicaA = new OrMap<string, PnCounter>();
        replicaA.MergeDelta(d1);
        replicaA.MergeDelta(d2);

        var replicaC = new OrMap<string, PnCounter>();
        replicaC.MergeDelta(d1);

        Assert.That(replicaC.Get("k")!.Value, Is.EqualTo(5),
            "a replica applying only the first delta must observe only what that delta authored");
    }

    [Test]
    public void MergeDelta_when_an_add_carries_no_replica_id_does_not_throw()
    {
        // The wire ingress has no guard where the local mutation APIs all call
        // ArgumentException.ThrowIfNullOrEmpty. BumpContext hashes the replica
        // id straight into the context dictionary, so a null id from a foreign
        // or corrupted producer faults the whole batch instead of dropping the
        // one unusable dot.
        var m = new OrMap<string, PnCounter>();
        var pc = new PnCounter(); pc.Increment("a", 1);
        var good = new PnCounter(); good.Increment("a", 7);

        Assert.DoesNotThrow(() => m.MergeDelta(new OrMapDelta<string, PnCounter>
        {
            Adds = new[]
            {
                new OrMapDeltaEntry<string, PnCounter> { Key = "bad", ReplicaId = null!, Counter = 1, Value = pc },
                new OrMapDeltaEntry<string, PnCounter> { Key = "ok", ReplicaId = "r1", Counter = 1, Value = good },
            },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        }));

        Assert.Multiple(() =>
        {
            Assert.That(m.Get("bad"), Is.Null, "a dot with no replica id carries no causal identity and must be dropped");
            Assert.That(m.Get("ok")!.Value, Is.EqualTo(7), "a poisoned dot must not fail the rest of the batch");
        });
    }

    [Test]
    public void MergeDelta_when_a_tombstone_carries_no_replica_id_does_not_throw()
    {
        var m = new OrMap<string, PnCounter>();

        Assert.DoesNotThrow(() => m.MergeDelta(new OrMapDelta<string, PnCounter>
        {
            Adds = Array.Empty<OrMapDeltaEntry<string, PnCounter>>(),
            Tombstones = new[] { new OrMapDeltaTombstone<string> { Key = "k", ReplicaId = null!, Counter = 1 } },
        }));

        Assert.That(m.Tombstones.ContainsKey("k"), Is.False,
            "a tombstone dot with no replica id carries no causal identity and must be dropped");
    }

    [Test]
    public void MergeDelta_when_an_add_carries_an_empty_replica_id_drops_only_that_dot()
    {
        var m = new OrMap<string, PnCounter>();
        var pc = new PnCounter(); pc.Increment("a", 1);
        var good = new PnCounter(); good.Increment("a", 7);

        m.MergeDelta(new OrMapDelta<string, PnCounter>
        {
            Adds = new[]
            {
                new OrMapDeltaEntry<string, PnCounter> { Key = "bad", ReplicaId = string.Empty, Counter = 1, Value = pc },
                new OrMapDeltaEntry<string, PnCounter> { Key = "ok", ReplicaId = "r1", Counter = 1, Value = good },
            },
            Tombstones = Array.Empty<OrMapDeltaTombstone<string>>(),
        });

        Assert.Multiple(() =>
        {
            Assert.That(m.Get("bad"), Is.Null,
                "an empty replica id is rejected by Set/Remove and must be rejected on the wire path too");
            Assert.That(m.Get("ok")!.Value, Is.EqualTo(7));
        });
    }
}