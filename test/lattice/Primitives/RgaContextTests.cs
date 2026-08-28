namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Tests for the per-replica counter cache (<see cref="Rga.Context"/>) that lets
/// <c>NextCounter</c> mint a fresh dot in O(1) instead of rescanning every node on
/// every <see cref="Rga.InsertAfter(OrSetDot, string, byte[])"/>. The cache must stay
/// consistent across every mutator - including the two merge entry points,
/// <see cref="Rga.MergeFrom(Rga)"/> and <see cref="Rga.MergeDelta(RgaDelta)"/> - and
/// rebuild itself from the nodes when a legacy payload deserializes it as empty, so a
/// later local insert never mints a counter that collides with an existing dot (which
/// would mint a duplicate dot and break convergence). Mirrors
/// <see cref="OrMapContextTests"/> for the sibling <c>OrMap</c> cache.
/// </summary>
[TestFixture]
public class RgaContextTests
{
    private static byte[] Bytes(int k) => [(byte)k];

    /// <summary>
    /// Builds a sequence with three inserts authored by <paramref name="replicaId"/>
    /// - dots <c>(replicaId, 1..3)</c> - and then clears the Context cache to simulate
    /// a payload persisted before the Context field existed: nodes present, cache empty.
    /// </summary>
    private static Rga LegacySequence(string replicaId = "A")
    {
        var rga = new Rga();
        var d1 = rga.InsertAfter(Rga.Root, replicaId, Bytes(1));
        var d2 = rga.InsertAfter(d1, replicaId, Bytes(2));
        rga.InsertAfter(d2, replicaId, Bytes(3));
        rga.Context.Clear();
        return rga;
    }

    private static (string ReplicaId, long Counter)[] Dots(Rga rga) =>
        rga.Nodes.Select(static n => (n.ReplicaId, n.Counter)).ToArray();

    [Test]
    public void Context_tracks_per_replica_maximum()
    {
        var rga = new Rga();
        var a1 = rga.InsertAfter(Rga.Root, "r1", Bytes(1));
        rga.InsertAfter(a1, "r1", Bytes(2));
        rga.InsertAfter(Rga.Root, "r2", Bytes(3));

        Assert.That(rga.Context["r1"], Is.EqualTo(2));
        Assert.That(rga.Context["r2"], Is.EqualTo(1));
    }

    [Test]
    public void Legacy_payload_rebuilds_on_first_insert_without_merge()
    {
        // The pure-local mutation path (InsertAfter -> NextCounter) already rebuilds
        // the cache, so this passes with or without the merge-path fix - it isolates
        // the defect to the two merge entry points exercised by the tests below.
        var rga = LegacySequence("A");

        var dot = rga.InsertAfter(Rga.Root, "A", Bytes(9));

        Assert.That(dot.Counter, Is.EqualTo(4));
        Assert.That(Dots(rga), Is.Unique);
    }

    [Test]
    public void MergeFrom_on_legacy_sequence_preserves_local_maxima_so_later_inserts_do_not_collide()
    {
        // Regression for the missing EnsureContextRebuilt() in Rga.MergeFrom: a
        // legacy-loaded sequence that merges a peer before its first local insert must
        // still fold its own per-replica maxima into the cache, or the next insert
        // re-mints an already-authored dot and convergence breaks.
        var local = LegacySequence("A"); // dots (A,1),(A,2),(A,3); Context empty

        var peer = new Rga();
        peer.InsertAfter(Rga.Root, "B", Bytes(7)); // maintained peer, Context {B:1}

        local.MergeFrom(peer);

        var dot = local.InsertAfter(Rga.Root, "A", Bytes(9));

        Assert.That(dot.Counter, Is.EqualTo(4), "next A dot must continue A's run, not collide with an existing dot");
        Assert.That(Dots(local), Is.Unique, "merge + insert must not mint a duplicate dot");
    }

    [Test]
    public void MergeDelta_on_legacy_sequence_preserves_local_maxima_so_later_inserts_do_not_collide()
    {
        // Regression for the missing EnsureContextRebuilt() in Rga.MergeDelta: the
        // delta-apply path (steady-state replication and staged cross-tree writes)
        // carries the identical defect as the full-state MergeFrom path above.
        var local = LegacySequence("A"); // dots (A,1),(A,2),(A,3); Context empty

        var delta = new RgaDelta
        {
            Inserts =
            [
                new RgaDeltaNode { ReplicaId = "B", Counter = 1, ParentDot = Rga.Root, Value = Bytes(7) },
            ],
            Tombstones = [],
        };

        local.MergeDelta(delta);

        var dot = local.InsertAfter(Rga.Root, "A", Bytes(9));

        Assert.That(dot.Counter, Is.EqualTo(4), "next A dot must continue A's run, not collide with an existing dot");
        Assert.That(Dots(local), Is.Unique, "delta apply + insert must not mint a duplicate dot");
    }

    [Test]
    public void MergeFrom_folds_an_incoming_context_even_when_the_other_side_has_no_nodes()
    {
        // Regression for the early return in Rga.MergeFrom that short-circuited
        // ahead of the Context fold. Context is a second, independent piece of
        // merge state, and an incoming sequence can carry a populated Context with
        // no nodes - a decoded wire/storage payload, or any future tombstone GC,
        // which Context exists precisely to survive. Dropping those maxima lets
        // the next local insert re-mint an already-authored dot.
        var local = new Rga();
        local.InsertAfter(Rga.Root, "A", Bytes(1));

        // A peer that has observed B up to counter 7 but carries no nodes.
        var peer = new Rga { Context = { ["B"] = 7 } };

        local.MergeFrom(peer);

        Assert.That(local.Context.TryGetValue("B", out var observed), Is.True,
            "the incoming per-replica maxima must be folded even with no incoming nodes");
        Assert.That(observed, Is.EqualTo(7));

        var dot = local.InsertAfter(Rga.Root, "B", Bytes(9));
        Assert.That(dot.Counter, Is.EqualTo(8),
            "the next B dot must continue past the observed maximum, not collide with it");
    }

    [Test]
    public void MergeFrom_with_an_empty_other_still_rebuilds_a_legacy_local_context()
    {
        // The Context fold must stay *below* EnsureContextRebuilt: that helper
        // bails out on a non-empty Context, so folding the incoming side first
        // would suppress the rebuild of the receiver's own maxima.
        var local = LegacySequence("A"); // dots (A,1..3); Context cleared
        var peer = new Rga { Context = { ["B"] = 2 } };

        local.MergeFrom(peer);

        var dot = local.InsertAfter(Rga.Root, "A", Bytes(9));

        Assert.That(dot.Counter, Is.EqualTo(4),
            "the receiver's own maxima must still be rebuilt when the incoming side folds first");
        Assert.That(Dots(local), Is.Unique);
    }

    [Test]
    public void MergeFrom_of_an_empty_peer_is_still_idempotent()
    {
        var local = new Rga();
        local.InsertAfter(Rga.Root, "A", Bytes(1));
        var peer = new Rga { Context = { ["B"] = 3 } };

        local.MergeFrom(peer);
        var afterFirst = local.Context.ToDictionary(static kv => kv.Key, static kv => kv.Value);
        local.MergeFrom(peer);

        Assert.That(local.Context, Is.EqualTo(afterFirst).AsCollection,
            "re-folding the same context must be a no-op");
    }
}
