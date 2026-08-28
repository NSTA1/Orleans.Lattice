using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regressions for the buffer-ownership contract documented on
/// <see cref="ICrdt{TSelf}"/>: a fold from a peer or a delta copies, and egress
/// to a caller copies, so no caller and no peer is ever left holding a live
/// handle on another instance's durable state.
/// <para>
/// The egress leg is the one with a history: <c>OrMap.Clone</c> and
/// <c>OrMap.Get</c> were each fixed for sharing a nested value, and
/// <see cref="Rga.Clone"/> was the remaining sibling - it duplicated every node
/// but shared each node's value byte array, so a caller that read a sequence out
/// of an <c>OrMap</c> could write straight into the map's stored state.
/// </para>
/// </summary>
[TestFixture]
public class CrdtBufferOwnershipTests
{
    private static byte[] Bytes(params byte[] values) => values;

    [Test]
    public void Rga_Clone_copies_node_values_so_a_caller_cannot_write_into_the_original()
    {
        var original = new Rga();
        original.InsertAfter(Rga.Root, "A", Bytes(1, 2, 3));

        var clone = original.Clone();
        clone.Nodes[0].Value[0] = 99;

        Assert.That(original.Nodes[0].Value[0], Is.EqualTo(1),
            "mutating a clone's node value must not write through into the source sequence");
    }

    [Test]
    public void Rga_Clone_does_not_share_any_node_value_reference()
    {
        var original = new Rga();
        var first = original.InsertAfter(Rga.Root, "A", Bytes(1));
        original.InsertAfter(first, "A", Bytes(2));

        var clone = original.Clone();

        for (var i = 0; i < original.Nodes.Count; i++)
        {
            Assert.That(ReferenceEquals(original.Nodes[i].Value, clone.Nodes[i].Value), Is.False,
                $"node {i} value array must not be shared between a sequence and its clone");
        }
    }

    [Test]
    public void Rga_Clone_preserves_node_values_by_content()
    {
        var original = new Rga();
        var first = original.InsertAfter(Rga.Root, "A", Bytes(1, 2));
        original.InsertAfter(first, "B", Bytes(3, 4));

        var clone = original.Clone();

        Assert.That(clone.ToList().Select(static e => e.Value),
            Is.EqualTo(original.ToList().Select(static e => e.Value)).AsCollection,
            "the copy must be by value, not merely independent");
    }

    [Test]
    public void Rga_Clone_of_a_tombstoned_node_allocates_no_new_array()
    {
        // A tombstone carries Array.Empty<byte>(), and an empty span's ToArray()
        // returns the shared singleton - so the ownership fix costs nothing on the
        // tombstone-heavy shapes an RGA accumulates over time.
        var original = new Rga();
        var dot = original.InsertAfter(Rga.Root, "A", Bytes(1));
        original.Remove(dot);
        original.Nodes[0].Value = Array.Empty<byte>();

        var clone = original.Clone();

        Assert.That(ReferenceEquals(clone.Nodes[0].Value, Array.Empty<byte>()), Is.True,
            "an empty value must reuse the Array.Empty<byte>() singleton rather than allocate");
    }

    [Test]
    public void BoundedRegister_Clone_still_copies_both_arrays()
    {
        // The sibling that already honoured the contract; pinned so the family
        // stays consistent rather than regressing to the shared-array shape.
        var register = BoundedRegister.CreateEmpty(isMin: false);
        register.Set(Bytes(1, 2), Bytes(9));

        var clone = register.Clone();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(register.Value, clone.Value), Is.False);
            Assert.That(ReferenceEquals(register.OrderKey, clone.OrderKey), Is.False);
        });
    }

    [Test]
    public void BoundedRegister_fold_copies_the_winning_candidate_from_a_peer()
    {
        var peer = BoundedRegister.CreateEmpty(isMin: false);
        peer.Set(Bytes(5), Bytes(9));

        var receiver = BoundedRegister.CreateEmpty(isMin: false);
        receiver.MergeFrom(peer);

        Assert.That(ReferenceEquals(receiver.Value, peer.Value), Is.False,
            "a fold must not leave the receiver aliased to the peer's buffer");
    }

    [Test]
    public void Rga_MergeFrom_copies_the_adopted_node_value_from_a_peer()
    {
        // The fold leg of the same contract BoundedRegister honours above: a
        // state merge that adopts a node the receiver has not seen must copy the
        // peer's value bytes, or the two sequences share a live buffer and a
        // later mutation on either side bleeds into the other's durable state.
        var peer = new Rga();
        peer.InsertAfter(Rga.Root, "peer", Bytes(1, 2, 3));

        var receiver = new Rga();
        receiver.MergeFrom(peer);

        var adopted = receiver.Nodes.Single(n => n.Dot.Equals(peer.Nodes[0].Dot));
        Assert.That(ReferenceEquals(adopted.Value, peer.Nodes[0].Value), Is.False,
            "a state-merge fold must not leave the receiver aliased to the peer's node buffer");

        peer.Nodes[0].Value[0] = 99;
        Assert.That(receiver.ToList().Single().Value[0], Is.EqualTo(1),
            "mutating the peer's buffer after the fold must not change the receiver");
    }

    [Test]
    public void Rga_MergeFrom_copies_the_winning_value_on_a_same_dot_collision()
    {
        // Two replicas independently authored the same dot with different values.
        // The merge deterministically keeps the lexicographically larger value;
        // it must keep a copy, not a live handle on the peer's array.
        var receiver = new Rga();
        receiver.InsertAfter(Rga.Root, "R", Bytes(1));

        var peer = new Rga();
        peer.InsertAfter(Rga.Root, "R", Bytes(2));

        receiver.MergeFrom(peer);

        var node = receiver.Nodes.Single();
        Assert.Multiple(() =>
        {
            Assert.That(node.Value, Is.EqualTo(Bytes(2)).AsCollection,
                "the larger value wins the same-dot collision");
            Assert.That(ReferenceEquals(node.Value, peer.Nodes[0].Value), Is.False,
                "the winning value must be copied, not aliased to the peer's buffer");
        });
    }

    [Test]
    public void Rga_MergeDelta_copies_the_adopted_node_value_from_a_delta()
    {
        // A producer fans one typed delta out to several receivers (and may pool
        // the value buffer). Applying it must copy the insert's value, or every
        // receiver of that delta shares one durable buffer.
        var value = Bytes(1, 2, 3);
        var delta = new RgaDelta
        {
            Inserts = new[]
            {
                new RgaDeltaNode { ReplicaId = "P", Counter = 1, ParentDot = Rga.Root, Value = value },
            },
            Tombstones = Array.Empty<OrSetDot>(),
        };

        var receiver = new Rga();
        receiver.MergeDelta(delta);

        Assert.That(ReferenceEquals(receiver.Nodes.Single().Value, value), Is.False,
            "applying a delta must copy the insert's value, not adopt the producer's buffer");

        value[0] = 99;
        Assert.That(receiver.ToList().Single().Value[0], Is.EqualTo(1),
            "mutating the producer's delta buffer after the apply must not change the receiver");
    }

    [Test]
    public void Rga_MergeDelta_copies_the_winning_value_on_a_same_dot_collision()
    {
        var receiver = new Rga();
        receiver.InsertAfter(Rga.Root, "P", Bytes(1));

        var winning = Bytes(2);
        var delta = new RgaDelta
        {
            Inserts = new[]
            {
                new RgaDeltaNode { ReplicaId = "P", Counter = 1, ParentDot = Rga.Root, Value = winning },
            },
            Tombstones = Array.Empty<OrSetDot>(),
        };

        receiver.MergeDelta(delta);

        var node = receiver.Nodes.Single();
        Assert.Multiple(() =>
        {
            Assert.That(node.Value, Is.EqualTo(Bytes(2)).AsCollection,
                "the larger value wins the same-dot collision on delta apply");
            Assert.That(ReferenceEquals(node.Value, winning), Is.False,
                "the winning value must be copied, not aliased to the delta's buffer");
        });
    }

    [Test]
    public void Rga_MergeDelta_of_an_empty_insert_reuses_the_empty_singleton()
    {
        // The copy-on-fold fix must stay zero-allocation on empty values: an
        // empty span's ToArray() returns Array.Empty<byte>(), so a tombstone or
        // empty insert still shares the singleton rather than allocating.
        var delta = new RgaDelta
        {
            Inserts = new[]
            {
                new RgaDeltaNode { ReplicaId = "P", Counter = 1, ParentDot = Rga.Root, Value = Array.Empty<byte>() },
            },
            Tombstones = Array.Empty<OrSetDot>(),
        };

        var receiver = new Rga();
        receiver.MergeDelta(delta);

        Assert.That(ReferenceEquals(receiver.Nodes.Single().Value, Array.Empty<byte>()), Is.True,
            "an empty insert value must reuse the Array.Empty<byte>() singleton rather than allocate");
    }

    [Test]
    public void OrMap_Get_of_a_multi_contributor_Rga_key_does_not_alias_the_maps_durable_state()
    {
        // OrMap.Get seeds the accumulator from a clone of the first contributor
        // but folds every later contributor in via Rga.MergeFrom. If that fold
        // aliases, the returned sequence shares the map's durable buffers for the
        // second-and-later contributors, so a caller that mutates the value it
        // read corrupts the stored map - exactly what Get's contract forbids.
        var replicaA = new OrMap<string, Rga>();
        var rgaA = new Rga();
        rgaA.InsertAfter(Rga.Root, "A", Bytes(1));
        replicaA.Set("k", "A", rgaA);

        var replicaB = new OrMap<string, Rga>();
        var rgaB = new Rga();
        rgaB.InsertAfter(Rga.Root, "B", Bytes(2));
        replicaB.Set("k", "B", rgaB);

        replicaA.MergeFrom(replicaB);

        var got = replicaA.Get("k")!;
        foreach (var node in got.Nodes)
        {
            node.Value[0] = 99;
        }

        var reread = replicaA.Get("k")!;
        Assert.That(reread.ToList().Select(static e => e.Value[0]).ToArray(), Has.None.EqualTo((byte)99),
            "mutating the value returned by OrMap.Get must not corrupt the map's durable state");
    }
}
