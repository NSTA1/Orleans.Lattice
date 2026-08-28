using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Buffer-ownership regressions for <see cref="MvRegister"/> and for
/// <see cref="Rga.ToList"/> - the legs that were still open after the sequence's
/// fold was closed.
/// <para>
/// <see cref="MvRegister"/> violated all three legs at once while being one of the
/// four primitives <see cref="ICrdt{TSelf}"/> names in its own remarks: its fold
/// adopted a peer's and a delta's buffers, its <c>Clone</c> was a shallow entry
/// copy justified in a comment by the values being "immutable by every production
/// call site" (the same reasoning already rejected for <c>Rga.Clone</c>), and
/// <c>Values</c> handed out the stored arrays and cached them.
/// </para>
/// </summary>
public partial class CrdtBufferOwnershipTests
{
    [Test]
    public void MvRegister_MergeFrom_copies_the_adopted_entry_value_from_a_peer()
    {
        var peer = new MvRegister();
        peer.Set("B", Bytes(1, 2, 3));

        var receiver = new MvRegister();
        receiver.MergeFrom(peer);

        Assert.That(ReferenceEquals(receiver.Values()[0], peer.Entries[0].Value), Is.False,
            "a state-merge fold must not leave the receiver aliased to the peer's entry buffer");

        peer.Entries[0].Value[0] = 99;
        Assert.That(receiver.Values()[0][0], Is.EqualTo(1),
            "mutating the peer's buffer after the fold must not change the receiver");
    }

    [Test]
    public void MvRegister_MergeFrom_copies_the_winning_value_on_a_same_dot_collision()
    {
        // Both sides carry dot (R, 1) with different values. The merge keeps the
        // deterministically-greater one; it must keep a copy of it.
        var receiver = new MvRegister();
        receiver.Set("R", Bytes(1));

        var peer = new MvRegister();
        peer.Set("R", Bytes(2));

        receiver.MergeFrom(peer);

        Assert.Multiple(() =>
        {
            Assert.That(receiver.Values()[0], Is.EqualTo(Bytes(2)).AsCollection,
                "the larger value wins the same-dot collision");
            Assert.That(ReferenceEquals(receiver.Entries[0].Value, peer.Entries[0].Value), Is.False,
                "the winning value must be copied, not aliased to the peer's buffer");
        });
    }

    [Test]
    public void MvRegister_MergeFrom_that_keeps_the_local_value_copies_nothing()
    {
        // Only a winning candidate is copied: a fold that keeps the local side
        // must not allocate, or the steady-state replication merge pays for a
        // copy it never uses.
        var receiver = new MvRegister();
        receiver.Set("R", Bytes(9));
        var localBuffer = receiver.Entries[0].Value;

        var peer = new MvRegister();
        peer.Set("R", Bytes(1));

        receiver.MergeFrom(peer);

        Assert.That(ReferenceEquals(receiver.Entries[0].Value, localBuffer), Is.True,
            "a losing fold must leave the local buffer in place rather than copy it");
    }

    [Test]
    public void MvRegister_MergeDelta_copies_the_adopted_entry_value_from_a_delta()
    {
        var producer = new MvRegister();
        producer.Set("P", Bytes(1, 2, 3));
        var delta = new MvRegisterDelta
        {
            Entries = producer.Entries.ToList(),
            Context = new Dictionary<string, long>(producer.Context),
        };

        var receiver = new MvRegister();
        receiver.MergeDelta(delta);

        Assert.That(ReferenceEquals(receiver.Values()[0], delta.Entries![0].Value), Is.False,
            "applying a delta must copy the entry's value, not adopt the producer's buffer");

        delta.Entries[0].Value[0] = 99;
        Assert.That(receiver.Values()[0][0], Is.EqualTo(1),
            "mutating the producer's delta buffer after the apply must not change the receiver");
    }

    [Test]
    public void MvRegister_Clone_copies_entry_values_so_a_caller_cannot_write_into_the_original()
    {
        var original = new MvRegister();
        original.Set("A", Bytes(1, 2, 3));

        var clone = original.Clone();
        clone.Entries[0].Value[0] = 99;

        Assert.That(original.Entries[0].Value[0], Is.EqualTo(1),
            "mutating a clone's entry value must not write through into the source register");
    }

    [Test]
    public void MvRegister_Values_copies_so_a_caller_cannot_write_into_stored_state()
    {
        var register = new MvRegister();
        register.Set("A", Bytes(1, 2, 3));

        register.Values()[0][0] = 99;

        Assert.That(register.Entries[0].Value[0], Is.EqualTo(1),
            "a materialised projection must not hand out the register's stored buffers");
    }

    [Test]
    public void MvRegister_Values_does_not_hand_the_same_buffer_to_two_readers()
    {
        // The single-value read is cached. Caching the ordering is fine; caching
        // the buffers would let one reader corrupt every later read.
        var register = new MvRegister();
        register.Set("A", Bytes(1, 2, 3));

        Assert.That(ReferenceEquals(register.Values()[0], register.Values()[0]), Is.False,
            "two readers must not receive the same mutable buffer");
    }

    [Test]
    public void MvRegister_Values_of_an_empty_value_reuses_the_empty_singleton()
    {
        var register = new MvRegister();
        register.Set("A", Array.Empty<byte>());

        Assert.That(ReferenceEquals(register.Values()[0], Array.Empty<byte>()), Is.True,
            "an empty value must reuse the Array.Empty<byte>() singleton rather than allocate");
    }

    [Test]
    public void Rga_ToList_copies_so_a_caller_cannot_write_into_stored_state()
    {
        // ToList is the sequence's materialised projection and its primary read
        // API. It shared the live node buffers, and because the resolved order is
        // cached and nothing invalidates it, a single write through a returned
        // value corrupted every later read.
        var rga = new Rga();
        rga.InsertAfter(Rga.Root, "A", Bytes(1, 2, 3));

        rga.ToList()[0].Value[0] = 99;

        Assert.Multiple(() =>
        {
            Assert.That(rga.Nodes[0].Value[0], Is.EqualTo(1),
                "a projection must not hand out the sequence's live node buffers");
            Assert.That(rga.ToList()[0].Value[0], Is.EqualTo(1),
                "and a later read must not observe the corruption");
        });
    }

    [Test]
    public void Rga_ToList_does_not_hand_the_same_buffer_to_two_readers()
    {
        var rga = new Rga();
        rga.InsertAfter(Rga.Root, "A", Bytes(1, 2, 3));

        Assert.That(ReferenceEquals(rga.ToList()[0].Value, rga.ToList()[0].Value), Is.False,
            "two readers must not receive the same mutable buffer");
    }

    [Test]
    public void Rga_ToList_of_a_tombstoned_sequence_allocates_no_value_arrays()
    {
        // A tombstoned node is not emitted at all, so an aged sequence that is
        // mostly tombstones pays nothing for the egress copy.
        var rga = new Rga();
        var dot = rga.InsertAfter(Rga.Root, "A", Bytes(1));
        rga.Remove(dot);

        Assert.That(rga.ToList(), Is.Empty,
            "tombstoned nodes are excluded from the projection, so they cost no copy");
    }

    [Test]
    public void OrMap_Get_of_a_single_contributor_MvRegister_key_does_not_alias_the_maps_durable_state()
    {
        // Get's steady-state fast path returns Clone() of the sole contributor,
        // precisely so the caller may mutate what it read. That guarantee is only
        // as deep as the nested type's Clone.
        var map = new OrMap<string, MvRegister>();
        var register = new MvRegister();
        register.Set("A", Bytes(1));
        map.Set("k", "A", register);

        map.Get("k")!.Entries[0].Value[0] = 99;

        Assert.That(map.Get("k")!.Values()[0][0], Is.EqualTo(1),
            "mutating the value returned by OrMap.Get must not corrupt the map's durable state");
    }

    [Test]
    public void OrMap_Get_of_a_multi_contributor_MvRegister_key_does_not_alias_the_maps_durable_state()
    {
        var replicaA = new OrMap<string, MvRegister>();
        var a = new MvRegister();
        a.Set("A", Bytes(1));
        replicaA.Set("k", "A", a);

        var replicaB = new OrMap<string, MvRegister>();
        var b = new MvRegister();
        b.Set("B", Bytes(2));
        replicaB.Set("k", "B", b);

        replicaA.MergeFrom(replicaB);

        var got = replicaA.Get("k")!;
        foreach (var entry in got.Entries) entry.Value[0] = 99;

        Assert.That(replicaA.Get("k")!.Values().Select(static v => v[0]).ToArray(), Has.None.EqualTo((byte)99),
            "mutating the value returned by OrMap.Get must not corrupt the map's durable state");
    }
}
