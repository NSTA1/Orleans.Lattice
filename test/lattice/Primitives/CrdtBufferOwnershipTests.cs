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
}
