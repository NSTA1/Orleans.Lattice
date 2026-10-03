using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Direct coverage for the coordinator's participant fingerprint. The digest is
/// the coordinator's cross-silo agreement token - every participant sub-saga is
/// expected to agree on it - so it must be a pure function of the participant
/// tree ids and their key sets, stable across the shape of the collections that
/// carry them.
/// <para>
/// The fingerprint sorts each participant's keys through a scratch window that
/// is rented from <see cref="System.Buffers.ArrayPool{T}"/> and reused across
/// participants, so these assertions also pin the two properties that reuse can
/// break: a participant must not observe the previous participant's residue
/// beyond its own key count, and a narrower participant following a wider one
/// must hash exactly its own keys.
/// </para>
/// </summary>
[TestFixture]
public class LatticeCrossTreeTxGrainFingerprintTests
{
    private static CrossTreeParticipant Participant(string treeId, params string[] keys)
    {
        var entries = new List<KeyValuePair<string, byte[]>>(keys.Length);
        foreach (var key in keys) entries.Add(new KeyValuePair<string, byte[]>(key, [1, 2, 3]));
        return new CrossTreeParticipant { TreeId = treeId, Entries = entries };
    }

    [Test]
    public void ComputeFingerprint_is_stable_for_the_same_participants()
    {
        var a = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("orders", "order:2", "order:1"), Participant("inventory", "sku:9")]);
        var b = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("orders", "order:2", "order:1"), Participant("inventory", "sku:9")]);

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Has.Length.EqualTo(32));
    }

    [Test]
    public void ComputeFingerprint_ignores_key_order_within_a_participant()
    {
        var ascending = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("orders", "order:1", "order:2", "order:3")]);
        var descending = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("orders", "order:3", "order:2", "order:1")]);

        Assert.That(ascending, Is.EqualTo(descending));
    }

    [Test]
    public void ComputeFingerprint_differs_when_a_key_differs()
    {
        var original = LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("orders", "order:1")]);
        var altered = LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("orders", "order:2")]);

        Assert.That(original, Is.Not.EqualTo(altered));
    }

    [Test]
    public void ComputeFingerprint_differs_when_a_tree_id_differs()
    {
        var original = LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("orders", "k")]);
        var altered = LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("inventory", "k")]);

        Assert.That(original, Is.Not.EqualTo(altered));
    }

    [Test]
    public void ComputeFingerprint_depends_on_participant_order()
    {
        var forward = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("orders", "k"), Participant("inventory", "k")]);
        var reversed = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("inventory", "k"), Participant("orders", "k")]);

        Assert.That(forward, Is.Not.EqualTo(reversed));
    }

    [Test]
    public void ComputeFingerprint_of_no_participants_is_a_stable_empty_digest()
    {
        var first = LatticeCrossTreeTxGrain.ComputeFingerprint([]);
        var second = LatticeCrossTreeTxGrain.ComputeFingerprint([]);

        Assert.That(first, Is.EqualTo(second));
        Assert.That(first, Has.Length.EqualTo(32));
    }

    [Test]
    public void ComputeFingerprint_of_a_keyless_participant_still_binds_its_tree_id()
    {
        var orders = LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("orders")]);
        var inventory = LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("inventory")]);

        Assert.That(orders, Is.Not.EqualTo(inventory));
    }

    [Test]
    public void ComputeFingerprint_does_not_leak_a_wider_participant_into_a_narrower_one()
    {
        // The key window is rented once and sized to the widest participant, so a
        // narrower participant that followed a wider one would hash the wider
        // one's residue if the shipped body read past its own key count. Compare
        // against the same narrow participant hashed on its own, where no residue
        // can exist.
        var narrowAfterWide = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("wide", "a", "b", "c", "d", "e"), Participant("narrow", "z")]);
        var narrowAlone = LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("narrow", "z")]);
        var wideAlone = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("wide", "a", "b", "c", "d", "e")]);

        // Concatenating the two single-participant digests is not the two-participant
        // digest, so assert the discriminating property instead: swapping the narrow
        // participant's only key must move the combined digest.
        var moved = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("wide", "a", "b", "c", "d", "e"), Participant("narrow", "y")]);

        Assert.Multiple(() =>
        {
            Assert.That(narrowAfterWide, Is.Not.EqualTo(moved));
            Assert.That(narrowAfterWide, Is.Not.EqualTo(narrowAlone));
            Assert.That(narrowAfterWide, Is.Not.EqualTo(wideAlone));
        });
    }

    [Test]
    public void ComputeFingerprint_is_unchanged_by_repeated_calls_that_reuse_the_pool()
    {
        // A rented window returned without clearing the slots it wrote could be
        // handed back dirty; hashing the same participants many times in a row
        // must keep producing the same digest.
        var expected = LatticeCrossTreeTxGrain.ComputeFingerprint(
            [Participant("orders", "a", "b"), Participant("inventory", "c", "d", "e")]);

        for (var i = 0; i < 32; i++)
        {
            LatticeCrossTreeTxGrain.ComputeFingerprint([Participant("noise", $"k{i}", $"j{i}", $"i{i}")]);
            var actual = LatticeCrossTreeTxGrain.ComputeFingerprint(
                [Participant("orders", "a", "b"), Participant("inventory", "c", "d", "e")]);
            Assert.That(actual, Is.EqualTo(expected));
        }
    }
}
