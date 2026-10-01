using System.Text;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regression coverage that a sequential <see cref="Rga.InsertAfter(OrSetDot, string, byte[])"/>
/// lands immediately after its parent whichever replica authored the parent's
/// existing children. Siblings sort by descending <c>(Counter, ReplicaId)</c>, so
/// the minted counter must exceed every counter the sequence has observed (a
/// Lamport clock). It used to be one above the authoring replica's own highest
/// counter only, which let another replica's earlier sibling sort first and
/// pushed the insert past that sibling's whole subtree - so
/// <c>RgaAccessor.InsertAtAsync(0, ...)</c> by a second replica could land at
/// the tail rather than the head.
/// </summary>
[TestFixture]
public class RgaInsertOrderingTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static IReadOnlyList<string> Strings(Rga r) =>
        r.ToList().Select(t => Encoding.UTF8.GetString(t.Value)).ToArray();

    [Test]
    public void InsertAfter_root_by_a_second_replica_lands_at_the_head()
    {
        // Sequential, not concurrent: replica "a" has observed "b"'s head node
        // and inserts at the root. Under a per-replica counter both nodes had
        // counter 1 and the "b" > "a" tie-break kept "b" first, so a head
        // insert landed at the tail.
        var r = new Rga();
        r.InsertAfter(Rga.Root, "b", B("B"));
        r.InsertAfter(Rga.Root, "a", B("A"));

        Assert.That(Strings(r), Is.EqualTo(new[] { "A", "B" }));
    }

    [Test]
    public void InsertAfter_a_parent_lands_immediately_after_it_when_another_replica_holds_a_higher_counter()
    {
        // "b" builds X -> Y (counters 1, 2). "a" then inserts after X. Under a
        // per-replica counter "a"'s node got counter 1, sorted below Y (2), and
        // was emitted after Y's whole subtree instead of directly after X.
        var r = new Rga();
        var x = r.InsertAfter(Rga.Root, "b", B("X"));
        r.InsertAfter(x, "b", B("Y"));
        r.InsertAfter(x, "a", B("N"));

        Assert.That(Strings(r), Is.EqualTo(new[] { "X", "N", "Y" }));
    }

    [Test]
    public void InsertAfter_after_merging_a_peer_lands_ahead_of_the_peers_siblings()
    {
        // The observed counters a later insert must exceed include those
        // folded in from a peer, not only the ones authored locally.
        var peer = new Rga();
        var p = peer.InsertAfter(Rga.Root, "peer", B("P"));
        peer.InsertAfter(p, "peer", B("Q"));
        peer.InsertAfter(p, "peer", B("R"));

        var local = new Rga();
        local.MergeFrom(peer);
        Assume.That(Strings(local), Is.EqualTo(new[] { "P", "R", "Q" }));

        local.InsertAfter(p, "local", B("L"));

        Assert.That(Strings(local), Is.EqualTo(new[] { "P", "L", "R", "Q" }));
    }

    [Test]
    public void InsertAfter_after_a_delta_lands_ahead_of_the_deltas_siblings()
    {
        // The replication delta-apply path must raise the next counter too.
        var local = new Rga();
        var p = new OrSetDot { ReplicaId = "peer", Counter = 1 };
        local.MergeDelta(new RgaDelta
        {
            Inserts =
            [
                new RgaDeltaNode { ReplicaId = "peer", Counter = 1, ParentDot = Rga.Root, Value = B("P") },
                new RgaDeltaNode { ReplicaId = "peer", Counter = 9, ParentDot = p, Value = B("Q") },
            ],
            Tombstones = [],
        });

        local.InsertAfter(p, "local", B("L"));

        Assert.That(Strings(local), Is.EqualTo(new[] { "P", "L", "Q" }));
    }

    [Test]
    public void Concurrent_inserts_under_one_parent_still_converge_in_either_merge_order()
    {
        // Minting from the observed maximum does not disturb convergence: two
        // replicas that insert concurrently from the same prefix fold to one
        // order whichever side merges first.
        var seed = new Rga();
        var prefix = seed.InsertAfter(Rga.Root, "r0", B("H"));

        var left = seed.Clone();
        left.InsertAfter(prefix, "r1", B("a"));
        var right = seed.Clone();
        right.InsertAfter(prefix, "r2", B("b"));

        Assert.That(Strings(Rga.Merge(left, right)), Is.EqualTo(Strings(Rga.Merge(right, left))));
    }
}
