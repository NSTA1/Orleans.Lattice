using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class RgaTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string S(byte[] b) => Encoding.UTF8.GetString(b);

    private static IReadOnlyList<string> Strings(Rga r) =>
        r.ToList().Select(t => S(t.Value)).ToArray();

    [Test]
    public void New_sequence_is_empty_and_bottom()
    {
        var r = new Rga();
        Assert.That(r.IsEmpty, Is.True);
        Assert.That(r.IsBottom, Is.True);
        Assert.That(r.Count, Is.EqualTo(0));
        Assert.That(r.ToList(), Is.Empty);
        Assert.That(r.Nodes, Is.Empty);
    }

    [Test]
    public void Root_is_default_OrSetDot()
    {
        Assert.That(Rga.Root, Is.EqualTo(default(OrSetDot)));
    }

    [Test]
    public void InsertAfter_throws_on_invalid_arguments()
    {
        var r = new Rga();
        Assert.That(() => r.InsertAfter(Rga.Root, null!, B("x")), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => r.InsertAfter(Rga.Root, "", B("x")), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => r.InsertAfter(Rga.Root, "r1", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void InsertAfter_at_root_appends_in_descending_counter_order()
    {
        // Sibling order is descending (Counter, ReplicaId), so each
        // newer insert at the root is emitted *before* every prior
        // root-level insert.
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a")); // counter 1
        r.InsertAfter(Rga.Root, "r1", B("b")); // counter 2
        r.InsertAfter(Rga.Root, "r1", B("c")); // counter 3
        Assert.That(Strings(r), Is.EqualTo(new[] { "c", "b", "a" }));
        Assert.That(r.Count, Is.EqualTo(3));
    }

    [Test]
    public void InsertAfter_chains_under_parent_to_build_visible_order()
    {
        // Building a typeable text buffer one character at a time
        // requires every new node to attach to the previous node, so
        // descending sibling order does not invert the prefix.
        var r = new Rga();
        var d1 = r.InsertAfter(Rga.Root, "r1", B("H"));
        var d2 = r.InsertAfter(d1, "r1", B("i"));
        var d3 = r.InsertAfter(d2, "r1", B("!"));
        Assert.That(Strings(r), Is.EqualTo(new[] { "H", "i", "!" }));
        Assert.That(d1.Counter, Is.EqualTo(1));
        Assert.That(d2.Counter, Is.EqualTo(2));
        Assert.That(d3.Counter, Is.EqualTo(3));
    }

    [Test]
    public void Concurrent_root_inserts_break_ties_by_replicaId_descending()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a")); // (1, r1)
        // Simulate a concurrent insert at the root from another
        // replica with the same counter by hand-stamping the node.
        r.Nodes.Add(new RgaNode { ReplicaId = "r2", Counter = 1, ParentDot = Rga.Root, Value = B("z") });
        Assert.That(Strings(r), Is.EqualTo(new[] { "z", "a" }));
    }

    [Test]
    public void Remove_unknown_dot_is_noop()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("x"));
        Assert.That(r.Remove(new OrSetDot { ReplicaId = "missing", Counter = 99 }), Is.False);
    }

    [Test]
    public void Remove_tombstones_node_but_preserves_descendants()
    {
        var r = new Rga();
        var d1 = r.InsertAfter(Rga.Root, "r1", B("x"));
        var d2 = r.InsertAfter(d1, "r1", B("y"));
        Assert.That(r.Remove(d1), Is.True);
        Assert.That(Strings(r), Is.EqualTo(new[] { "y" }));
        // d2 is still attached under the (now-tombstoned) d1, so
        // d1 must remain in storage.
        Assert.That(r.Nodes.Any(n => n.Dot.Equals(d1)), Is.True);
        Assert.That(r.ContainsDot(d1), Is.True);
        Assert.That(r.ContainsDot(d2), Is.True);
        Assert.That(r.Count, Is.EqualTo(1));
    }

    [Test]
    public void Remove_already_tombstoned_returns_false()
    {
        var r = new Rga();
        var d = r.InsertAfter(Rga.Root, "r1", B("x"));
        Assert.That(r.Remove(d), Is.True);
        Assert.That(r.Remove(d), Is.False);
    }

    [Test]
    public void IsEmpty_is_true_when_every_live_node_is_removed()
    {
        var r = new Rga();
        var d = r.InsertAfter(Rga.Root, "r1", B("x"));
        r.Remove(d);
        Assert.That(r.IsEmpty, Is.True);
        Assert.That(r.IsBottom, Is.True);
        Assert.That(r.Nodes, Is.Not.Empty); // tombstone preserved
    }

    [Test]
    public void Merge_unions_disjoint_dots_and_resolves_in_descending_order()
    {
        var left = new Rga();
        left.InsertAfter(Rga.Root, "r1", B("a"));
        var right = new Rga();
        right.InsertAfter(Rga.Root, "r2", B("b"));
        var merged = Rga.Merge(left, right);
        // Both inserts are root-children with counter 1; r2 wins the
        // descending tie-break.
        Assert.That(Strings(merged), Is.EqualTo(new[] { "b", "a" }));
    }

    [Test]
    public void Merge_with_null_throws()
    {
        Assert.That(() => Rga.Merge(null!, new Rga()), Throws.ArgumentNullException);
        Assert.That(() => Rga.Merge(new Rga(), null!), Throws.ArgumentNullException);
        Assert.That(() => new Rga().MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Merge_preserves_tombstone_from_either_side()
    {
        var a = new Rga();
        var dot = a.InsertAfter(Rga.Root, "r1", B("x"));
        var b = a.Clone();
        b.Remove(dot);
        var merged = Rga.Merge(a, b);
        Assert.That(merged.Count, Is.EqualTo(0));
        // Tombstone is monotonic: the merge applied in the other
        // direction must produce the same outcome.
        var reversed = Rga.Merge(b, a);
        Assert.That(reversed.Count, Is.EqualTo(0));
    }

    [Test]
    public void Merge_is_commutative_associative_idempotent()
    {
        var a = new Rga(); a.InsertAfter(Rga.Root, "r1", B("a"));
        var b = new Rga(); b.InsertAfter(Rga.Root, "r2", B("b"));
        var c = new Rga(); c.InsertAfter(Rga.Root, "r3", B("c"));

        var ab  = Strings(Rga.Merge(a, b));
        var ba  = Strings(Rga.Merge(b, a));
        Assert.That(ab, Is.EqualTo(ba));

        var abc = Strings(Rga.Merge(Rga.Merge(a, b), c));
        var aBC = Strings(Rga.Merge(a, Rga.Merge(b, c)));
        Assert.That(abc, Is.EqualTo(aBC));

        var aa  = Rga.Merge(a, a);
        Assert.That(Strings(aa), Is.EqualTo(Strings(a)));
    }

    [Test]
    public void Merge_same_dot_resolves_deterministically()
    {
        // Same-dot collisions should never occur under normal
        // authoring (NextCounter prevents it), but a transport bug
        // could produce divergent values under the same dot. The
        // merge must still converge.
        var a = new Rga();
        a.Nodes.Add(new RgaNode { ReplicaId = "r1", Counter = 1, ParentDot = Rga.Root, Value = B("aa") });
        var b = new Rga();
        b.Nodes.Add(new RgaNode { ReplicaId = "r1", Counter = 1, ParentDot = Rga.Root, Value = B("bb") });
        var ab = Rga.Merge(a, b);
        var ba = Rga.Merge(b, a);
        Assert.That(Strings(ab), Is.EqualTo(Strings(ba)));
        Assert.That(Strings(ab).Single(), Is.EqualTo("bb"));
    }

    [Test]
    public void Concurrent_inserts_on_different_replicas_under_same_parent_resolve_consistently()
    {
        // Both replicas start from the same prefix, then independently
        // append a child of the same parent. After cross-merge, both
        // sides see the same resolved order.
        var seed = new Rga();
        var prefix = seed.InsertAfter(Rga.Root, "r0", B("H"));

        var left = seed.Clone();
        left.InsertAfter(prefix, "r1", B("a"));

        var right = seed.Clone();
        right.InsertAfter(prefix, "r2", B("b"));

        var leftMerged  = Rga.Merge(left, right);
        var rightMerged = Rga.Merge(right, left);
        Assert.That(Strings(leftMerged), Is.EqualTo(Strings(rightMerged)));
        // Both children have counter 1 under r1/r2 respectively; r2
        // wins the descending tie.
        Assert.That(Strings(leftMerged), Is.EqualTo(new[] { "H", "b", "a" }));
    }

    [Test]
    public void Reinsert_after_tombstoned_parent_still_resolves()
    {
        // Causal-stability check: a node tombstoned by replica A
        // must remain in the tree so replica B's later insert under
        // it still resolves on every replica.
        var a = new Rga();
        var head = a.InsertAfter(Rga.Root, "r1", B("H"));
        var b = a.Clone();
        a.Remove(head);
        b.InsertAfter(head, "r2", B("i"));

        var merged = Rga.Merge(a, b);
        // head is tombstoned, but its child "i" resolves under the
        // preserved (tombstoned) parent.
        Assert.That(Strings(merged), Is.EqualTo(new[] { "i" }));
    }

    [Test]
    public void Clone_is_independent()
    {
        var r = new Rga();
        var d = r.InsertAfter(Rga.Root, "r1", B("x"));
        var c = r.Clone();
        c.Remove(d);
        // The clone's tombstone must not bleed back to the original.
        Assert.That(r.Count, Is.EqualTo(1));
        Assert.That(c.Count, Is.EqualTo(0));
    }

    [Test]
    public void NextCounter_increments_per_replica_independently()
    {
        var r = new Rga();
        var d1 = r.InsertAfter(Rga.Root, "r1", B("a"));
        var d2 = r.InsertAfter(Rga.Root, "r2", B("b"));
        var d3 = r.InsertAfter(Rga.Root, "r1", B("c"));
        Assert.That(d1.Counter, Is.EqualTo(1));
        Assert.That(d2.Counter, Is.EqualTo(1));
        Assert.That(d3.Counter, Is.EqualTo(2));
    }

    [Test]
    public void ContainsDot_returns_false_for_unknown_and_true_for_known()
    {
        var r = new Rga();
        var d = r.InsertAfter(Rga.Root, "r1", B("x"));
        Assert.That(r.ContainsDot(d), Is.True);
        Assert.That(r.ContainsDot(new OrSetDot { ReplicaId = "x", Counter = 99 }), Is.False);
    }

    [Test]
    public void ToList_is_deterministic_across_storage_order_permutations()
    {
        // Because the visible order is recomputed from the tree
        // structure on every call, two sequences with identical node
        // content but different storage-list orderings must produce
        // the same materialised projection.
        var a = new Rga();
        var d1 = a.InsertAfter(Rga.Root, "r1", B("a"));
        var d2 = a.InsertAfter(d1, "r1", B("b"));
        var d3 = a.InsertAfter(d2, "r1", B("c"));

        var b = new Rga();
        // Insert nodes in reverse storage order but with the same
        // (dot, parentDot) shape.
        b.Nodes.Add(new RgaNode { ReplicaId = d3.ReplicaId, Counter = d3.Counter, ParentDot = d2, Value = B("c") });
        b.Nodes.Add(new RgaNode { ReplicaId = d2.ReplicaId, Counter = d2.Counter, ParentDot = d1, Value = B("b") });
        b.Nodes.Add(new RgaNode { ReplicaId = d1.ReplicaId, Counter = d1.Counter, ParentDot = Rga.Root, Value = B("a") });

        Assert.That(Strings(a), Is.EqualTo(Strings(b)));
        Assert.That(Strings(a), Is.EqualTo(new[] { "a", "b", "c" }));
    }
}