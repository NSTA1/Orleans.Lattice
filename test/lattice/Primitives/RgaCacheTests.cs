using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Covers the two performance caches added to <see cref="Rga"/>: the
/// serialized per-replica dot-context cache that makes
/// <see cref="Rga.InsertAfter(OrSetDot, string, byte[])"/> O(1) in the
/// counter lookup (issue 1211), and the transient materialisation cache that
/// makes repeated <see cref="Rga.ToList"/> reads O(1) between mutations
/// (issue 1212). Both must stay consistent across every mutation, clone, and
/// legacy-payload load without changing the observable CRDT semantics.
/// </summary>
[TestFixture]
public class RgaCacheTests
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string S(byte[] b) => Encoding.UTF8.GetString(b);

    private static IReadOnlyList<string> Strings(Rga r) =>
        r.ToList().Select(t => S(t.Value)).ToArray();

    // ── 1211: per-replica dot-context cache ─────────────────────

    [Test]
    public void Context_tracks_the_highest_counter_per_replica()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a")); // r1 -> 1
        r.InsertAfter(Rga.Root, "r2", B("b")); // r2 -> 1
        r.InsertAfter(Rga.Root, "r1", B("c")); // r1 -> 2

        Assert.Multiple(() =>
        {
            Assert.That(r.Context["r1"], Is.EqualTo(2));
            Assert.That(r.Context["r2"], Is.EqualTo(1));
        });
    }

    [Test]
    public void NextCounter_is_rebuilt_lazily_from_nodes_on_a_legacy_payload()
    {
        // Simulate a payload serialized before the Context field existed:
        // nodes are present but Context is empty. The next local insert must
        // still mint a counter strictly greater than the highest observed.
        var r = new Rga();
        r.Nodes.Add(new RgaNode { ReplicaId = "r1", Counter = 5, ParentDot = Rga.Root, Value = B("x") });
        r.Nodes.Add(new RgaNode { ReplicaId = "r1", Counter = 7, ParentDot = Rga.Root, Value = B("y") });
        Assume.That(r.Context, Is.Empty);

        var dot = r.InsertAfter(Rga.Root, "r1", B("z"));

        Assert.That(dot.Counter, Is.EqualTo(8));
    }

    [Test]
    public void MergeFrom_folds_the_other_sides_context_so_the_next_insert_does_not_collide()
    {
        var local = new Rga();
        local.InsertAfter(Rga.Root, "r1", B("a")); // r1 -> 1

        var other = new Rga();
        other.InsertAfter(Rga.Root, "r1", B("b")); // r1 -> 1
        other.InsertAfter(Rga.Root, "r1", B("c")); // r1 -> 2
        other.InsertAfter(Rga.Root, "r1", B("d")); // r1 -> 3

        local.MergeFrom(other);
        Assert.That(local.Context["r1"], Is.EqualTo(3));

        // The next local insert must not reuse any counter already present.
        var dot = local.InsertAfter(Rga.Root, "r1", B("e"));
        Assert.That(dot.Counter, Is.EqualTo(4));
    }

    [Test]
    public void MergeFrom_folds_a_legacy_other_without_a_context()
    {
        var local = new Rga();
        local.InsertAfter(Rga.Root, "r1", B("a"));

        // Legacy other: nodes present, Context empty.
        var other = new Rga();
        other.Nodes.Add(new RgaNode { ReplicaId = "r1", Counter = 9, ParentDot = Rga.Root, Value = B("b") });
        Assume.That(other.Context, Is.Empty);

        local.MergeFrom(other);

        Assert.That(local.Context["r1"], Is.EqualTo(9));
        var dot = local.InsertAfter(Rga.Root, "r1", B("c"));
        Assert.That(dot.Counter, Is.EqualTo(10));
    }

    [Test]
    public void MergeDelta_bumps_context_from_inserts_and_tombstones()
    {
        var r = new Rga();
        r.MergeDelta(new RgaDelta
        {
            Inserts = new[]
            {
                new RgaDeltaNode { ReplicaId = "r1", Counter = 4, ParentDot = Rga.Root, Value = B("a") },
            },
            Tombstones = new[] { new OrSetDot { ReplicaId = "r2", Counter = 6 } },
        });

        Assert.Multiple(() =>
        {
            Assert.That(r.Context["r1"], Is.EqualTo(4));
            Assert.That(r.Context["r2"], Is.EqualTo(6));
        });

        // A subsequent local insert on r1 respects the folded maximum.
        var dot = r.InsertAfter(Rga.Root, "r1", B("b"));
        Assert.That(dot.Counter, Is.EqualTo(5));
    }

    [Test]
    public void Clone_copies_the_context_and_is_independent()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a"));
        r.InsertAfter(Rga.Root, "r1", B("b")); // r1 -> 2

        var clone = r.Clone();
        Assert.That(clone.Context["r1"], Is.EqualTo(2));

        // Mutating the clone's counter state must not bleed into the original.
        clone.InsertAfter(Rga.Root, "r1", B("c")); // clone r1 -> 3
        Assert.Multiple(() =>
        {
            Assert.That(clone.Context["r1"], Is.EqualTo(3));
            Assert.That(r.Context["r1"], Is.EqualTo(2));
        });
    }

    // ── 1212: transient materialisation cache ───────────────────

    [Test]
    public void ToList_returns_the_same_instance_on_repeated_reads()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a"));

        var first = r.ToList();
        var second = r.ToList();
        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void ToList_caches_the_empty_result()
    {
        var r = new Rga();
        var first = r.ToList();
        var second = r.ToList();
        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Empty);
            Assert.That(second, Is.SameAs(first));
        });
    }

    [Test]
    public void InsertAfter_invalidates_the_materialisation_cache()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a"));
        var before = r.ToList();

        r.InsertAfter(Rga.Root, "r1", B("b"));
        var after = r.ToList();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.Not.SameAs(before));
            Assert.That(after.Select(t => S(t.Value)), Is.EqualTo(new[] { "b", "a" }));
        });
    }

    [Test]
    public void Remove_invalidates_the_materialisation_cache()
    {
        var r = new Rga();
        var d = r.InsertAfter(Rga.Root, "r1", B("a"));
        r.InsertAfter(Rga.Root, "r1", B("b"));
        var before = r.ToList();

        r.Remove(d);
        var after = r.ToList();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.Not.SameAs(before));
            Assert.That(after.Select(t => S(t.Value)), Is.EqualTo(new[] { "b" }));
        });
    }

    [Test]
    public void MergeFrom_invalidates_the_materialisation_cache()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a"));
        var before = r.ToList();

        var other = new Rga();
        other.InsertAfter(Rga.Root, "r2", B("b"));
        r.MergeFrom(other);
        var after = r.ToList();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.Not.SameAs(before));
            Assert.That(after.Select(t => S(t.Value)), Is.EqualTo(new[] { "b", "a" }));
        });
    }

    [Test]
    public void MergeDelta_invalidates_the_materialisation_cache()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a"));
        var before = r.ToList();

        r.MergeDelta(new RgaDelta
        {
            Inserts = new[]
            {
                new RgaDeltaNode { ReplicaId = "r2", Counter = 1, ParentDot = Rga.Root, Value = B("b") },
            },
            Tombstones = Array.Empty<OrSetDot>(),
        });
        var after = r.ToList();

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.Not.SameAs(before));
            Assert.That(after.Select(t => S(t.Value)), Is.EqualTo(new[] { "b", "a" }));
        });
    }

    [Test]
    public void Clone_does_not_share_the_materialisation_cache()
    {
        var r = new Rga();
        r.InsertAfter(Rga.Root, "r1", B("a"));
        _ = r.ToList(); // warm the original's cache

        var clone = r.Clone();
        clone.InsertAfter(Rga.Root, "r1", B("b"));

        // The clone's mutation must not corrupt the original's cached read.
        Assert.Multiple(() =>
        {
            Assert.That(Strings(r), Is.EqualTo(new[] { "a" }));
            Assert.That(Strings(clone), Is.EqualTo(new[] { "b", "a" }));
        });
    }
}
