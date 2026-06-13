using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="BPlusLeafGrain.GetProjectionDigestForRangeAsync"/>.
/// Verifies the unbounded fast path equals the whole-leaf fold, half-open
/// bound semantics, the early-break upper bound, content-convergence
/// (cross-cluster layout independence at the leaf), and that the empty
/// sub-range folds to a zero count.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task RangeDigest_empty_leaf_full_range_has_zero_count_and_16_byte_hash()
    {
        var grain = CreateGrain();

        var snap = await grain.GetProjectionDigestForRangeAsync(null, null);

        Assert.That(snap.EntryCount, Is.Zero);
        Assert.That(snap.Hash, Is.Not.Null);
        Assert.That(snap.Hash!.Length, Is.EqualTo(16));
    }

    [Test]
    public async Task RangeDigest_full_range_equals_whole_leaf_raw_fold()
    {
        // Two leaves with identical content applied at identical HLCs converge on
        // the same raw fold; the full-range probe reproduces that whole-leaf fold.
        var g1 = CreateGrain(replicaId: "leaf-x");
        var g2 = CreateGrain(replicaId: "leaf-x");
        var p1 = (ILeafProjection)g1;
        var p2 = (ILeafProjection)g2;

        p1.Apply(BuildSet("a", Encoding.UTF8.GetBytes("1"), hlcPhysical: 100));
        p1.Apply(BuildSet("b", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));

        p2.Apply(BuildSet("a", Encoding.UTF8.GetBytes("1"), hlcPhysical: 100));
        p2.Apply(BuildSet("b", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));

        var s1 = await g1.GetProjectionDigestForRangeAsync(null, null);
        var s2 = await g2.GetProjectionDigestForRangeAsync(null, null);

        Assert.That(s1.EntryCount, Is.EqualTo(2));
        Assert.That(s1.Hash, Is.EqualTo(s2.Hash));
    }

    [Test]
    public async Task RangeDigest_start_bound_is_inclusive_and_end_bound_is_exclusive()
    {
        var grain = CreateGrain(replicaId: "leaf-x");
        var p = (ILeafProjection)grain;
        p.Apply(BuildSet("a", Encoding.UTF8.GetBytes("1"), hlcPhysical: 100));
        p.Apply(BuildSet("b", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));
        p.Apply(BuildSet("c", Encoding.UTF8.GetBytes("3"), hlcPhysical: 300));

        // [b, c) selects exactly {b}.
        var mid = await grain.GetProjectionDigestForRangeAsync("b", "c");
        Assert.That(mid.EntryCount, Is.EqualTo(1));

        // [null, b) selects exactly {a}.
        var head = await grain.GetProjectionDigestForRangeAsync(null, "b");
        Assert.That(head.EntryCount, Is.EqualTo(1));

        // [b, null) selects {b, c}.
        var tail = await grain.GetProjectionDigestForRangeAsync("b", null);
        Assert.That(tail.EntryCount, Is.EqualTo(2));
    }

    [Test]
    public async Task RangeDigest_subrange_is_content_convergent_across_layout()
    {
        // Leaf 1 holds {a,b,c,d}; leaf 2 holds only {b,c} (as if a different
        // cluster split the same logical range across leaves differently). The
        // [b, d) sub-range of leaf 1 folds {b,c}, which must equal leaf 2's
        // full fold of {b,c} - the layout-independence property at leaf scope.
        var wide = CreateGrain(replicaId: "leaf-x");
        var narrow = CreateGrain(replicaId: "leaf-x");
        var pw = (ILeafProjection)wide;
        var pn = (ILeafProjection)narrow;

        pw.Apply(BuildSet("a", Encoding.UTF8.GetBytes("1"), hlcPhysical: 100));
        pw.Apply(BuildSet("b", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));
        pw.Apply(BuildSet("c", Encoding.UTF8.GetBytes("3"), hlcPhysical: 300));
        pw.Apply(BuildSet("d", Encoding.UTF8.GetBytes("4"), hlcPhysical: 400));

        pn.Apply(BuildSet("b", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));
        pn.Apply(BuildSet("c", Encoding.UTF8.GetBytes("3"), hlcPhysical: 300));

        var wideSub = await wide.GetProjectionDigestForRangeAsync("b", "d");
        var narrowFull = await narrow.GetProjectionDigestForRangeAsync(null, null);

        Assert.That(wideSub.EntryCount, Is.EqualTo(2));
        Assert.That(wideSub.Hash, Is.EqualTo(narrowFull.Hash));
    }

    [Test]
    public async Task RangeDigest_empty_subrange_yields_zero_count()
    {
        var grain = CreateGrain(replicaId: "leaf-x");
        var p = (ILeafProjection)grain;
        p.Apply(BuildSet("a", Encoding.UTF8.GetBytes("1"), hlcPhysical: 100));
        p.Apply(BuildSet("z", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));

        // [m, n) selects nothing.
        var snap = await grain.GetProjectionDigestForRangeAsync("m", "n");

        Assert.That(snap.EntryCount, Is.Zero);
    }
}
