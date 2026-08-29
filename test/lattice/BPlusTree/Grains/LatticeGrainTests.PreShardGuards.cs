using NUnit.Framework;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for deterministic pre-shard guard clauses on
/// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeGrain"/>: the materialised-view write/read protection
/// (<c>ThrowIfProtectedView</c> / <c>ThrowIfProtectedViewRead</c>) and the TTL
/// range validation on the <see cref="ILattice.ApplyCrdtDeltaAsync"/> TTL
/// overload. Each throws before any shard grain is consulted, so no cluster or
/// shard-root wiring is required.
/// </summary>
public partial class LatticeGrainTests
{
    [Test]
    public void SetAsync_to_view_prefixed_tree_throws_protected_view()
    {
        var (grain, _) = CreateGrain(treeId: "view-orders");

        Assert.That(async () => await grain.SetAsync("k", [1]),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void GetAsync_to_view_prefixed_tree_throws_protected_view_read()
    {
        var (grain, _) = CreateGrain(treeId: "view-orders");

        Assert.That(async () => await grain.GetAsync("k"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void ApplyCrdtDeltaAsync_with_nonpositive_ttl_throws()
    {
        var (grain, _) = CreateGrain();

        Assert.That(async () => await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, [1], TimeSpan.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ApplyCrdtDeltaAsync_with_overflowing_ttl_throws()
    {
        var (grain, _) = CreateGrain();

        Assert.That(async () => await grain.ApplyCrdtDeltaAsync("k", LatticeMergeMode.OrSet, [1], TimeSpan.MaxValue),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }
}
