using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// #4586 on the real site-B tree: a causal dependency names one write, so an
/// entry must not apply before that write does, even when the origin's
/// high-water mark is already above it. Per-leaf clocks are unordered and WAL
/// partitions interleave, so a later write of the origin routinely arrives
/// first (#1060). This is the 7-state TLC trace of the issue.
/// </summary>
public partial class ReplicationApplyIntegrationTests
{
    private static HybridLogicalClock RecentHlc(long ticksAgo) =>
        new() { WallClockTicks = DateTime.UtcNow.Ticks - ticksAgo, Counter = 0 };

    private static WalRecord DependencyOriginSet(string tree, string key, HybridLogicalClock ts) =>
        LwwSet(tree, key, new byte[] { 3 }, ts) with { OriginClusterId = DependencyOrigin };

    [Test]
    public async Task An_entry_does_not_apply_before_its_dependency_when_a_later_write_of_the_origin_arrived_first()
    {
        const string tree = "ri-causal-order";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();
        var a1 = RecentHlc(TimeSpan.TicksPerSecond * 2);
        var a2 = RecentHlc(TimeSpan.TicksPerSecond);

        // a2 takes the earlier shipping position and lands first: the
        // origin's high-water mark is now above a1.
        Assert.That((await applier.ApplyAsync(DependencyOriginSet(tree, "a2", a2))).Applied, Is.True);

        // b2 was authored after its author learned a1.
        var b2 = LwwSet(tree, "b2", new byte[] { 2 }, RecentHlc(0)) with { VectorClock = DependsOn(a1) };
        var b2Result = await applier.ApplyAsync(b2);

        var b2BeforeA1 = (await lattice.GetWithVersionAsync("b2")).Value;
        var parkedBeforeA1 = await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(tree).CountAsync();
        Assert.Multiple(() =>
        {
            Assert.That(b2Result.Applied, Is.False, "b2 depends on a1, which has not arrived.");
            Assert.That(b2BeforeA1, Is.Null);
            Assert.That(parkedBeforeA1, Is.EqualTo(1));
        });

        // a1 arrives below the high-water mark. Its identity alone releases
        // b2, without waiting for a maintenance tick.
        Assert.That((await applier.ApplyAsync(DependencyOriginSet(tree, "a1", a1))).Applied, Is.True);

        var b2AfterA1 = (await lattice.GetWithVersionAsync("b2")).Value;
        var parkedAfterA1 = await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(tree).CountAsync();
        Assert.Multiple(() =>
        {
            Assert.That(b2AfterA1, Is.EqualTo(new byte[] { 2 }));
            Assert.That(parkedAfterA1, Is.Zero);
        });
    }

    [Test]
    public async Task A_batched_entry_does_not_apply_before_its_dependency_when_a_later_write_of_the_origin_arrived_first()
    {
        const string tree = "ri-causal-order-batch";
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var applier = CreateSiteBApplier();
        var a1 = RecentHlc(TimeSpan.TicksPerSecond * 2);
        var a2 = RecentHlc(TimeSpan.TicksPerSecond);

        await applier.ApplyBatchAsync([DependencyOriginSet(tree, "a2", a2)]);
        var b2 = LwwSet(tree, "b2", new byte[] { 2 }, RecentHlc(0)) with { VectorClock = DependsOn(a1) };
        await applier.ApplyBatchAsync([b2]);

        Assert.That((await lattice.GetWithVersionAsync("b2")).Value, Is.Null, "b2 depends on a1, which has not arrived.");

        await applier.ApplyBatchAsync([DependencyOriginSet(tree, "a1", a1)]);

        var b2AfterA1 = (await lattice.GetWithVersionAsync("b2")).Value;
        var parkedAfterA1 = await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(tree).CountAsync();
        Assert.Multiple(() =>
        {
            Assert.That(b2AfterA1, Is.EqualTo(new byte[] { 2 }));
            Assert.That(parkedAfterA1, Is.Zero);
        });
    }

    [Test]
    public async Task A_dependency_on_a_write_another_tree_applied_waits_for_the_origin_low_watermark()
    {
        // Each low-watermark test uses its own origin: the frontier is per origin
        // and outlives the test on the shared fixture.
        const string origin = "site-f";
        const string dependencyTree = "ri-causal-cross-dep";
        const string dependentTree = "ri-causal-cross-main";
        var applier = CreateSiteBApplier();
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(dependentTree);
        var a1 = RecentHlc(TimeSpan.TicksPerSecond * 3);

        // a1 lands in another tree: this tree's identity record never sees it.
        Assert.That((await applier.ApplyAsync(OriginSet(origin, dependencyTree, "a1", a1))).Applied, Is.True);
        var b = LwwSet(dependentTree, "b", new byte[] { 2 }, RecentHlc(0)) with { VectorClock = DependsOnOrigin(origin, a1) };
        Assert.That((await applier.ApplyAsync(b)).Applied, Is.False, "nothing yet shows every origin write up to a1 arrived");

        // The origin's low watermark passes a1: every write of the origin below
        // it was acknowledged here, and none is held.
        await _fixture.SiteB.Client.GetGrain<IReplicationOriginFrontierGrain>(origin)
            .RecordLowWatermarkAsync(RecentHlc(TimeSpan.TicksPerSecond), generation: 0, CancellationToken.None);
        await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(dependentTree).DrainAsync();

        Assert.That((await lattice.GetWithVersionAsync("b")).Value, Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public async Task A_write_held_in_another_trees_buffer_keeps_its_dependent_parked_past_the_low_watermark()
    {
        const string origin = "site-g";
        const string dependencyTree = "ri-causal-held-dep";
        const string dependentTree = "ri-causal-held-main";
        const string otherOrigin = "site-e";
        var applier = CreateSiteBApplier();
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(dependentTree);
        var x1 = RecentHlc(TimeSpan.TicksPerSecond * 4);
        var a1 = RecentHlc(TimeSpan.TicksPerSecond * 3);

        // a1 arrives before its own dependency x1, so it is acknowledged and parked.
        var a1Entry = OriginSet(origin, dependencyTree, "a1", a1) with { VectorClock = DependsOnOrigin(otherOrigin, x1) };
        Assert.That((await applier.ApplyAsync(a1Entry)).Applied, Is.False);
        var b = LwwSet(dependentTree, "b", new byte[] { 2 }, RecentHlc(0)) with { VectorClock = DependsOnOrigin(origin, a1) };
        Assert.That((await applier.ApplyAsync(b)).Applied, Is.False);

        // The low watermark passes a1, but a1 is held, not applied.
        await _fixture.SiteB.Client.GetGrain<IReplicationOriginFrontierGrain>(origin)
            .RecordLowWatermarkAsync(RecentHlc(TimeSpan.TicksPerSecond), generation: 0, CancellationToken.None);
        await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(dependentTree).DrainAsync();
        Assert.That((await lattice.GetWithVersionAsync("b")).Value, Is.Null, "acknowledged is not applied while a1 is parked");

        // x1 arrives: a1 drains, its tree releases it, and b follows.
        var x1Entry = LwwSet(dependencyTree, "x1", new byte[] { 9 }, x1) with { OriginClusterId = otherOrigin };
        Assert.That((await applier.ApplyAsync(x1Entry)).Applied, Is.True);
        await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(dependentTree).DrainAsync();

        Assert.That((await lattice.GetWithVersionAsync("b")).Value, Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public async Task A_dependent_parks_after_the_receiver_tree_is_rolled_back_to_before_its_dependency()
    {
        const string origin = "site-h";
        const string tree = "ri-causal-lineage";
        const string emptyCopy = "ri-causal-lineage-restored";
        var applier = CreateSiteBApplier();
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var w = RecentHlc(TimeSpan.TicksPerSecond * 2);

        // w is applied, and the tree records its identity.
        Assert.That((await applier.ApplyAsync(OriginSet(origin, tree, "w", w))).Applied, Is.True);

        // Roll the receiver's tree back to before w: repoint it onto a copy that
        // never held w, which is the alias swap a shadow-cutover restore or a
        // revert performs. The registry's alias observers run on the swap.
        await _fixture.SiteB.Client.GetGrain<ILattice>(emptyCopy).SetAsync("seed", new byte[] { 0 });
        var registry = _fixture.SiteB.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(tree, emptyCopy);
        Assert.That(await registry.GetAliasesTargetingAsync(emptyCopy), Does.Contain(tree), "precondition: the tree now resolves to the copy");

        // A dependent of w must not be released on the record from before.
        var b = LwwSet(tree, "b", new byte[] { 2 }, RecentHlc(0)) with { VectorClock = DependsOnOrigin(origin, w) };
        var result = await applier.ApplyAsync(b);
        var parked = await _fixture.SiteB.Client.GetGrain<ICausalApplyBufferGrain>(tree).CountAsync();

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False, "w is not in the restored tree, so b must park");
            Assert.That(parked, Is.EqualTo(1));
        });
    }

    private static WalRecord OriginSet(string origin, string tree, string key, HybridLogicalClock ts) =>
        LwwSet(tree, key, new byte[] { 3 }, ts) with { OriginClusterId = origin };

    private static VersionVector DependsOnOrigin(string origin, HybridLogicalClock dependency)
    {
        var vc = new VersionVector();
        vc.Entries[origin] = dependency;
        return vc;
    }

    private static VersionVector DependsOn(HybridLogicalClock dependency)
    {
        var vc = new VersionVector();
        vc.Entries[DependencyOrigin] = dependency;
        return vc;
    }
}
