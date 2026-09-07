using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Diagnostics;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for the <em>wiring</em> of empty-leaf reclaim, as distinct from the
/// mechanism (issue 2099).
/// <para>
/// The mechanism is covered by <see cref="LeafReclaimIntegrationTests"/>, and
/// that suite has a blind spot it cannot see past: every one of its cases
/// invokes the shard root's reclaim entry point by name. Such a suite proves
/// the fold is correct <em>once invoked</em> and is structurally incapable of
/// observing whether anything invokes it. A reclaim mechanism with no trigger
/// passes all of it while leaving the reported defect fully present in the
/// field, because a range that shrinks in production calls nothing.
/// </para>
/// <para>
/// This fixture therefore never names the reclaim entry point. It shrinks a
/// range and then drives only the production trigger - a compaction tick,
/// which is the same <c>ProcessNextShardAsync</c> path the periodic
/// <c>tombstone-compaction</c> reminder drives - and asserts the chain
/// shortened on its own. Delete the reclaim call from
/// <c>TombstoneCompactionGrain</c> and these tests go red; no test in the
/// mechanism suite does.
/// </para>
/// <para>
/// Compaction is the trigger because compaction is what creates the condition:
/// a leaf becomes empty when the grace period expires and its last tombstones
/// are swept. The reclaim pass therefore runs exactly when new candidates can
/// exist, and needs no reminder of its own.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class TombstoneCompactionReclaimWiringIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<(ILattice Router, IShardRootGrain Shard, ITombstoneCompactionGrain Compaction)>
        CreateSingleShardTreeAsync(string treeName)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry
        {
            ShardCount = 1,
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
        });
        return (_cluster.GrainFactory.GetGrain<ILattice>(treeName),
                _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0"),
                _cluster.GrainFactory.GetGrain<ITombstoneCompactionGrain>(treeName));
    }

    private async Task<List<GrainId>> WalkChainAsync(IShardRootGrain shard)
    {
        var chain = new List<GrainId>();
        var leafId = await shard.GetLeftmostLeafIdAsync();

        while (leafId is { } id && chain.Count < 5_000)
        {
            chain.Add(id);
            leafId = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        return chain;
    }

    private async Task SeedAsync(ILattice router, int count)
    {
        for (var i = 0; i < count; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
    }

    /// <summary>
    /// Drives one compaction tick and waits for the pass to run to completion.
    /// The pass is timer-driven (one shard per tick), so the trigger call
    /// returns before the work is done and the caller must wait on an
    /// observable outcome rather than on the call itself.
    /// </summary>
    private static async Task<bool> WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < timeout)
        {
            if (await condition()) return true;
            await Task.Delay(100);
        }

        return await condition();
    }

    /// <summary>
    /// The finding this fixture exists for. A range grows, is emptied, and the
    /// chain must come back down <em>without anyone asking it to</em> - only a
    /// compaction tick is driven here.
    /// </summary>
    [Test]
    public async Task Compaction_tick_reclaims_the_leaves_an_emptied_range_left_behind()
    {
        var treeName = $"reclaim-wiring-{Guid.NewGuid():N}";
        var (router, shard, compaction) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);

        var grown = await WalkChainAsync(shard);
        Assert.That(grown.Count, Is.GreaterThan(8),
            "precondition: the seed must actually have split the tree, or this test proves nothing");

        await router.DeleteRangeAsync("k030", "k090");
        Assert.That(await router.CountAsync(), Is.EqualTo(60),
            "precondition: exactly the middle range was deleted");

        var afterDelete = await WalkChainAsync(shard);
        Assert.That(afterDelete.Count, Is.EqualTo(grown.Count),
            "precondition: emptying a range does not itself shorten the chain");

        // The only thing driven. No reclaim entry point is named anywhere in
        // this fixture: if the compaction grain does not invoke reclaim, the
        // chain below never shortens and this test fails.
        var honoured = await compaction.RequestCompactionAsync(0, "operator");
        Assert.That(honoured, Is.True, "precondition: the compaction request must have been accepted");

        var shortened = await WaitUntilAsync(
            async () => (await WalkChainAsync(shard)).Count < afterDelete.Count,
            TimeSpan.FromSeconds(60));

        Assert.That(shortened, Is.True,
            "a compaction pass must reclaim the leaves the emptied range left behind; "
            + "if this fails, the reclaim mechanism exists but nothing in production invokes it");
    }

    /// <summary>
    /// A shorter chain obtained by a background trigger is worth nothing if it
    /// cost a row. The surviving data must still be readable and countable
    /// after the pass folded leaves out underneath it.
    /// </summary>
    [Test]
    public async Task Compaction_driven_reclaim_preserves_every_surviving_row()
    {
        var treeName = $"reclaim-wiring-rows-{Guid.NewGuid():N}";
        var (router, shard, compaction) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var afterDelete = await WalkChainAsync(shard);
        await compaction.RequestCompactionAsync(0, "operator");
        var shortened = await WaitUntilAsync(
            async () => (await WalkChainAsync(shard)).Count < afterDelete.Count,
            TimeSpan.FromSeconds(60));

        // Without this the case is vacuous: it would pass on a tree where the
        // pass folded nothing, and so would assert data integrity against the
        // pre-reclaim tree rather than the post-reclaim one.
        Assert.That(shortened, Is.True,
            "precondition: the compaction pass must actually have folded leaves, "
            + "or the assertions below are about a tree reclaim never touched");

        Assert.That(await router.CountAsync(), Is.EqualTo(60),
            "the reclaim pass must not have changed how many rows are live");

        for (var i = 0; i < 30; i++)
        {
            var value = await router.GetAsync($"k{i:D3}");
            Assert.That(value, Is.Not.Null, $"row k{i:D3} below the deleted range must survive the pass");
            Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo($"v{i}"));
        }

        for (var i = 90; i < 120; i++)
        {
            var value = await router.GetAsync($"k{i:D3}");
            Assert.That(value, Is.Not.Null, $"row k{i:D3} above the deleted range must survive the pass");
            Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo($"v{i}"));
        }
    }

    /// <summary>
    /// The vacated range must remain writable after a background pass folded
    /// its leaves away, which is the invariant a mis-ordered fold breaks: a
    /// range claimed by nobody accepts a write into cache and loses it on the
    /// next projection rebuild.
    /// </summary>
    [Test]
    public async Task Compaction_driven_reclaim_leaves_the_vacated_range_writable()
    {
        var treeName = $"reclaim-wiring-rewrite-{Guid.NewGuid():N}";
        var (router, shard, compaction) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var afterDelete = await WalkChainAsync(shard);
        await compaction.RequestCompactionAsync(0, "operator");
        var shortened = await WaitUntilAsync(
            async () => (await WalkChainAsync(shard)).Count < afterDelete.Count,
            TimeSpan.FromSeconds(60));

        Assert.That(shortened, Is.True,
            "precondition: the compaction pass must actually have folded leaves, "
            + "or the rewrite below is not exercising a vacated range at all");

        await router.SetAsync("k050", Encoding.UTF8.GetBytes("rewritten"));

        var readBack = await router.GetAsync("k050");
        Assert.That(readBack, Is.Not.Null, "a key in the vacated range must be writable again");
        Assert.That(Encoding.UTF8.GetString(readBack!), Is.EqualTo("rewritten"));
        Assert.That(await router.CountAsync(), Is.EqualTo(61),
            "the rewritten key must be visible to a chain-walking scan, not only to a point read");
    }

    /// <summary>
    /// A tree whose leaves all still hold rows must come through a compaction
    /// tick with its chain intact. This is the counterpart to the headline
    /// case: it fails if the wiring folds leaves indiscriminately rather than
    /// only the empty ones.
    /// </summary>
    [Test]
    public async Task Compaction_tick_folds_nothing_when_every_leaf_holds_rows()
    {
        var treeName = $"reclaim-wiring-noop-{Guid.NewGuid():N}";
        var (router, shard, compaction) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 60);

        var before = await WalkChainAsync(shard);
        Assert.That(before.Count, Is.GreaterThan(4), "precondition: the tree must have split");

        await compaction.RequestCompactionAsync(0, "operator");
        await Task.Delay(TimeSpan.FromSeconds(3));

        var after = await WalkChainAsync(shard);
        Assert.That(after.Count, Is.EqualTo(before.Count),
            "no leaf was empty, so a compaction pass must not have folded any");
        Assert.That(await router.CountAsync(), Is.EqualTo(60), "and no row may have been lost");
    }
}
