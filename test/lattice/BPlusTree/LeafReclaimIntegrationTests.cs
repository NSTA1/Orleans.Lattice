using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for empty-leaf chain reclaim - the direction the B+
/// tree never had (issue 2099).
/// <para>
/// Splitting allocated a leaf whenever a key range grew past one leaf's
/// capacity, and nothing ever took one back when the range shrank. A range
/// that grew to many leaves and was then emptied kept every one of them, so a
/// scan went on paying for the high-water mark of the range rather than for
/// the rows that are live. These tests are written against that quantity
/// directly: each one establishes that the chain is still at its high-water
/// mark after the rows are gone - which is the bug, and is what the tree does
/// without this change - and only then asserts that a reclaim pass brings it
/// down. A test that merely asserted the chain was short afterwards would
/// pass on a tree that had never grown.
/// </para>
/// <para>
/// The other half is that reclaim must never buy a shorter chain with a
/// corrupt tree. Folding a leaf out moves a key range from one leaf to
/// another, and the WAL materialiser filters records by exactly the span a
/// leaf declares it owns, so a range left claimed by nobody loses writes on
/// the next projection rebuild and a range claimed by two leaves materialises
/// twice. The contiguity, routing and re-write assertions below are what pin
/// that down, and the growth-after-reclaim case checks that the direction
/// which always worked still does.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafReclaimIntegrationTests
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

    private async Task<(ILattice Router, IShardRootGrain Shard)> CreateSingleShardTreeAsync(string treeName)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeName, new TreeRegistryEntry
        {
            ShardCount = 1,
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
        });
        return (_cluster.GrainFactory.GetGrain<ILattice>(treeName),
                _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0"));
    }

    /// <summary>Walks the sibling chain from the leftmost leaf and returns every leaf id in order.</summary>
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
    /// Asserts that consecutive leaves in the chain tile the keyspace with no
    /// gap and no overlap. This is the invariant reclaim is most able to break:
    /// a gap is a range routed to a leaf whose replay filter rejects it (writes
    /// survive in cache and vanish on rebuild), and an overlap is a record two
    /// leaves both materialise.
    /// </summary>
    private async Task AssertChainTilesKeyspaceAsync(List<GrainId> chain, string because)
    {
        for (var i = 0; i < chain.Count - 1; i++)
        {
            var here = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i]).GetKeyRangeAsync();
            var next = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i + 1]).GetKeyRangeAsync();

            Assert.That(here.HighKeyExclusive, Is.EqualTo(next.LowKeyInclusive),
                $"{because}: leaf {i} ends at '{here.HighKeyExclusive}' but leaf {i + 1} begins at '{next.LowKeyInclusive}', so that span is owned by {(here.HighKeyExclusive is null ? "both" : "nobody")}");
        }
    }

    // --- the bug, and the fix ---

    /// <summary>
    /// The headline case. Without the reclaim path the second assertion here
    /// is the tree's permanent state: the rows are gone and every leaf that
    /// ever held them remains.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_shortens_a_chain_whose_range_has_been_emptied()
    {
        var treeName = $"reclaim-shrink-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);

        var grown = await WalkChainAsync(shard);
        Assert.That(grown.Count, Is.GreaterThan(8),
            "precondition: the seed must actually have split the tree, or this test proves nothing");

        await router.DeleteRangeAsync("k030", "k090");
        Assert.That(await router.CountAsync(), Is.EqualTo(60), "precondition: exactly the middle range was deleted");

        // The bug, stated as an assertion. Every leaf the deleted range ever
        // occupied is still in the chain, still an activation to schedule and
        // still a hop in every scan that crosses it.
        var afterDelete = await WalkChainAsync(shard);
        Assert.That(afterDelete.Count, Is.EqualTo(grown.Count),
            "precondition: emptying a range does not itself shorten the chain - that is the defect under test");

        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        Assert.That(reclaimed, Is.GreaterThan(0), "the emptied range should have yielded reclaimable leaves");

        var afterReclaim = await WalkChainAsync(shard);
        Assert.That(afterReclaim.Count, Is.EqualTo(afterDelete.Count - reclaimed),
            "the chain must be shorter by exactly the number of leaves the pass reported folding");
    }

    /// <summary>
    /// A shorter chain is worthless if it lost a row. The surviving data must
    /// be readable by point read and visible to a scan, which are different
    /// paths: a point read descends the tree, a scan walks the chain.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_preserves_every_surviving_row()
    {
        var treeName = $"reclaim-survive-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        Assert.That(await router.CountAsync(), Is.EqualTo(60));

        for (var i = 0; i < 120; i++)
        {
            var value = await router.GetAsync($"k{i:D3}");
            if (i is >= 30 and < 90)
                Assert.That(value, Is.Null, $"k{i:D3} was deleted and must not resurface");
            else
                Assert.That(value, Is.Not.Null.And.EqualTo(Encoding.UTF8.GetBytes($"v{i}")), $"k{i:D3} must survive the reclaim");
        }

        var scanned = new List<string>();
        await foreach (var key in router.KeysAsync())
            scanned.Add(key);

        Assert.That(scanned.Count, Is.EqualTo(60), "a chain walk must see exactly the surviving rows");
        Assert.That(scanned, Is.Ordered, "folding leaves out must not disorder the chain");
    }

    /// <summary>
    /// The corruption test. After the fold the vacated range belongs to the
    /// predecessor, so a write into it must route there, persist, and be
    /// visible to a scan. If the parent separator had not been removed the
    /// write would route to a leaf that is no longer in the chain: readable by
    /// point read, invisible to every scan, and gone on the next rebuild.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_leaves_the_vacated_range_writable_and_scannable()
    {
        var treeName = $"reclaim-rewrite-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);
        Assert.That(reclaimed, Is.GreaterThan(0), "precondition: something was folded, or this proves nothing");

        await router.SetAsync("k050", Encoding.UTF8.GetBytes("rewritten"));

        Assert.That(await router.GetAsync("k050"), Is.EqualTo(Encoding.UTF8.GetBytes("rewritten")),
            "a point read must find the row written into the reclaimed range");

        var scanned = new List<string>();
        await foreach (var key in router.KeysAsync())
            scanned.Add(key);

        Assert.That(scanned, Does.Contain("k050"),
            "a chain walk must see the row too - if it does not, the write landed on a leaf no longer in the chain");
        Assert.That(await router.CountAsync(), Is.EqualTo(61));
    }

    /// <summary>
    /// A gap or an overlap between consecutive leaves is the silent-loss shape
    /// this pass could introduce, so it is asserted across the whole chain
    /// rather than at the fold site.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_leaves_the_chain_tiling_the_keyspace()
    {
        var treeName = $"reclaim-tiling-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);

        await AssertChainTilesKeyspaceAsync(await WalkChainAsync(shard), "before reclaim");

        await router.DeleteRangeAsync("k030", "k090");
        await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        await AssertChainTilesKeyspaceAsync(await WalkChainAsync(shard), "after reclaim");
    }

    // --- bounds and refusals ---

    [Test]
    public async Task ReclaimEmptyLeaves_folds_no_more_than_the_requested_number()
    {
        var treeName = $"reclaim-bound-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var before = (await WalkChainAsync(shard)).Count;
        var reclaimed = await shard.ReclaimEmptyLeavesAsync(2);

        Assert.That(reclaimed, Is.EqualTo(2), "the pass must honour its bound so it cannot hold an activation turn open");
        Assert.That((await WalkChainAsync(shard)).Count, Is.EqualTo(before - 2));
    }

    /// <summary>
    /// Reclaim is background work that will be re-driven, so a pass over a
    /// chain it has already folded must be a no-op rather than eroding leaves
    /// that hold rows.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_is_idempotent()
    {
        var treeName = $"reclaim-idempotent-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var first = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);
        Assert.That(first, Is.GreaterThan(0));

        var settled = (await WalkChainAsync(shard)).Count;
        var second = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        Assert.That(second, Is.Zero, "a settled chain has nothing left to fold");
        Assert.That((await WalkChainAsync(shard)).Count, Is.EqualTo(settled));
        Assert.That(await router.CountAsync(), Is.EqualTo(60));
    }

    /// <summary>
    /// A leaf holding live rows is never a candidate however short the chain
    /// would become. The whole range is populated here, so a pass that folded
    /// anything would be moving rows, which this pass is explicitly not
    /// allowed to do.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_folds_nothing_when_every_leaf_holds_rows()
    {
        var treeName = $"reclaim-populated-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);

        var before = await WalkChainAsync(shard);
        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        Assert.That(reclaimed, Is.Zero);
        Assert.That(await WalkChainAsync(shard), Is.EqualTo(before));
        Assert.That(await router.CountAsync(), Is.EqualTo(120));
    }

    /// <summary>
    /// The head leaf owns everything below the tree's first separator and has
    /// no predecessor to inherit that range, so emptying it must not remove
    /// it. Deleting the whole keyspace is the strongest form of this: every
    /// leaf is empty, and exactly one must remain.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_never_folds_the_head_leaf()
    {
        var treeName = $"reclaim-head-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);

        var head = await shard.GetLeftmostLeafIdAsync();
        await router.DeleteRangeAsync("k000", "k999");
        Assert.That(await router.CountAsync(), Is.Zero, "precondition: the tree is empty");

        await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        var chain = await WalkChainAsync(shard);
        Assert.That(chain, Is.Not.Empty, "a tree must always retain a leaf to route to");
        Assert.That(chain[0], Is.EqualTo(head), "the head leaf must be the survivor");

        var range = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[0]).GetKeyRangeAsync();
        Assert.That(range.LowKeyInclusive, Is.Null, "the surviving head must still be the catch-all for the range below the first separator");
    }

    // --- the direction that already worked ---

    /// <summary>
    /// Reclaim must not be bought at the cost of growth. A tree that has been
    /// folded down has to split again exactly as it did before, so this
    /// re-seeds the emptied range and requires the chain to grow back and the
    /// rows to be readable.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_leaves_the_tree_able_to_split_again()
    {
        var treeName = $"reclaim-regrow-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");
        await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        var folded = (await WalkChainAsync(shard)).Count;

        for (var i = 30; i < 90; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"w{i}"));

        var regrown = await WalkChainAsync(shard);
        Assert.That(regrown.Count, Is.GreaterThan(folded), "re-filling the range must split the tree back out");
        await AssertChainTilesKeyspaceAsync(regrown, "after regrowth");

        Assert.That(await router.CountAsync(), Is.EqualTo(120));
        for (var i = 30; i < 90; i++)
            Assert.That(await router.GetAsync($"k{i:D3}"), Is.EqualTo(Encoding.UTF8.GetBytes($"w{i}")));
    }
}
