using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for the compare half of
/// <c>IBPlusLeafGrain.TryUnlinkSuccessorAsync</c> - the guard that stops empty-leaf
/// reclaim from destroying a leaf a concurrent split has just created.
/// <para>
/// Reclaim is a multi-grain sequence and the split gate is per-grain, so the
/// two are NOT serialised with respect to each other. Between the shard root
/// reading a predecessor's sibling pointer and writing it, a split of that
/// predecessor can land and insert a brand new leaf between it and the
/// successor being folded away - carrying with it the rows the split has just
/// moved into it. A blind write of the pointer the reclaim had planned would
/// step straight past that leaf, unlinking it from the chain and losing rows
/// that were live the whole time. That is silent data loss caused by the
/// reclaim path, in the growth direction, which is the direction that was
/// already correct.
/// </para>
/// <para>
/// These tests drive that interleaving with a real split rather than a
/// synthetic pointer change, and the inserted leaf holds real rows, so they
/// are evidence for the claim the guard's docstring makes rather than merely
/// adjacent to it. Reverting the comparison to an unconditional write makes
/// every one of them fail.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafReclaimSplitRaceIntegrationTests
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

    private IBPlusLeafGrain Leaf(GrainId id) => _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id);

    /// <summary>
    /// Builds the exact interleaving: a head leaf whose successor a reclaim has
    /// already observed, then a genuine split of that head which inserts a new
    /// leaf holding live rows between the two.
    /// </summary>
    private async Task<(GrainId Head, GrainId StaleNext, GrainId Inserted, List<string> InsertedKeys)>
        BuildSplitUnderneathAsync(ILattice router, IShardRootGrain shard)
    {
        // Grow past one leaf so the head has a successor at all.
        for (var i = 0; i < 40; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var head = (await shard.GetLeftmostLeafIdAsync())!.Value;

        // What a reclaim pass would have read into its plan just before the
        // split lands.
        var staleNext = (await Leaf(head).GetNextSiblingAsync())!.Value;

        // Force a real split of the head by writing keys that sort below
        // everything already present, so they all route to the head and
        // overflow it. '!' is ordinal-below the digits and letters in use.
        var insertedKeys = new List<string>();
        for (var i = 0; i < 20; i++)
        {
            var key = $"!{i:D3}";
            await router.SetAsync(key, Encoding.UTF8.GetBytes($"split-{i}"));
            insertedKeys.Add(key);
        }

        var inserted = (await Leaf(head).GetNextSiblingAsync())!.Value;

        Assert.That(inserted, Is.Not.EqualTo(staleNext),
            "precondition: the split must genuinely have inserted a new leaf between the head and its old successor");

        return (head, staleNext, inserted, insertedKeys);
    }

    /// <summary>
    /// The inserted leaf must actually hold rows, or the "orphans a leaf
    /// holding rows the split just moved into it" claim would be asserted
    /// nowhere and this fixture would only be proving that an equality check
    /// notices inequality.
    /// </summary>
    private async Task<int> CountRowsInAsync(ILattice router, GrainId leafId)
    {
        var range = await Leaf(leafId).GetKeyRangeAsync();
        return await router.CountAsync(range.LowKeyInclusive, range.HighKeyExclusive);
    }

    [Test]
    public async Task TryUnlinkSuccessor_declines_when_a_split_has_inserted_a_leaf_underneath_the_reclaim()
    {
        var treeName = $"reclaim-race-decline-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        var (head, staleNext, inserted, _) = await BuildSplitUnderneathAsync(router, shard);

        Assert.That(await CountRowsInAsync(router, inserted), Is.GreaterThan(0),
            "precondition: the split moved live rows into the inserted leaf, which is what a blind unlink would lose");

        var staleNextRange = await Leaf(staleNext).GetKeyRangeAsync();

        // The reclaim now tries to fold `staleNext` out using the plan it made
        // before the split landed. It must refuse.
        var unlinked = await Leaf(head).TryUnlinkSuccessorAsync(
            staleNext,
            await Leaf(staleNext).GetNextSiblingAsync(),
            staleNextRange.HighKeyExclusive);

        Assert.That(unlinked, Is.False,
            "the predecessor no longer points at the leaf the reclaim planned to fold, so the fold must be refused");
    }

    /// <summary>
    /// "Reclaim declines" and "reclaim declines leaving a recoverable state"
    /// are different claims, and only the second is the one the design relies
    /// on. The load-bearing assertion is the predecessor's high bound: if the
    /// widen leaked through despite the declined unlink, the predecessor would
    /// claim a range it does not route to - a partially applied fold, and a
    /// distinct silent-loss shape from the orphaned leaf.
    /// </summary>
    [Test]
    public async Task TryUnlinkSuccessor_changes_nothing_when_it_declines()
    {
        var treeName = $"reclaim-race-intact-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        var (head, staleNext, inserted, _) = await BuildSplitUnderneathAsync(router, shard);

        var headRangeBefore = await Leaf(head).GetKeyRangeAsync();
        var staleNextRange = await Leaf(staleNext).GetKeyRangeAsync();

        await Leaf(head).TryUnlinkSuccessorAsync(
            staleNext,
            await Leaf(staleNext).GetNextSiblingAsync(),
            staleNextRange.HighKeyExclusive);

        Assert.That(await Leaf(head).GetNextSiblingAsync(), Is.EqualTo(inserted),
            "the chain must still run through the leaf the split inserted");

        var headRangeAfter = await Leaf(head).GetKeyRangeAsync();
        Assert.That(headRangeAfter.HighKeyExclusive, Is.EqualTo(headRangeBefore.HighKeyExclusive),
            "the widen must not leak through a declined unlink: a predecessor claiming a range it does not route to loses every write into it on the next projection rebuild");
        Assert.That(headRangeAfter.LowKeyInclusive, Is.EqualTo(headRangeBefore.LowKeyInclusive));
    }

    /// <summary>
    /// The point of declining rather than corrupting: the rows the split moved
    /// into the new leaf are still there, still reachable by point read, and
    /// still visible to a chain walk. A blind unlink would leave them readable
    /// by neither.
    /// </summary>
    [Test]
    public async Task TryUnlinkSuccessor_does_not_orphan_the_rows_a_split_moved()
    {
        var treeName = $"reclaim-race-rows-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        var (head, staleNext, _, insertedKeys) = await BuildSplitUnderneathAsync(router, shard);

        var staleNextRange = await Leaf(staleNext).GetKeyRangeAsync();

        await Leaf(head).TryUnlinkSuccessorAsync(
            staleNext,
            await Leaf(staleNext).GetNextSiblingAsync(),
            staleNextRange.HighKeyExclusive);

        foreach (var key in insertedKeys)
        {
            Assert.That(await router.GetAsync(key), Is.Not.Null,
                $"'{key}' was moved by the split and must survive the declined fold");
        }

        var scanned = new List<string>();
        await foreach (var key in router.KeysAsync())
            scanned.Add(key);

        Assert.That(scanned.Count, Is.EqualTo(60), "every row must remain visible to a chain walk");
        Assert.That(scanned, Is.SupersetOf(insertedKeys),
            "the split's rows must still be reachable by walking the chain - if they are not, the fold orphaned the leaf holding them");
    }

    /// <summary>
    /// The recoverable half. A declined fold is not a dead end: once the
    /// topology has settled, a full reclaim pass over the same tree still
    /// works and still preserves every row. If declining left the tree in a
    /// state reclaim could not make progress from, an emptied range would be
    /// stuck at its high-water mark forever.
    /// </summary>
    [Test]
    public async Task A_declined_fold_leaves_the_tree_reclaimable_by_a_later_pass()
    {
        var treeName = $"reclaim-race-recover-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        var (head, staleNext, _, insertedKeys) = await BuildSplitUnderneathAsync(router, shard);

        var staleNextRange = await Leaf(staleNext).GetKeyRangeAsync();
        await Leaf(head).TryUnlinkSuccessorAsync(
            staleNext,
            await Leaf(staleNext).GetNextSiblingAsync(),
            staleNextRange.HighKeyExclusive);

        // Empty the original range, leaving the split's rows in place.
        await router.DeleteRangeAsync("k000", "k999");

        var chainBefore = new List<GrainId>();
        var walk = await shard.GetLeftmostLeafIdAsync();
        while (walk is { } id)
        {
            chainBefore.Add(id);
            walk = await Leaf(id).GetNextSiblingAsync();
        }

        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);

        Assert.That(reclaimed, Is.GreaterThan(0),
            "a settled topology must be foldable again after an earlier decline");

        foreach (var key in insertedKeys)
            Assert.That(await router.GetAsync(key), Is.Not.Null, $"'{key}' must survive the later pass too");

        Assert.That(await router.CountAsync(), Is.EqualTo(insertedKeys.Count));
    }
}
