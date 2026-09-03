using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for <c>IShardRootGrain.GetLeafIdForKeyAsync</c>, the
/// resume seam the background coordinators' bounded leaf walks re-descend
/// through (issue 1973).
/// <para>
/// A resume position is a key rather than a leaf grain id precisely so that it
/// can always be resolved against the tree's <em>current</em> shape. These tests
/// exercise that against a real multi-leaf shard rather than a substitute: the
/// descent must land on the leaf that owns the key, agree with the sibling chain
/// so a walk resumed at one leaf's exclusive high bound continues at the next
/// leaf, and keep resolving correctly after the tree has split underneath it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardRootLeafKeyDescentIntegrationTests
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

    [Test]
    public async Task GetLeafIdForKey_returns_null_for_an_uninitialised_shard()
    {
        var treeName = $"keydescent-empty-{Guid.NewGuid():N}";
        var (_, shard) = await CreateSingleShardTreeAsync(treeName);

        Assert.Multiple(async () =>
        {
            Assert.That(await shard.GetLeafIdForKeyAsync(null), Is.Null);
            Assert.That(await shard.GetLeafIdForKeyAsync("anything"), Is.Null);
        });
    }

    [Test]
    public async Task GetLeafIdForKey_with_a_null_key_matches_the_leftmost_leaf()
    {
        var treeName = $"keydescent-leftmost-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        for (var i = 0; i < 40; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var leftmost = await shard.GetLeftmostLeafIdAsync();
        var resolved = await shard.GetLeafIdForKeyAsync(null);

        Assert.That(resolved, Is.EqualTo(leftmost));
    }

    /// <summary>
    /// The property every bounded walk resume depends on: a leaf's exclusive
    /// high bound is exactly where the next leaf begins, so re-descending onto
    /// it lands on that next leaf. If these disagreed, a resumed pass would
    /// either re-walk the leaf it just finished (no forward progress) or skip
    /// past a leaf entirely (silent truncation).
    /// </summary>
    [Test]
    public async Task GetLeafIdForKey_on_a_leafs_high_bound_resolves_to_its_next_sibling()
    {
        var treeName = $"keydescent-chain-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        for (var i = 0; i < 60; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null, "precondition: the tree has leaves");

        var checkedBounds = 0;
        while (leafId is not null)
        {
            var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var next = await leaf.GetNextSiblingAsync();
            if (next is null) break;

            var bounds = await leaf.GetKeyRangeAsync();
            if (bounds.HighKeyExclusive is { } high)
            {
                var resolved = await shard.GetLeafIdForKeyAsync(high);
                Assert.That(resolved, Is.EqualTo(next),
                    $"re-descending onto '{high}' must land on the leaf that follows the one declaring it");
                checkedBounds++;
            }

            leafId = next;
        }

        Assert.That(checkedBounds, Is.GreaterThan(0),
            "the fixture must produce a multi-leaf chain with declared high bounds, or this test proves nothing");
    }

    /// <summary>
    /// The reason the resume position is a key. After the tree splits, a key
    /// captured earlier still resolves - onto whichever leaf now owns it - so a
    /// walk that parked at that key resumes on real data rather than on a leaf
    /// id that may no longer be part of the chain.
    /// </summary>
    [Test]
    public async Task GetLeafIdForKey_still_resolves_a_key_captured_before_the_tree_split()
    {
        var treeName = $"keydescent-split-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        for (var i = 0; i < 20; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        const string probe = "k010";
        var before = await shard.GetLeafIdForKeyAsync(probe);
        Assert.That(before, Is.Not.Null);

        // Grow the tree well past its leaf capacity so the chain splits
        // underneath the key we captured.
        for (var i = 20; i < 200; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var after = await shard.GetLeafIdForKeyAsync(probe);
        Assert.That(after, Is.Not.Null,
            "a key cursor must keep resolving across a split, which is why it is preferred to a leaf id");

        // Whichever leaf now owns the key must actually hold it, so a resumed
        // walk continues from the right place rather than from an arbitrary leaf.
        var owner = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(after!.Value);
        var keys = await owner.GetKeysAsync();
        Assert.That(keys, Does.Contain(probe));
    }
}
