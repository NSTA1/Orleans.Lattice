using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Chain-integrity coverage for the direction the leaf-reclaim work does
/// <b>not</b> change: pure GROWTH (issue 2103, epic 2102 acceptance criterion 3).
/// <para>
/// The reclaim change (issue 2099) modified leaf linkage, the split path's
/// interaction with retirement, and added a boundary-narrowing invariant.
/// Growth is therefore the direction most likely to be broken by accident,
/// precisely because it is the direction nobody was editing. A survey of the
/// existing suite found the growth direction well covered in aggregate but
/// with three properties never asserted together, and two never asserted at
/// all after growth alone:
/// </para>
/// <list type="bullet">
///   <item><description>that the forward walk <b>terminates</b> - existing
///   walks bound their loop at 5,000 and reconcile a count afterwards, so a
///   cycle fails as a confusing count mismatch rather than as a cycle;</description></item>
///   <item><description>that the walk visits every leaf <b>exactly once</b> -
///   uniqueness of the visited ids is never explicitly asserted anywhere;</description></item>
///   <item><description>that <c>PrevSibling</c> <b>mirrors</b>
///   <c>NextSibling</c> - back-links are only ever exercised transitively, so a
///   split that wired the forward chain correctly and the backward chain
///   wrongly would pass the whole existing suite.</description></item>
/// </list>
/// <para>
/// The pre-existing tiling assertions that do cover this
/// (<c>LeafReclaimIntegrationTests.AssertChainTilesKeyspaceAsync</c>) live in
/// the reclaim fixtures the epic added, so they are new coverage rather than a
/// regression net that predates the change. This fixture supplies the net: it
/// never deletes anything and never triggers a reclaim pass, so every
/// assertion here describes the tree as it was before the epic, and a
/// regression in growth shows up here as a direct, named failure.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafChainGrowthIntegrityIntegrationTests
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

    /// <summary>
    /// Walks forward from the leftmost leaf, asserting termination and
    /// uniqueness as it goes rather than afterwards, so a cycle is reported as
    /// a cycle and names the leaf that closed it.
    /// </summary>
    private async Task<List<GrainId>> WalkForwardAssertingAcyclicAsync(IShardRootGrain shard)
    {
        var chain = new List<GrainId>();
        var seen = new HashSet<GrainId>();
        var leafId = await shard.GetLeftmostLeafIdAsync();

        // Deliberately generous, and deliberately NOT the loop's real defence:
        // the uniqueness assertion below fails first and says what happened.
        // The bound only stops a runaway if that assertion is ever removed.
        var hardStop = 10_000;

        while (leafId is { } id)
        {
            Assert.That(seen.Add(id), Is.True,
                $"the forward chain is cyclic: leaf '{id}' was reached twice, at positions "
                + $"{chain.IndexOf(id)} and {chain.Count}");

            chain.Add(id);

            Assert.That(chain.Count, Is.LessThan(hardStop),
                "the forward chain did not terminate within the hard stop");

            leafId = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        return chain;
    }

    /// <summary>
    /// The growth-direction discriminator. Seeds a tree hard enough to split
    /// it many times and then asserts the whole bundle of chain properties at
    /// once, so that any single broken linkage fails with a message naming
    /// which property broke.
    /// </summary>
    [Test]
    public async Task A_grown_chain_is_acyclic_singly_visited_doubly_linked_and_tiles_the_keyspace()
    {
        var treeName = $"growth-integrity-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);

        const int Seeded = 160;
        for (var i = 0; i < Seeded; i++)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var chain = await WalkForwardAssertingAcyclicAsync(shard);

        // Precondition. Without this the rest of the fixture would pass
        // vacuously on a tree that never split, which is exactly the failure
        // mode a growth test is most prone to.
        Assert.That(chain.Count, Is.GreaterThan(8),
            "precondition: the seed must actually have split the tree many times, or this test proves nothing");

        // (1) Backward links mirror forward links, walked independently from
        // the last leaf rather than derived from the forward walk.
        var backward = new List<GrainId>();
        var seenBackward = new HashSet<GrainId>();
        var cursor = (GrainId?)chain[^1];
        while (cursor is { } id)
        {
            Assert.That(seenBackward.Add(id), Is.True,
                $"the backward chain is cyclic: leaf '{id}' was reached twice");
            backward.Add(id);
            cursor = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetPrevSiblingAsync();
        }

        backward.Reverse();
        Assert.That(backward, Is.EqualTo(chain),
            "the backward walk must visit exactly the forward walk's leaves in exactly the reverse order; "
            + "a split that wires NextSibling correctly and PrevSibling wrongly is invisible to every "
            + "forward-only assertion in the suite");

        // (2) The chain's outer bounds are open on both sides, so no key can
        // fall outside the tree.
        var first = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[0]).GetKeyRangeAsync();
        var last = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[^1]).GetKeyRangeAsync();
        Assert.That(first.LowKeyInclusive, Is.Null, "the leftmost leaf must be unbounded below");
        Assert.That(last.HighKeyExclusive, Is.Null, "the rightmost leaf must be unbounded above");

        // (3) Consecutive leaves tile the keyspace with no gap and no overlap.
        // A gap is a span owned by nobody, whose writes vanish on the next
        // projection rebuild; an overlap is a span two leaves both materialise.
        for (var i = 0; i < chain.Count - 1; i++)
        {
            var here = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i]).GetKeyRangeAsync();
            var next = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i + 1]).GetKeyRangeAsync();

            Assert.That(here.HighKeyExclusive, Is.EqualTo(next.LowKeyInclusive),
                $"after growth: leaf {i} ends at '{here.HighKeyExclusive}' but leaf {i + 1} begins at "
                + $"'{next.LowKeyInclusive}', so that span is owned by "
                + $"{(here.HighKeyExclusive is null ? "both" : "nobody")}");
        }

        // (4) Every seeded key is reachable by descent, and lands on the leaf
        // whose declared range contains it - the property the tiling above is
        // a proxy for, asserted directly.
        for (var i = 0; i < Seeded; i++)
        {
            var key = $"k{i:D3}";
            var leafId = await shard.GetLeafIdForKeyAsync(key);
            Assert.That(leafId, Is.Not.Null, $"descent for '{key}' resolved to no leaf");

            var range = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId!.Value).GetKeyRangeAsync();
            Assert.That(range.LowKeyInclusive is null || string.CompareOrdinal(key, range.LowKeyInclusive) >= 0,
                Is.True, $"descent routed '{key}' to a leaf whose range starts at '{range.LowKeyInclusive}'");
            Assert.That(range.HighKeyExclusive is null || string.CompareOrdinal(key, range.HighKeyExclusive) < 0,
                Is.True, $"descent routed '{key}' to a leaf whose range ends at '{range.HighKeyExclusive}'");

            Assert.That(chain, Does.Contain(leafId!.Value),
                $"descent for '{key}' resolved to leaf '{leafId}', which the sibling walk never visits - "
                + "the tree is reachable by descent but not by scan");
        }

        // (5) Nothing was lost on the way up.
        Assert.That(await router.CountAsync(), Is.EqualTo(Seeded),
            "growth alone must not lose a key");
    }

    /// <summary>
    /// Growth arriving in descending key order drives the split path through
    /// its other side - every insert lands left of everything already stored,
    /// so the leaf being split is the leftmost rather than the rightmost. The
    /// chain properties must hold identically.
    /// </summary>
    [Test]
    public async Task A_chain_grown_by_descending_inserts_is_linked_and_tiled_the_same_way()
    {
        var treeName = $"growth-descending-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);

        const int Seeded = 120;
        for (var i = Seeded - 1; i >= 0; i--)
            await router.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));

        var chain = await WalkForwardAssertingAcyclicAsync(shard);
        Assert.That(chain.Count, Is.GreaterThan(8),
            "precondition: the descending seed must also have split the tree many times");

        for (var i = 0; i < chain.Count - 1; i++)
        {
            var here = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i]).GetKeyRangeAsync();
            var next = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i + 1]).GetKeyRangeAsync();

            Assert.That(here.HighKeyExclusive, Is.EqualTo(next.LowKeyInclusive),
                $"after descending growth: leaf {i} and leaf {i + 1} do not tile");
        }

        var backward = new List<GrainId>();
        var cursor = (GrainId?)chain[^1];
        var guard = 0;
        while (cursor is { } id && guard++ < 10_000)
        {
            backward.Add(id);
            cursor = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetPrevSiblingAsync();
        }

        backward.Reverse();
        Assert.That(backward, Is.EqualTo(chain),
            "descending growth must leave the backward chain mirroring the forward chain");

        Assert.That(await router.CountAsync(), Is.EqualTo(Seeded));
    }
}
