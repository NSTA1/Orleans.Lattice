using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for empty-leaf reclaim running concurrently with writes.
/// <para>
/// The reclaim tests alongside this fixture all drive a quiescent tree: they
/// empty a range, run a pass, and inspect what is left. That is the shape the
/// feature was written and reviewed against, and it is exactly the shape that
/// cannot see its most serious failure mode. A fold is not one atomic step but
/// a sequence of grain calls - probe, descend, unroute, unlink, clear - and the
/// leaf mutation surface is <c>[AlwaysInterleave]</c>, so a write can be
/// routed, logged, applied and acknowledged in the middle of that sequence. On
/// a quiescent tree no write ever does, so nothing fails; every one of those
/// tests passed against an implementation that would erase such a write.
/// </para>
/// <para>
/// The loss is silent and permanent. After the fold the key routes to the
/// predecessor, whose projection checkpoint is already past the offset the
/// erased write occupies, so no replay re-materialises it: the caller was told
/// the write succeeded and the row is gone. These tests therefore assert
/// against a projection rebuilt from the write-ahead log rather than against
/// the live cache, because a cache read would report the row present right up
/// until the moment the tree was restarted.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafReclaimConcurrencyIntegrationTests
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
    /// Discards every leaf's in-memory projection and rebuilds it from the
    /// write-ahead log, which is what a restart would do.
    /// <para>
    /// This is the assertion that matters for a lost write, and the reason a
    /// plain read-back is not enough. A write erased by a fold is still in the
    /// predecessor's cache for as long as that activation lives, so a live read
    /// reports success; only a rebuild asks the durable log what the tree
    /// actually holds, and only the rebuild shows the row is gone.
    /// </para>
    /// </summary>
    private async Task RebuildEveryProjectionAsync(IShardRootGrain shard)
    {
        foreach (var leafId in await WalkChainAsync(shard))
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId).RebuildProjectionFromWalAsync();
    }

    /// <summary>Finds an empty, non-head leaf: the shape a pass would fold.</summary>
    private async Task<(GrainId LeafId, LeafKeyRange Range)> FindReclaimCandidateAsync(IShardRootGrain shard)
    {
        var chain = await WalkChainAsync(shard);

        // Skip the head: it owns everything below the first separator and is
        // never a candidate, because it has no predecessor to inherit its
        // range.
        for (var i = 1; i < chain.Count; i++)
        {
            var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i]);
            if (await leaf.CountAsync() != 0) continue;

            var range = await leaf.GetKeyRangeAsync();
            if (range.LowKeyInclusive is null) continue;

            return (chain[i], range);
        }

        Assert.Fail("precondition: the emptied range must have left at least one empty non-head leaf to fold");
        return default;
    }

    /// <summary>Returns a key that routes into the given leaf's declared range.</summary>
    private static string KeyInRange(LeafKeyRange range)
    {
        // The low bound is inclusive, so it is always in range, and it is the
        // one key guaranteed to exist whatever the bounds happen to be.
        return range.LowKeyInclusive!;
    }

    // --- the window between the probe and the fold ---

    /// <summary>
    /// The regression test for the defect, reproduced exactly rather than
    /// raced for.
    /// <para>
    /// A fold decides on the evidence of a probe and then acts across several
    /// more grain calls. This drives that sequence by hand and lands a write in
    /// the middle of it, which is the interleaving a quiescent test can never
    /// produce and a racing one can only produce sometimes. The leaf must
    /// refuse to retire: it is no longer the empty leaf the probe described,
    /// and retiring it would destroy an acknowledged write.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_leaf_that_takes_a_write_after_its_probe_refuses_to_retire()
    {
        var treeName = $"reclaim-window-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var (leafId, range) = await FindReclaimCandidateAsync(shard);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);

        // Step one of a fold: the probe says the leaf is empty and may go.
        var probe = await leaf.GetReclaimProbeAsync();
        Assert.That(probe.LiveRowCount, Is.Zero, "precondition: the fold must begin from an empty leaf");
        Assert.That(probe.HasBlockingState, Is.False);

        // The window. A write is routed, logged, applied and acknowledged
        // while the fold is still several calls from the destructive step.
        var racingKey = KeyInRange(range);
        await router.SetAsync(racingKey, Encoding.UTF8.GetBytes("racing"));

        // The decision. It must be taken against the leaf as it is now, not as
        // the probe described it several calls ago.
        Assert.That(await leaf.TryBeginRetirementAsync(), Is.False,
            "a leaf that took a write after its probe must refuse to retire; retiring it would erase an acknowledged write");

        // And the write must survive what a restart would do to it.
        await RebuildEveryProjectionAsync(shard);
        Assert.That(await router.GetAsync(racingKey), Is.Not.Null,
            "the acknowledged write must survive a projection rebuild, which is where a silently erased write shows up");
    }

    /// <summary>
    /// Once a leaf has latched itself retired it must refuse writes outright,
    /// not apply them. By that point the fold is committed: the leaf is on its
    /// way out of the routing table and the chain, so a write applied here
    /// would land in state that is about to be cleared. Refusing lets the
    /// caller re-route to the leaf that now owns the range.
    /// </summary>
    [Test]
    public async Task A_retired_leaf_refuses_writes_until_the_retirement_is_abandoned()
    {
        var treeName = $"reclaim-latch-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var (leafId, range) = await FindReclaimCandidateAsync(shard);
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafId);

        Assert.That(await leaf.TryBeginRetirementAsync(), Is.True,
            "precondition: an empty leaf with no blocking state must be allowed to retire");

        var key = KeyInRange(range);
        Assert.That(async () => await router.SetAsync(key, Encoding.UTF8.GetBytes("refused")),
            Throws.Exception,
            "a retired leaf must refuse a write rather than apply it to state that is about to be cleared");

        // Abandoning must fully reopen the leaf, or an interrupted fold would
        // leave a perfectly good leaf permanently unwritable.
        await leaf.AbandonRetirementAsync();

        await router.SetAsync(key, Encoding.UTF8.GetBytes("accepted"));
        await RebuildEveryProjectionAsync(shard);
        Assert.That(await router.GetAsync(key), Is.Not.Null,
            "abandoning a retirement must leave the leaf writable again");
    }

    // --- the whole path, under concurrent load ---

    /// <summary>
    /// The end-to-end form of the same property, and the one that also covers
    /// the routing gap a fold opens between unrouting a leaf and handing its
    /// range on.
    /// <para>
    /// Writes are issued into the emptied range while reclaim passes run
    /// against it, and every acknowledged write is then required to survive a
    /// projection rebuild. This catches both ways the window loses data: a
    /// write erased by the clear, and a write accepted into a range that the
    /// fold left owned by nobody, which the leaf serves from cache and drops
    /// on rebuild because the key falls outside the span its replay filter
    /// admits.
    /// </para>
    /// <para>
    /// The interleaving is not deterministic, which is why the two tests above
    /// exist. What is deterministic is the assertion: whatever order the calls
    /// happen to take, a write that was acknowledged must be readable
    /// afterwards, so this test can only fail when the tree has genuinely lost
    /// one.
    /// </para>
    /// </summary>
    [Test]
    public async Task Writes_racing_a_reclaim_pass_are_never_lost()
    {
        var treeName = $"reclaim-race-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);
        await router.DeleteRangeAsync("k030", "k090");

        var acknowledged = new List<string>();

        // Reclaim and write at the same time, several times over, so the
        // writes land at varying points in the fold sequence.
        for (var round = 0; round < 4; round++)
        {
            var reclaim = shard.ReclaimEmptyLeavesAsync(int.MaxValue);

            var writes = new List<Task>();
            for (var i = 0; i < 15; i++)
            {
                // Straight back into the range the pass is folding away.
                var key = $"k{30 + (round * 15) + i:D3}";
                acknowledged.Add(key);
                writes.Add(router.SetAsync(key, Encoding.UTF8.GetBytes($"race-{key}")));
            }

            await Task.WhenAll(writes);
            await reclaim;
        }

        // Every write returned successfully above, so every key must be here -
        // and must still be here once the projections are rebuilt from the log
        // rather than served from a cache that has not been torn down yet.
        await RebuildEveryProjectionAsync(shard);

        var lost = new List<string>();
        foreach (var key in acknowledged)
        {
            if (await router.GetAsync(key) is null) lost.Add(key);
        }

        Assert.That(lost, Is.Empty,
            $"{lost.Count} acknowledged write(s) did not survive a projection rebuild after racing a reclaim pass");

        // A fold that opened a gap it never closed shows up here too: the
        // chain must still tile the keyspace with no span owned by nobody.
        var chain = await WalkChainAsync(shard);
        for (var i = 0; i < chain.Count - 1; i++)
        {
            var here = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i]).GetKeyRangeAsync();
            var next = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(chain[i + 1]).GetKeyRangeAsync();

            Assert.That(here.HighKeyExclusive, Is.EqualTo(next.LowKeyInclusive),
                $"after racing writes and reclaim, leaf {i} ends at '{here.HighKeyExclusive}' but leaf {i + 1} begins at '{next.LowKeyInclusive}'");
        }
    }

    // --- the walk bound ---

    /// <summary>
    /// A pass must stop walking when its fold budget is spent, not merely stop
    /// folding.
    /// <para>
    /// Probing a leaf activates it and counts its rows, and the pass holds the
    /// shard root's activation turn while it runs, so a pass that walked on
    /// after spending its budget would put every remaining leaf in the shard
    /// through an activation and a count for a decision it could no longer
    /// act on - head-of-line blocking every read and write on the shard behind
    /// it.
    /// </para>
    /// <para>
    /// Stopping early is only safe if the next pass picks up where this one
    /// stopped, so the two properties are tested together: no pass folds more
    /// than it was asked to, and repeated small passes still reclaim the whole
    /// chain rather than re-walking the same prefix forever.
    /// </para>
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_stops_at_its_budget_and_resumes_on_the_next_pass()
    {
        var treeName = $"reclaim-budget-{Guid.NewGuid():N}";
        var (router, shard) = await CreateSingleShardTreeAsync(treeName);
        await SeedAsync(router, 120);

        var grown = (await WalkChainAsync(shard)).Count;
        Assert.That(grown, Is.GreaterThan(8), "precondition: the seed must actually have split the tree");

        await router.DeleteRangeAsync("k030", "k090");

        // One leaf per pass. Each pass must honour the budget exactly, and the
        // passes together must still drain the chain: that is what proves the
        // bounded walk resumes rather than restarting at the head every time.
        //
        // A pass that folds nothing is not evidence that the chain is drained.
        // A bounded walk resumes where the last one stopped, so a pass can
        // legitimately spend its whole window on leaves that hold rows, fold
        // nothing, reach the tail and wrap back to the head for the next one.
        // The loop therefore runs a fixed number of passes rather than
        // stopping at the first empty-handed one.
        var totalReclaimed = 0;
        for (var pass = 0; pass < 60; pass++)
        {
            var reclaimed = await shard.ReclaimEmptyLeavesAsync(1);
            Assert.That(reclaimed, Is.LessThanOrEqualTo(1), "a pass must never fold more than its budget");
            totalReclaimed += reclaimed;
        }

        Assert.That(totalReclaimed, Is.GreaterThan(0),
            "single-leaf passes must make progress; a bounded walk that never resumed would keep re-walking the same prefix");

        var settled = (await WalkChainAsync(shard)).Count;
        Assert.That(settled, Is.EqualTo(grown - totalReclaimed),
            "the chain must be exactly as much shorter as the passes claimed to have folded");

        // An unbounded pass now has nothing left to find, so the small passes
        // reached everything a single large one would have.
        Assert.That(await shard.ReclaimEmptyLeavesAsync(int.MaxValue), Is.Zero,
            "repeated bounded passes must reclaim everything an unbounded pass would, or the walk is not resuming");

        Assert.That(await router.CountAsync(), Is.EqualTo(60),
            "reclaim must not have disturbed the rows that are still live");
    }
}
