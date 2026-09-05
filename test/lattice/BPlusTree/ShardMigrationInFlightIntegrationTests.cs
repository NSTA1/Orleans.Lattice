using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration coverage for shard-migration contracts that only hold
/// <b>while an operation is in flight</b>: shadow-forwarding of live writes
/// during a split, and the in-flight idempotency guards on the split and
/// reshard coordinators.
/// <para>
/// <b>Why these live in their own fixture.</b> A coordinator's phase timer is
/// armed with a due time of zero, so its first tick lands as the start call
/// returns. On the default <see cref="FourShardClusterFixture"/> a small tree
/// drains in a single pass, so a test that starts an operation and then depends
/// on it still being in flight is racing a timer it cannot see. That race does
/// not usually surface as a failure - it surfaces as a <em>vacuous pass</em>:
/// the operation finishes first, the follow-up call takes a different code path
/// that also happens to succeed, and the contract the test is named for is never
/// exercised. <see cref="SlowDrainPumpClusterFixture"/> holds the operation open
/// structurally so the scenario is reachable.
/// </para>
/// <para>
/// A structural hold makes the scenario reachable; it does not make it proven.
/// Every test below therefore states an <b>unconditional reachability
/// precondition</b> that fails loudly and names the state it actually observed,
/// with the contract assertion left unconditional underneath. That is not the
/// same as widening an assertion to accept either outcome - the contract still
/// has exactly one acceptable result; the precondition only proves the test
/// reached the situation in which that contract applies.
/// </para>
/// <para>
/// The narrower phase-machine transitions these guards sit on are covered
/// deterministically, with no timer at all, by the corresponding unit fixtures
/// (<c>TreeShardSplitGrainTests</c>, <c>TreeReshardGrainTests</c>). What these
/// integration tests add is that the guard holds end to end against a real
/// registry, real shard roots and a real running pump.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ShardMigrationInFlightIntegrationTests
{
    /// <summary>Physical shard these tests split.</summary>
    private const int SourceShard = 0;

    /// <summary>
    /// First physical shard index a split on <see cref="SourceShard"/> can
    /// allocate, given the fixture pins the tree at four shards (0..3). Its
    /// presence in the persisted shard map is the observable proof that a split
    /// has reached <c>Swap</c> and committed.
    /// </summary>
    private const int FirstAllocatedShardIndex = SlowDrainPumpClusterFixture.TestShardCount;

    /// <summary>
    /// Enough keys that the source shard holds a long leaf chain. With a leaf
    /// fan-out of <see cref="SlowDrainPumpClusterFixture.SmallMaxLeafKeys"/> and
    /// one leaf drained per background pass, the split needs many pump ticks at
    /// the coordinator's two-second cadence before it can leave its drain phase.
    /// </summary>
    private const int SeedKeyCount = 240;

    private SlowDrainPumpClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SlowDrainPumpClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Split_shadow_forward_preserves_TTL_on_target_shard()
    {
        // Regression test for interaction:
        // shadow-forward used to reconstruct LwwValue via LwwValue.Create(value, version),
        // silently dropping ExpiresAtTicks. After split commit, the target shard held
        // non-expiring copies of TTL'd writes. The fix routes shadow-forward through
        // IBPlusLeafGrain.GetRawEntryAsync so the raw LwwValue (including expiry)
        // is forwarded verbatim.
        //
        // The contract only exists inside the shadow-forward window, so the TTL'd
        // writes below are worthless unless they land while the split is open. That
        // is asserted, not assumed.
        var treeId = $"split-ttl-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await SeedAsync(tree);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{SourceShard}");
        await split.SplitAsync(sourceShardIndex: SourceShard);

        // Write TTL'd entries while the split is active. Any key whose virtual slot
        // is moved will be shadow-forwarded to the target shard; once the map swaps,
        // subsequent reads route to the target and the entry's expiry must still apply.
        var ttl = TimeSpan.FromSeconds(3);
        var ttlKeys = new List<string>();
        for (int i = 0; i < 50; i++)
        {
            var k = $"ttl-{i:D3}";
            ttlKeys.Add(k);
            await tree.SetAsync(k, Encoding.UTF8.GetBytes($"e{i}"), ttl);
        }

        // Reachability precondition, in two parts, both monotone across a single
        // split and therefore safe to observe after the fact:
        //   1. The source shard raises SplitInProgress inside BeginSplitAsync -
        //      before SplitAsync returns - and lowers it exactly once, at
        //      finalisation. Still raised here means the shadow-forward window was
        //      open throughout the TTL writes above.
        //   2. The persisted shard map does not yet name a newly allocated shard.
        //      SwapAsync is what publishes that index, so its absence proves the
        //      split had not yet swapped: the moved slots still routed to the
        //      source, which is precisely the condition under which a live write is
        //      shadow-forwarded rather than written straight to its new owner.
        // Part 2 is the load-bearing half - SplitInProgress alone stays raised
        // through Swap and Reject, so it cannot tell pre-swap from post-swap.
        var splitInProgress = await SourceSplitInProgressAsync(tree);
        var shardsBeforeSwap = await PhysicalShardIndicesAsync(treeId);
        Assert.Multiple(() =>
        {
            Assert.That(splitInProgress, Is.True,
                $"Precondition: shard {SourceShard} must still be mid-split when the TTL'd writes land, " +
                "otherwise they never enter the shadow-forward path this test exists to cover. " +
                $"Observed SplitInProgress={splitInProgress}.");
            Assert.That(shardsBeforeSwap, Does.Not.Contain(FirstAllocatedShardIndex),
                "Precondition: the split must not have swapped yet when the TTL'd writes land, otherwise they " +
                "route straight to the post-swap owner and shadow-forward is never exercised. " +
                $"Observed shards: [{string.Join(", ", shardsBeforeSwap)}].");
        });

        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "Split should be complete after RunSplitPassAsync.");

        // Completing the precondition: the split committed a new physical shard, so
        // the shadow-forwarded entries genuinely crossed a shard boundary and the
        // reads below are served by the target rather than the original owner.
        var committedShards = await PhysicalShardIndicesAsync(treeId);
        Assert.That(committedShards, Does.Contain(FirstAllocatedShardIndex),
            "Precondition: the split must have swapped in a new physical shard, otherwise no " +
            $"shadow-forwarded entry ever moved. Observed shards: [{string.Join(", ", committedShards)}].");

        // Immediately after split: every TTL'd key should still be live
        // (the TTL has not elapsed yet).
        foreach (var k in ttlKeys)
        {
            Assert.That(await tree.GetAsync(k), Is.Not.Null,
                $"TTL'd key '{k}' should be live immediately after split.");
        }

        // Wait past the TTL. Every TTL'd key must now read null regardless of which
        // physical shard serves it - previously, shadow-forwarded copies on the
        // target shard had ExpiresAtTicks=0 and would remain live indefinitely.
        await Task.Delay(ttl + TimeSpan.FromMilliseconds(500));

        var leaked = new List<string>();
        foreach (var k in ttlKeys)
        {
            if (await tree.GetAsync(k) is not null)
                leaked.Add(k);
        }

        Assert.That(leaked, Is.Empty,
            $"TTL must survive shadow-forward: {leaked.Count} key(s) remained live past expiry " +
            $"({string.Join(", ", leaked.Take(5))}{(leaked.Count > 5 ? "..." : "")}).");
    }

    [Test]
    public async Task SplitAsync_is_idempotent_while_the_same_source_split_is_in_flight()
    {
        var treeId = $"split-idem-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await SeedAsync(tree);

        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{SourceShard}");
        await split.SplitAsync(sourceShardIndex: SourceShard);

        // Contract under test: a second request naming the same source shard while
        // the first is still running is absorbed, not rejected and not restarted.
        await split.SplitAsync(sourceShardIndex: SourceShard);

        // Reachability precondition. Two observations are needed, because
        // "not idle" alone cannot tell an absorbed second request apart from a
        // second request that started a brand new split over a finished one:
        //   1. the coordinator is running, and
        //   2. no split has committed yet - a completed split would have swapped a
        //      newly allocated physical shard into the persisted map.
        // Together they pin the running coordinator to the *first* split, so the
        // second call above demonstrably hit the in-flight branch.
        var idle = await split.IsIdleAsync();
        var shardsWhileRunning = await PhysicalShardIndicesAsync(treeId);
        Assert.Multiple(() =>
        {
            Assert.That(idle, Is.False,
                $"Precondition: the split must still be in flight when the second request lands. Observed IsIdle={idle}.");
            Assert.That(shardsWhileRunning, Does.Not.Contain(FirstAllocatedShardIndex),
                "Precondition: no split may have committed yet, otherwise the second request started a " +
                $"fresh split instead of being absorbed. Observed shards: [{string.Join(", ", shardsWhileRunning)}].");
        });

        await split.RunSplitPassAsync();

        Assert.That(await split.IsIdleAsync(), Is.True,
            "The single absorbed split should run to completion and leave the coordinator idle.");
        Assert.That(await PhysicalShardIndicesAsync(treeId), Does.Contain(FirstAllocatedShardIndex),
            "Exactly the first split should have committed its allocated target shard.");
    }

    [Test]
    public async Task ReshardAsync_is_idempotent_while_the_same_target_reshard_is_in_flight()
    {
        var treeId = $"reshard-idem-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        await SeedAsync(tree);

        const int targetShardCount = SlowDrainPumpClusterFixture.TestShardCount + 1;
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);

        await tree.ReshardAsync(targetShardCount);

        // Contract under test: a second request naming the same target while the
        // first is still running is absorbed, not rejected.
        await tree.ReshardAsync(targetShardCount);

        // Reachability precondition. Here "not idle" is sufficient on its own: had
        // the first reshard already finished, the tree would be at the target count
        // and the second request would have taken the equal-count no-op path, which
        // starts no coordinator and would leave this idle. A running coordinator can
        // therefore only be the first reshard, still in flight.
        var idle = await reshard.IsIdleAsync();
        Assert.That(idle, Is.False,
            "Precondition: the reshard must still be in flight when the second request lands, otherwise " +
            $"the second request takes the equal-count no-op path and the in-flight guard is never exercised. Observed IsIdle={idle}.");

        await DriveReshardToCompletionAsync(treeId);

        Assert.That(await tree.IsReshardCompleteAsync(), Is.True, "The absorbed reshard should run to completion.");
        Assert.That((await PhysicalShardIndicesAsync(treeId)).Count, Is.EqualTo(targetShardCount),
            "The tree should land on exactly the requested shard count - the second request must not have grown it further.");
        Assert.That(await tree.GetAsync("seed-0000"), Is.Not.Null, "Reshard must preserve data.");
    }

    private static async Task SeedAsync(ILattice tree)
    {
        for (int i = 0; i < SeedKeyCount; i++)
            await tree.SetAsync($"seed-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
    }

    /// <summary>
    /// Reads the source shard's own view of whether it is currently acting as a
    /// split source. The flag is raised by <c>BeginSplitAsync</c> and cleared
    /// once at finalisation, so it is monotone across a single split.
    /// </summary>
    private static async Task<bool> SourceSplitInProgressAsync(ILattice tree)
    {
        var report = await tree.DiagnoseAsync();
        var source = report.Shards.FirstOrDefault(s => s.ShardIndex == SourceShard);
        return source.SplitInProgress;
    }

    /// <summary>
    /// Physical shard indices in the persisted shard map, or an empty list while
    /// no migration has committed one yet.
    /// </summary>
    private async Task<IReadOnlyList<int>> PhysicalShardIndicesAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var map = await registry.GetShardMapAsync(treeId);
        return map?.GetPhysicalShardIndices() ?? [];
    }

    /// <summary>
    /// Drives the reshard coordinator and all dispatched per-shard split
    /// coordinators to completion synchronously. The fixture deliberately slows
    /// the <em>background</em> pump only; driving a coordinator explicitly still
    /// runs its bounded drain through to completion in one call, so the test
    /// budget stays small.
    /// </summary>
    private async Task DriveReshardToCompletionAsync(string treeId)
    {
        var reshard = _cluster.GrainFactory.GetGrain<ITreeReshardGrain>(treeId);
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        for (int i = 0; i < 50; i++)
        {
            if (await reshard.IsIdleAsync()) return;

            await reshard.RunReshardPassAsync();

            var map = await registry.GetShardMapAsync(treeId)
                ?? ShardMap.CreateDefault(
                    LatticeConstants.DefaultVirtualShardCount,
                    SlowDrainPumpClusterFixture.TestShardCount);
            foreach (var idx in map.GetPhysicalShardIndices())
            {
                var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>($"{treeId}/{idx}");
                if (!await split.IsIdleAsync())
                    await split.RunSplitPassAsync();
            }

            await Task.Delay(50);
        }

        var finalMap = await registry.GetShardMapAsync(treeId);
        var distinct = finalMap?.GetPhysicalShardIndices().Count ?? -1;
        Assert.Fail($"Reshard did not converge. reshard.IsIdle={await reshard.IsIdleAsync()}, map.distinct={distinct}");
    }
}
