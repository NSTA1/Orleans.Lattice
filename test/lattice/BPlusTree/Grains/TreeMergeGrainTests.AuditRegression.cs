using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for audit bugs #4 and #5:
/// <list type="bullet">
///   <item>Bug #4 — <see cref="TreeMergeGrain"/> drains an entire source shard's leaf chain
///   into a single in-memory dictionary before fanning out to target shards, which can OOM
///   on shards holding millions of keys. The merge must flush each leaf's delta to target
///   shards before loading the next leaf.</item>
///   <item>Bug #5 — <see cref="TreeMergeGrain"/> constructs source and target shard-root grain
///   keys from the logical tree ids and iterates <c>0..options.ShardCount</c>, bypassing the
///   registry alias + current <see cref="ShardMap"/>. The merge must resolve aliases on both
///   sides and iterate physical shard indices from the map.</item>
/// </list>
/// </summary>
public partial class TreeMergeGrainTests
{
    [Test]
    public async Task Merge_streams_per_leaf_and_does_not_buffer_whole_shard()
    {
        // Two source leaves in shard 0, each holding one entry. All entries
        // are routed to a single target shard (via a single-slot target map),
        // so a buffering implementation produces exactly ONE MergeManyAsync
        // call containing both entries; a streaming implementation produces
        // ONE call per leaf (two total).
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(SourceTreeId);
        registry.ResolveAsync(TargetTreeId).Returns(TargetTreeId);
        registry.GetShardMapAsync(TargetTreeId)
            .Returns(Task.FromResult<ShardMap?>(new ShardMap { Slots = [0, 0, 0, 0] }));

        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());

        var sourceShard = Substitute.For<IShardRootGrain>();
        sourceShard.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leaf0));
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0").Returns(sourceShard);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 1); // empty

        var clock0 = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var clock1 = HybridLogicalClock.Tick(clock0);
        var leaf0Mock = Substitute.For<IBPlusLeafGrain>();
        var leaf1Mock = Substitute.For<IBPlusLeafGrain>();
        grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).Returns(leaf0Mock);
        grainFactory.GetGrain<IBPlusLeafGrain>(leaf1).Returns(leaf1Mock);
        leaf0Mock.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(Task.FromResult(
            new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    ["alpha"] = LwwValue<byte[]>.Create([1], clock0)
                },
                Version = new VersionVector()
            }));
        leaf0Mock.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(leaf1));
        leaf1Mock.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(Task.FromResult(
            new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    ["beta"] = LwwValue<byte[]>.Create([2], clock1)
                },
                Version = new VersionVector()
            }));
        leaf1Mock.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.NextShardIndex = 0;

        await grain.ProcessNextShardAsync();

        // Count MergeManyAsync calls across all target shards. With the fix we
        // expect >= 2 (one flush per leaf); buffered implementation produces <= 1.
        var totalMergeCalls = 0;
        for (int i = 0; i < ShardCount; i++)
        {
            totalMergeCalls += grainFactory.GetGrain<IShardRootGrain>($"{TargetTreeId}/{i}")
                .ReceivedCalls()
                .Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.MergeManyAsync));
        }

        Assert.That(totalMergeCalls, Is.GreaterThanOrEqualTo(2),
            "Streaming merge must flush each leaf before loading the next.");
    }

    [Test]
    public async Task Merge_resolves_source_and_target_aliases_through_registry()
    {
        const string sourcePhysical = "source-physical";
        const string targetPhysical = "target-physical";

        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(sourcePhysical);
        registry.ResolveAsync(TargetTreeId).Returns(targetPhysical);
        registry.GetShardMapAsync(SourceTreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, 2)));
        registry.GetShardMapAsync(TargetTreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, 2)));

        // Source shard 0 at the physical key, holding one entry.
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var srcShard0 = Substitute.For<IShardRootGrain>();
        srcShard0.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        grainFactory.GetGrain<IShardRootGrain>($"{sourcePhysical}/0").Returns(srcShard0);
        var srcShard1 = Substitute.For<IShardRootGrain>();
        srcShard1.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IShardRootGrain>($"{sourcePhysical}/1").Returns(srcShard1);

        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        var leafMock = Substitute.For<IBPlusLeafGrain>();
        grainFactory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leafMock);
        leafMock.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(Task.FromResult(
            new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    ["gamma"] = LwwValue<byte[]>.Create([1, 2, 3], clock)
                },
                Version = new VersionVector()
            }));
        leafMock.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        // Stub target shard grains at both physical and logical keys so the
        // test fails cleanly whichever key the grain chooses.
        for (int i = 0; i < ShardCount; i++)
        {
            grainFactory.GetGrain<IShardRootGrain>($"{targetPhysical}/{i}")
                .Returns(Substitute.For<IShardRootGrain>());
            grainFactory.GetGrain<IShardRootGrain>($"{TargetTreeId}/{i}")
                .Returns(Substitute.For<IShardRootGrain>());
        }

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.NextShardIndex = 0;

        await grain.RunMergePassAsync();

        // Source side: physical key must have been used, not the logical one.
        grainFactory.Received().GetGrain<IShardRootGrain>($"{sourcePhysical}/0");
        grainFactory.DidNotReceive().GetGrain<IShardRootGrain>($"{SourceTreeId}/0");

        // Target side: at least one target physical shard must have seen
        // MergeManyAsync; no logical-keyed target shard should have.
        var physMergeCalls = 0;
        var logicalMergeCalls = 0;
        for (int i = 0; i < ShardCount; i++)
        {
            physMergeCalls += grainFactory.GetGrain<IShardRootGrain>($"{targetPhysical}/{i}")
                .ReceivedCalls()
                .Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.MergeManyAsync));
            logicalMergeCalls += grainFactory.GetGrain<IShardRootGrain>($"{TargetTreeId}/{i}")
                .ReceivedCalls()
                .Count(c => c.GetMethodInfo().Name == nameof(IShardRootGrain.MergeManyAsync));
        }

        Assert.That(physMergeCalls, Is.GreaterThan(0),
            "Merge must target the aliased physical target tree.");
        Assert.That(logicalMergeCalls, Is.Zero,
            "Merge must not target the logical (aliased) target tree id.");
    }

    [Test]
    public async Task Merge_iterates_source_physical_shards_from_ShardMap()
    {
        // Source has a custom ShardMap with non-contiguous physical indices {0, 7}.
        var sourceMap = new ShardMap { Slots = [0, 0, 0, 7, 7, 7, 7, 7] };

        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(SourceTreeId);
        registry.ResolveAsync(TargetTreeId).Returns(TargetTreeId);
        registry.GetShardMapAsync(SourceTreeId).Returns(Task.FromResult<ShardMap?>(sourceMap));
        registry.GetShardMapAsync(TargetTreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, 2)));

        var shard0 = Substitute.For<IShardRootGrain>();
        shard0.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        var shard7 = Substitute.For<IShardRootGrain>();
        shard7.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(null));
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0").Returns(shard0);
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/7").Returns(shard7);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        // With the fix, SourceShardCount is irrelevant — the grain must discover
        // physical shards from the map. We intentionally set it to a value that
        // would skip shard 7 under the old iteration.
        state.State.SourceShardCount = 2;
        state.State.NextShardIndex = 0;

        await grain.RunMergePassAsync();

        await shard0.Received().GetLeftmostLeafIdAsync();
        await shard7.Received().GetLeftmostLeafIdAsync();
    }

    [Test]
    public async Task ProcessNextShardAsync_poisons_shard_after_retry_budget_exhausted()
    {
        // FX-005 regression: when a source shard's merge has failed and
        // <see cref="TreeMergeState.ShardRetries"/> has reached the poison
        // cap (MaxRetriesPerShard = 2), the next tick must SKIP the shard,
        // advance the cursor, and reset the retry counter — without
        // running another merge attempt. Before the fix, the grain's
        // cursor advanced past failing shards immediately on the first
        // exception, so a single transient storage hiccup silently
        // dropped that shard from the merge.
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(SourceTreeId);
        registry.ResolveAsync(TargetTreeId).Returns(TargetTreeId);
        registry.GetShardMapAsync(TargetTreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, ShardCount)));
        registry.GetShardMapAsync(SourceTreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, ShardCount)));

        var sourceShard = Substitute.For<IShardRootGrain>();
        // Throwing from GetLeftmostLeafIdAsync would normally burn a retry,
        // but we seed ShardRetries at the cap so the grain must NOT call it.
        sourceShard.GetLeftmostLeafIdAsync().Returns<Task<GrainId?>>(
            _ => throw new InvalidOperationException("should not be called — shard is poisoned"));
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0").Returns(sourceShard);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourcePhysicalShards = [0, 1];
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 2; // == MaxRetriesPerShard

        await grain.ProcessNextShardAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextShardIndex, Is.EqualTo(1),
                "Poisoned shard must be skipped and cursor advanced.");
            Assert.That(state.State.ShardRetries, Is.EqualTo(0),
                "Retry counter must reset for the next shard.");
        });
        await sourceShard.DidNotReceive().GetLeftmostLeafIdAsync();
    }

    [Test]
    public async Task ProcessNextShardAsync_preincrements_retry_counter_before_attempt()
    {
        // FX-005 regression: the retry counter must be incremented BEFORE
        // the merge attempt and persisted, so that a non-throwing crash
        // (silo restart, host kill mid-merge) still burns budget on
        // reactivation. Without pre-increment, a deterministic-crash
        // shard would loop forever because ShardRetries stays at 0
        // across restarts.
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.ResolveAsync(SourceTreeId).Returns(SourceTreeId);
        registry.ResolveAsync(TargetTreeId).Returns(TargetTreeId);
        registry.GetShardMapAsync(TargetTreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, ShardCount)));
        registry.GetShardMapAsync(SourceTreeId)
            .Returns(Task.FromResult<ShardMap?>(ShardMap.CreateDefault(4, ShardCount)));

        var sourceShard = Substitute.For<IShardRootGrain>();
        sourceShard.GetLeftmostLeafIdAsync()
            .Returns<Task<GrainId?>>(_ => throw new InvalidOperationException("transient"));
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0").Returns(sourceShard);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourcePhysicalShards = [0, 1];
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.ProcessNextShardAsync());

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ShardRetries, Is.EqualTo(1),
                "Retry counter must be pre-incremented AND persisted before the attempt.");
            Assert.That(state.State.NextShardIndex, Is.EqualTo(0),
                "Cursor must not advance on a failing shard (until poison cap).");
            Assert.That(state.WriteCount, Is.GreaterThanOrEqualTo(1),
                "Pre-increment must be persisted so crash-recovery sees it.");
        });
    }
}
