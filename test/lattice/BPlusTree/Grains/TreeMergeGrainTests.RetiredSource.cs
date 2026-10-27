using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A merge reads the source shards it recorded when it started, straight from
/// their leaves. An online shard consolidation that retires one of them while
/// the merge runs releases its storage, and the survivor that absorbed its slots
/// may already have been drained - so before completing, the merge must notice
/// the retired shard and re-drain the source's current shards.
/// </summary>
public partial class TreeMergeGrainTests
{
    [Test]
    public void Retired_source_probe_matches_linear_membership_for_sparse_maps_and_unordered_generations()
    {
        int[] current = [0, 3, 17, int.MaxValue];
        int[][] generations =
        [
            [],
            [17, 0, int.MaxValue, 3],
            [0, 3, 16],
            [-1, 0],
            [int.MaxValue, 18],
        ];
        foreach (var recorded in generations)
        {
            for (var start = 0; start <= recorded.Length; start++)
            {
                var expected = recorded.Skip(start).Any(index => !current.Contains(index));
                Assert.That(TreeMergeGrain.HasRetiredSourceShard(recorded, start, current), Is.EqualTo(expected),
                    $"generation start {start} in [{string.Join(",", recorded)}]");
            }
        }

        Assert.That(TreeMergeGrain.HasRetiredSourceShard([0], 0, []), Is.True);
    }

    [TestCase(16)]
    [TestCase(256)]
    [TestCase(257)]
    [TestCase(4096)]
    public void Retired_source_probe_finds_missing_indices_in_large_sorted_maps(int shards)
    {
        var current = Enumerable.Range(0, shards).Select(index => index * 3).ToArray();
        var recorded = current.Reverse().ToArray();
        Assert.That(TreeMergeGrain.HasRetiredSourceShard(recorded, 0, current), Is.False);
        foreach (var missingAt in new[] { 0, shards / 2, shards - 1 })
        {
            var changed = (int[])recorded.Clone();
            changed[missingAt]++;
            Assert.That(TreeMergeGrain.HasRetiredSourceShard(changed, 0, current), Is.True);
            Assert.That(TreeMergeGrain.HasRetiredSourceShard(changed, missingAt + 1, current), Is.False,
                "retired shards from older generations must not trigger another drain");
        }
    }

    private static (IBPlusLeafGrain Leaf, GrainId LeafId) SetupSurvivorHoldingAbsorbedKey(IGrainFactory grainFactory)
    {
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 0,
            new Dictionary<string, LwwValue<byte[]>> { ["absorbed"] = LwwValue<byte[]>.Create([7], clock) },
            leafId);
        return (grainFactory.GetGrain<IBPlusLeafGrain>(leafId), leafId);
    }

    [Test]
    public async Task RunMergePass_re_drains_the_current_source_shards_when_a_recorded_one_was_retired()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var (survivorLeaf, _) = SetupSurvivorHoldingAbsorbedKey(grainFactory);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 1); // retired: no leaves left
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        // Shard 1 was folded onto shard 0 while the merge ran: the map no
        // longer references it.
        StubSourceEntry(grainFactory, physicalTreeId: null, new ShardMap { Slots = new int[16], Version = 2 });

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.SourcePhysicalTreeId = SourceTreeId;
        state.State.TargetPhysicalTreeId = TargetTreeId;
        state.State.SourcePhysicalShards = [0, 1];

        await grain.RunMergePassAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SourcePhysicalShards, Is.EqualTo(new[] { 0, 1, 0 }),
                "the source's current shards must be appended as a new generation");
            Assert.That(state.State.SourceGenerationStart, Is.EqualTo(2));
            Assert.That(state.State.Complete, Is.True, "one appended generation settles a map that stops changing");
        });
        await survivorLeaf.Received(2).GetDeltaSinceAsync(Arg.Any<VersionVector>());
    }

    [Test]
    public async Task RunMergePass_does_not_re_drain_when_every_recorded_shard_is_still_routed()
    {
        // A split adds shards but removes none, and its source keeps the moved
        // entries, so a map that merely grew needs no second pass.
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var (survivorLeaf, _) = SetupSurvivorHoldingAbsorbedKey(grainFactory);
        SetupSourceShardWithEntries(grainFactory, SourceTreeId, 1);
        SetupTargetShardMocks(grainFactory, TargetTreeId, ShardCount);

        var grown = ShardMap.CreateDefault(16, 2).Slots;
        grown[15] = 2;
        StubSourceEntry(grainFactory, physicalTreeId: null, new ShardMap { Slots = grown, Version = 3 });

        state.State.InProgress = true;
        state.State.SourceTreeId = SourceTreeId;
        state.State.SourceShardCount = ShardCount;
        state.State.SourcePhysicalTreeId = SourceTreeId;
        state.State.TargetPhysicalTreeId = TargetTreeId;
        state.State.SourcePhysicalShards = [0, 1];

        await grain.RunMergePassAsync();

        Assert.That(state.State.SourcePhysicalShards, Is.EqualTo(new[] { 0, 1 }));
        Assert.That(state.State.Complete, Is.True);
        await survivorLeaf.Received(1).GetDeltaSinceAsync(Arg.Any<VersionVector>());
    }
}
