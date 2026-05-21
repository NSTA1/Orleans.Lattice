using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the per-tree compaction leaf-batch size knob. Asserts that
/// <see cref="LatticeOptions.CompactionLeafBatchSize"/> is snapshotted at pass
/// start, that mid-pass option changes do not reshape an in-flight pass,
/// that values below <see cref="LatticeOptions.MinCompactionLeafBatchSize"/>
/// are clamped, and that the in-shard cursor (NextLeafIdInShard) is persisted
/// between batches so progress survives silo crashes the same way the
/// shard cursor does.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public async Task LeafBatch_default_is_propagated_into_pass_snapshot()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain();
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(grain.CurrentLeafBatchSizeForTests,
            Is.EqualTo(LatticeOptions.DefaultCompactionLeafBatchSize));
    }

    [Test]
    public async Task LeafBatch_per_tree_override_propagates_into_pass_snapshot()
    {
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionLeafBatchSize = 4,
        };
        var (grain, _, _, grainFactory, _) = CreateGrain(options);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(grain.CurrentLeafBatchSizeForTests, Is.EqualTo(4));
    }

    [Test]
    public async Task LeafBatch_below_floor_is_clamped_to_floor_in_pass_snapshot()
    {
        LatticeOptionsResolver.ResetWarnedClampedLeafBatchSizeTreesForTests();
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionLeafBatchSize = 0,
        };
        var (grain, _, _, grainFactory, _) = CreateGrain(options);
        SetupShardWithLeaves(grainFactory, 0);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        Assert.That(grain.CurrentLeafBatchSizeForTests,
            Is.EqualTo(LatticeOptions.MinCompactionLeafBatchSize));
    }

    [Test]
    public async Task LeafBatch_yields_within_shard_when_batch_size_smaller_than_leaf_count()
    {
        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionLeafBatchSize = 2,
        };
        var (grain, state, _, grainFactory, _) = CreateGrain(options);
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf2 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf3 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf4 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leaf0, leaf1, leaf2, leaf3, leaf4);
        SetupShardWithLeaves(grainFactory, 1);

        await grain.BeginCompactionStateAsync(startFromShard: 0);

        // Tick 1: visits leaf0, leaf1 -> cursor parks at leaf2.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0),
            "shard cursor must not advance until the shard's leaf walk completes");
        Assert.That(state.State.NextLeafIdInShard, Is.EqualTo(leaf2.ToString()));
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf1).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf2).DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());

        // Tick 2: visits leaf2, leaf3 -> cursor parks at leaf4.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
        Assert.That(state.State.NextLeafIdInShard, Is.EqualTo(leaf4.ToString()));
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf2).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf3).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());

        // Tick 3: visits leaf4 -> end of shard, cursor clears, shard advances.
        await grain.ProcessNextShardAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1),
            "shard cursor must advance after the leaf walk completes");
        Assert.That(state.State.NextLeafIdInShard, Is.Null);
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf4).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task LeafBatch_resumes_from_persisted_in_shard_cursor_after_crash()
    {
        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf2 = GrainId.Create("leaf", Guid.NewGuid().ToString());

        // Simulate a silo restart mid-shard: InProgress=true, NextShardIndex=0,
        // NextLeafIdInShard=leaf1 (i.e. the previous activation completed
        // leaf0 and parked at leaf1 before crashing).
        var existingState = new FakePersistentState<TombstoneCompactionState>();
        existingState.State.InProgress = true;
        existingState.State.NextShardIndex = 0;
        existingState.State.PhysicalTreeId = TreeId;
        existingState.State.PhysicalShardIndices = new[] { 0, 1 };
        existingState.State.NextLeafIdInShard = leaf1.ToString();

        var options = new LatticeOptions
        {
            TombstoneGracePeriod = TimeSpan.FromHours(24),
            CompactionLeafBatchSize = 64,
        };
        var (grain, state, _, grainFactory, _) =
            CreateGrain(options, existingState: existingState);
        SetupShardWithLeaves(grainFactory, 0, leaf0, leaf1, leaf2);
        SetupShardWithLeaves(grainFactory, 1);

        // Resume: the coordinator picks up a tick without going through
        // BeginCompactionStateAsync (the keepalive reminder path).
        // Snapshot the leaf-batch size by re-beginning at the persisted
        // shard index, mirroring the resume logic in real callers.
        await grain.BeginCompactionStateAsync(startFromShard: state.State.NextShardIndex);
        // The cursor must survive Begin when startFromShard matches the
        // persisted NextShardIndex (the keepalive-fired resume path).
        Assert.That(state.State.NextLeafIdInShard, Is.EqualTo(leaf1.ToString()),
            "cursor must be preserved across the resume call");

        await grain.ProcessNextShardAsync();

        // leaf0 must NOT be re-visited; only leaf1 and leaf2.
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf0).DidNotReceive().CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf1).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());
        await grainFactory.GetGrain<IBPlusLeafGrain>(leaf2).Received(1).CompactTombstonesAsync(Arg.Any<TimeSpan>());

        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));
        Assert.That(state.State.NextLeafIdInShard, Is.Null);
    }
}
