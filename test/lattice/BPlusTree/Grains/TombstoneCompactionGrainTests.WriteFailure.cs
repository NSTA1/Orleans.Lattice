using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Class B "persisted / in-memory divergence on failing <c>WriteStateAsync</c>" regressions
/// for <see cref="Orleans.Lattice.BPlusTree.Grains.TombstoneCompactionGrain"/>. Every
/// mutating site must snapshot the affected fields, attempt the persist, and restore
/// the in-memory state (and rethrow) when the storage call fails. Otherwise the keepalive
/// reminder branch in <c>ReceiveReminder</c> (which short-circuits on
/// <c>state.State.InProgress</c>) and the empty-shard-list check in
/// <c>ProcessNextShardAsync</c> read the dirty in-memory values and either skip work
/// or restart a completed pass from shard 0.
/// </summary>
public partial class TombstoneCompactionGrainTests
{
    [Test]
    public void BeginCompactionState_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _) = CreateGrain();

        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhysicalTreeId = state.State.PhysicalTreeId;
        var prevPhysicalShardIndices = state.State.PhysicalShardIndices;

        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.BeginCompactionStateAsync(startFromShard: 3));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.EqualTo(prevInProgress),
                "InProgress must revert so the keepalive reminder branch doesn't read a phantom in-progress pass.");
            Assert.That(state.State.NextShardIndex, Is.EqualTo(prevNextShardIndex));
            Assert.That(state.State.ShardRetries, Is.EqualTo(prevShardRetries));
            Assert.That(state.State.PhysicalTreeId, Is.EqualTo(prevPhysicalTreeId));
            Assert.That(state.State.PhysicalShardIndices, Is.EqualTo(prevPhysicalShardIndices));
            Assert.That(state.WriteCount, Is.Zero, "Failed write must not be counted as a successful persist.");
        });
    }

    [Test]
    public async Task ProcessNextShard_reverts_in_memory_state_when_WriteStateAsync_throws_on_success_path()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var leafId = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardWithLeaves(grainFactory, 0, leafId);

        // Seed an in-progress pass first (this write succeeds).
        await grain.BeginCompactionStateAsync(startFromShard: 0);

        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;

        // Arm the next write so the success-path WriteStateAsync inside
        // ProcessNextShardAsync throws.
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ProcessNextShardAsync());
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.NextShardIndex, Is.EqualTo(prevNextShardIndex),
                "NextShardIndex must revert so a subsequent tick re-processes the same shard rather than skipping it.");
            Assert.That(state.State.ShardRetries, Is.EqualTo(prevShardRetries));
        });
    }

    [Test]
    public async Task CompleteCompaction_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _) = CreateGrain();

        // Seed an in-progress pass first (this write succeeds).
        await grain.BeginCompactionStateAsync(startFromShard: 5);

        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhysicalTreeId = state.State.PhysicalTreeId;
        var prevPhysicalShardIndices = state.State.PhysicalShardIndices;

        // Arm the next write so CompleteCompactionAsync's WriteStateAsync throws.
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.CompleteCompactionAsync());
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            // If InProgress stayed false, the keepalive reminder firing
            // before reactivation would observe the dirty value and silently
            // unregister itself, then the next regular reminder tick would
            // restart the pass from shard 0 instead of resuming where it
            // left off.
            Assert.That(state.State.InProgress, Is.EqualTo(prevInProgress),
                "InProgress must revert so the keepalive reminder doesn't silently unregister against a dirty value.");
            Assert.That(state.State.NextShardIndex, Is.EqualTo(prevNextShardIndex));
            Assert.That(state.State.ShardRetries, Is.EqualTo(prevShardRetries));
            Assert.That(state.State.PhysicalTreeId, Is.EqualTo(prevPhysicalTreeId));
            Assert.That(state.State.PhysicalShardIndices, Is.EqualTo(prevPhysicalShardIndices));
        });
    }
}
