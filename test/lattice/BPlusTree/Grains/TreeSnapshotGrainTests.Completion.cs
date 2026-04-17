using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TreeSnapshotGrainTests
{
    // --- Completion ---

    [Test]
    public async Task CompleteSnapshot_resets_state_and_sets_up_compaction()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        state.State.InProgress = true;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "test-op";

        await grain.CompleteSnapshotAsync();

        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);

        await grainFactory.GetGrain<ITombstoneCompactionGrain>(DestTreeId)
            .Received(1).EnsureReminderAsync();
    }

    [Test]
    public async Task CompleteSnapshot_unregisters_keepalive()
    {
        var (grain, state, reminderRegistry, _, _) = CreateGrain();
        var mockReminder = Substitute.For<IGrainReminder>();
        reminderRegistry.GetReminder(Arg.Any<GrainId>(), "snapshot-keepalive")
            .Returns(Task.FromResult(mockReminder));

        state.State.DestinationTreeId = DestTreeId;

        await grain.CompleteSnapshotAsync();

        await reminderRegistry.Received(1)
            .UnregisterReminder(Arg.Any<GrainId>(), mockReminder);
    }

    // --- Full pass ---

    [Test]
    public async Task Full_offline_pass_copies_all_shards()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        var leaf1 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0,
            new Dictionary<string, byte[]> { ["a"] = [1] }, leaf0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1,
            new Dictionary<string, byte[]> { ["b"] = [2] }, leaf1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "full-pass";

        // Lock phase: marks source shards deleted, advances to Copy.
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Copy));

        // Shard 0: Copy → Unmark
        await grain.ProcessNextPhaseAsync(); // Copy shard 0
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Unmark));
        await grain.ProcessNextPhaseAsync(); // Unmark shard 0
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));

        // Shard 1: Copy → Unmark
        await grain.ProcessNextPhaseAsync(); // Copy shard 1
        await grain.ProcessNextPhaseAsync(); // Unmark shard 1
        Assert.That(state.State.NextShardIndex, Is.EqualTo(2));

        // Completion
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }

    [Test]
    public async Task Full_online_pass_copies_all_shards_without_unmark()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);

        var leaf0 = GrainId.Create("leaf", Guid.NewGuid().ToString());
        SetupShardForSnapshot(grainFactory, SourceTreeId, 0,
            new Dictionary<string, byte[]> { ["a"] = [1] }, leaf0);
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardMocks(grainFactory, DestTreeId);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "full-pass";

        // Shard 0: Copy (advances directly)
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1));

        // Shard 1: Copy (advances directly)
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(2));

        // Completion
        await grain.ProcessNextPhaseAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.True);
    }
}
