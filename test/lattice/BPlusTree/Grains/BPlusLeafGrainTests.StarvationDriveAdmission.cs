using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task DriveStarvedCheckpointAsync_busy_gate_refuses_without_queueing_and_retries_after_release()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: UniqueStarvationDriveTree(),
            persistedCheckpoint: -1,
            starvationDriveBudget: TestStarvationDriveBudget,
            maxConcurrentReplays: 1);
        await ActivateAsync(grain);
        wal.GrowTo(3);
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
        Assert.That(gate.Wait(0), Is.True);
        try
        {
            var drive = grain.DriveStarvedCheckpointAsync();
            var queued = BPlusLeafGrain.QueuedReplayPermitWaitersForTest;
            var fault = Assert.ThrowsAsync<LatticeSaturatedException>(async () => await drive);
            Assert.That(queued, Is.Zero,
                "GC must not add a waiter while the shared replay gate is occupied.");
            Assert.That(fault!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(-1));
        }
        finally
        {
            gate.Release();
        }

        try
        {
            await grain.DriveStarvedCheckpointAsync();
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(3));
            Assert.That(gate.CurrentCount, Is.EqualTo(1));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    [Test]
    [NonParallelizable]
    public async Task DriveStarvedCheckpointAsync_caps_gc_across_trees_and_leaves_foreground_capacity()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var firstWal = new GrowingWal();
        var secondWal = new GrowingWal();
        var (first, _, _, _) = CreateGrainWithMaterialiser(
            firstWal.Coordinator, treeId: UniqueStarvationDriveTree(), persistedCheckpoint: -1,
            starvationDriveBudget: TimeSpan.FromSeconds(10), maxConcurrentReplays: 2);
        var (second, secondState, _, _) = CreateGrainWithMaterialiser(
            secondWal.Coordinator, treeId: UniqueStarvationDriveTree(), persistedCheckpoint: -1,
            starvationDriveBudget: TimeSpan.FromSeconds(10), maxConcurrentReplays: 2);
        await ActivateAsync(first);
        await ActivateAsync(second);
        firstWal.GrowTo(3);
        secondWal.GrowTo(3);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var park = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        firstWal.OnRead = () =>
        {
            entered.TrySetResult();
            return park.Task;
        };
        var firstDrive = first.DriveStarvedCheckpointAsync();
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest!;
            Assert.That(gate.CurrentCount, Is.EqualTo(1), "first drive must really hold a permit.");
            var fault = Assert.ThrowsAsync<LatticeSaturatedException>(
                async () => await second.DriveStarvedCheckpointAsync());
            Assert.That(fault!.SaturationSource, Is.EqualTo(LatticeSaturationSource.ReplayPermitAdmission));
            Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero);
            Assert.That(secondState.State.ProjectionCheckpointOffset, Is.EqualTo(-1));

            var foregroundWal = new GrowingWal();
            foregroundWal.GrowTo(3);
            var (foreground, foregroundState, _, _) = CreateGrainWithMaterialiser(
                foregroundWal.Coordinator, treeId: UniqueStarvationDriveTree(), persistedCheckpoint: -1);
            await ActivateAsync(foreground).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(foregroundState.State.ProjectionCheckpointOffset, Is.EqualTo(3));
            Assert.That(park.Task.IsCompleted, Is.False);
            Assert.That(gate.CurrentCount, Is.EqualTo(1));
        }
        finally
        {
            park.TrySetResult();
            await firstDrive;
        }

        try
        {
            await second.DriveStarvedCheckpointAsync();
            Assert.That(secondState.State.ProjectionCheckpointOffset, Is.EqualTo(3),
                "returning a GC slot must let a previously refused tree make progress.");
            Assert.That(BPlusLeafGrain.ReplayConcurrencyGateForTest!.CurrentCount, Is.EqualTo(2));
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
