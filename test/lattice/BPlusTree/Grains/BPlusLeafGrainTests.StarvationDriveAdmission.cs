using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [Test]
    [NonParallelizable]
    public async Task DriveStarvedCheckpointAsync_busy_gate_refuses_without_queueing_and_retries_after_release()
    {
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var wal = new GrowingWal();
        var treeId = UniqueStarvationDriveTree();
        using var refusals = new SaturationRefusalRecorder(treeId);
        var (grain, state, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator,
            treeId: treeId,
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
            // Issue #3761: a refused background drive is the routine answer to a
            // bounded drive, so it returns a verdict and is counted rather than
            // raised as a LatticeSaturatedException that nothing downstream
            // could attribute.
            var verdict = await drive;
            Assert.That(queued, Is.Zero,
                "GC must not add a waiter while the shared replay gate is occupied.");
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.AdmissionRefused));
            Assert.That(refusals.Count("replay_permit_admission"), Is.EqualTo(1),
                "the refusal must be counted, attributed to its source, exactly once.");
            Assert.That(refusals.Total, Is.EqualTo(1));
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
        var secondTree = UniqueStarvationDriveTree();
        using var refusals = new SaturationRefusalRecorder(secondTree);
        var (second, secondState, _, _) = CreateGrainWithMaterialiser(
            secondWal.Coordinator, treeId: secondTree, persistedCheckpoint: -1,
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
            var verdict = await second.DriveStarvedCheckpointAsync();
            Assert.That(verdict, Is.EqualTo(LeafStarvationDriveOutcome.AdmissionRefused));
            Assert.That(refusals.Count("replay_permit_admission"), Is.EqualTo(1));
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

    /// <summary>
    /// Records <see cref="LatticeMetrics.SaturationRefusals"/> for one tree, by
    /// <c>source</c> tag value.
    /// </summary>
    private sealed class SaturationRefusalRecorder : IDisposable
    {
        private readonly ConcurrentDictionary<string, long> bySource = new();
        private readonly MeterListener listener;

        public SaturationRefusalRecorder(string treeId)
        {
            listener = MeterListening.StartForInstrument(
                LatticeMetrics.SaturationRefusals,
                l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                {
                    string? tree = null;
                    string? source = null;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value as string;
                        else if (tag.Key == LatticeMetrics.TagSaturationSource) source = tag.Value as string;
                    }

                    if (tree == treeId && source is not null)
                    {
                        bySource.AddOrUpdate(source, value, (_, current) => current + value);
                    }
                }));
        }

        public long Count(string source) => bySource.TryGetValue(source, out var value) ? value : 0;

        public long Total => bySource.Values.Sum();

        public void Dispose() => listener.Dispose();
    }
}
