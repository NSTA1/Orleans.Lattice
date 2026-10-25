using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    [NonParallelizable]
    public async Task Caller_deadlines_and_replacements_preserve_the_live_cold_owner(bool parkInStorage)
    {
        var (gate, baseline) = await WarmReplayGateAsync();
        var held = parkInStorage ? 0 : DrainEveryReplayPermit(gate);
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        wal.OnRead = () =>
        {
            entered.TrySetResult();
            return parked.Task;
        };
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, treeId: UniqueReplayPermitTree(), persistedCheckpoint: -1);
        using var activationDeadline = new CancellationTokenSource();
        var owners = new HashSet<Task>();
        var requests = new List<Task>();
        try
        {
            await ((IGrainBase)grain).OnActivateAsync(activationDeadline.Token);
            var owner = grain.ReplayBarrierForTest!;
            owners.Add(owner);
            if (parkInStorage)
                await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            else
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                while (BPlusLeafGrain.QueuedReplayPermitWaitersForTest == 0 && !owner.IsCompleted)
                    await Task.Delay(1, timeout.Token);
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(1));
            }

            activationDeadline.Cancel();
            Assert.That(await Task.WhenAny(owner, Task.Delay(100)), Is.Not.SameAs(owner),
                "the activation caller's deadline must not cancel the independent cold lifetime");

            for (var attempt = 0; attempt < 4; attempt++)
            {
                var request = grain.GetAsync("missing");
                requests.Add(request);
                owners.Add(grain.ReplayBarrierForTest!);
                using var deadline = new CancellationTokenSource();
                var caller = request.WaitAsync(deadline.Token);
                deadline.Cancel();
                Assert.ThrowsAsync(Is.InstanceOf<OperationCanceledException>(), async () => await caller);
                Assert.Multiple(() =>
                {
                    Assert.That(grain.ReplayBarrierForTest, Is.SameAs(owner),
                        "a replacement joins the live barrier instead of starting another permit owner");
                    Assert.That(owner.IsCompleted, Is.False);
                    Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest,
                        Is.EqualTo(parkInStorage ? 0 : 1));
                    Assert.That(gate.CurrentCount, Is.EqualTo(parkInStorage ? baseline - 1 : 0));
                });
            }
        }
        finally
        {
            grain.CancelReplayBarrierForTest();
            parked.TrySetResult();
            if (held > 0)
                gate.Release(held);
            foreach (var task in owners.Concat(requests))
            {
                try
                {
                    await task.WaitAsync(TimeSpan.FromSeconds(10));
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero);
        Assert.That(gate.CurrentCount, Is.EqualTo(baseline));
    }

    [TestCase(false)]
    [TestCase(true)]
    [NonParallelizable]
    public async Task Finite_interactive_and_bulk_cohort_progresses_when_capacity_and_runtime_are_available(
        bool bulkFirst)
    {
        var (gate, baseline) = await WarmReplayGateAsync();
        var held = DrainEveryReplayPermit(gate);
        var cancel = new List<Action>();
        var owners = new List<Task>();
        try
        {
            for (var i = 0; i < 2; i++)
            {
                var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                    preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
                state.State.TreeId = UniqueReplayPermitTree();
                cancel.Add(grain.CancelReplayBarrierForTest);
                using var bulk = (i == 0) == bulkFirst
                    ? LatticeReplayAdmissionContext.BeginBulkScope()
                    : null;
                await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
                owners.Add(grain.ReplayBarrierForTest!);
            }
            using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
            {
                while (BPlusLeafGrain.QueuedReplayPermitWaitersForTest < 2 && owners.All(t => !t.IsCompleted))
                    await Task.Delay(1, timeout.Token);
            }
            Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(2),
                "both real inherited classes must reach the shared semaphore");
            gate.Release();
            held--;
            var cohort = Task.WhenAll(owners);
            var settled = await Task.WhenAny(cohort, Task.Delay(TimeSpan.FromSeconds(10)));
            Assert.That(settled, Is.SameAs(cohort),
                $"both classes must progress with free={gate.CurrentCount} and queued="
                + BPlusLeafGrain.QueuedReplayPermitWaitersForTest);
            await cohort;
            Assert.Multiple(() =>
            {
                Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero);
                Assert.That(gate.CurrentCount, Is.EqualTo(1),
                    "the finite cohort reuses and returns the single available physical permit");
            });
        }
        finally
        {
            foreach (var cancelOwner in cancel)
                cancelOwner();
            if (held > 0)
                gate.Release(held);
            foreach (var owner in owners)
            {
                try
                {
                    await owner.WaitAsync(TimeSpan.FromSeconds(10));
                }
                catch (OperationCanceledException)
                {
                }
            }
        }
        Assert.That(gate.CurrentCount, Is.EqualTo(baseline));
    }
}
