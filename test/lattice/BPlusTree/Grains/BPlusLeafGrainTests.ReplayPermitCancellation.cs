using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class BPlusLeafGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Slice_cancellation_settles_before_a_late_provider_result(bool filtered)
    {
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        var provider = new TaskCompletionSource<IReadOnlyList<CommitLogSliceEntry>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        coordinator.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(provider.Task);
        coordinator.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(provider.Task);
        using var cancellation = new CancellationTokenSource();
        var reader = new ReplaySliceReader(
            coordinator, "cancelled-slice", 0, filter: filtered ? new WalKeyFilter("a", "z") : default);
        var read = reader.ReadSliceAsync(0, 1, null, cancellation.Token);
        cancellation.Cancel();
        try
        {
            var settled = await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(1)));
            Assert.That(settled, Is.SameAs(read));
            Assert.That(provider.Task.IsCompleted, Is.False);
            Assert.ThrowsAsync(Is.InstanceOf<OperationCanceledException>(), async () => await read);
        }
        finally
        {
            provider.TrySetResult([]);
            await provider.Task;
        }
        Assert.That(read.IsCanceled, Is.True, "a late result cannot resurrect the cancelled consumer");
        Assert.That(reader.Budget, Is.EqualTo(ReplaySliceReader.InitialBudget));
    }

    [TestCase(false)]
    [TestCase(true)]
    public void A_completed_provider_read_does_not_bypass_replay_cancellation(bool filtered)
    {
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();
        using var cancellation = new CancellationTokenSource();
        Task<IReadOnlyList<CommitLogSliceEntry>> CompleteAfterCancellation()
        {
            cancellation.Cancel();
            return Task.FromResult<IReadOnlyList<CommitLogSliceEntry>>([]);
        }
        coordinator.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(_ => CompleteAfterCancellation());
        coordinator.ReadSliceAsync(
                Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(_ => CompleteAfterCancellation());
        var reader = new ReplaySliceReader(
            coordinator, "cancelled-completed-slice", 0, filter: filtered ? new WalKeyFilter("a", "z") : default);

        Assert.ThrowsAsync(Is.InstanceOf<OperationCanceledException>(),
            async () => await reader.ReadSliceAsync(0, 1, null, cancellation.Token));
        Assert.That(reader.Budget, Is.EqualTo(ReplaySliceReader.InitialBudget));
    }

    [TestCase("slice")]
    [TestCase("head")]
    [TestCase("tail")]
    [NonParallelizable]
    public async Task Cancelling_a_replay_returns_its_permit_before_uncooperative_storage_completes(
        string provider)
    {
        var (gate, baseline) = await WarmReplayGateAsync();
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        wal.OnRead = () =>
        {
            if (provider != "slice")
                return Task.CompletedTask;
            entered.TrySetResult();
            return parked.Task;
        };
        if (provider == "head")
            wal.Coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                entered.TrySetResult();
                await parked.Task;
                return 3L;
            });
        if (provider == "tail")
            wal.Coordinator.GetTailOffsetAsync(Arg.Any<CancellationToken>()).Returns(async _ =>
            {
                entered.TrySetResult();
                await parked.Task;
                return 0L;
            });
        var (grain, _, _, _) = CreateGrainWithMaterialiser(
            wal.Coordinator, treeId: UniqueReplayPermitTree(), persistedCheckpoint: provider == "tail" ? 1 : -1);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        var replay = grain.ReplayBarrierForTest;
        Assert.That(replay, Is.Not.Null);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(gate.CurrentCount, Is.EqualTo(baseline - 1));
            grain.CancelReplayBarrierForTest();
            var settled = await Task.WhenAny(replay!, Task.Delay(TimeSpan.FromSeconds(1)));
            Assert.Multiple(() =>
            {
                Assert.That(settled, Is.SameAs(replay),
                    "cancellation must settle the permit owner, not only a request waiting on its barrier");
                Assert.That(parked.Task.IsCompleted, Is.False);
                Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                    "a provider that ignores cancellation must not retain a silo replay permit");
            });
        }
        finally
        {
            parked.TrySetResult();
            try
            {
                await replay!.WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Cancelled_queued_replay_replacements_leave_no_waiters_or_unowned_permits()
    {
        var (gate, baseline) = await WarmReplayGateAsync();
        var held = DrainEveryReplayPermit(gate);
        try
        {
            for (var attempt = 0; attempt < 4; attempt++)
            {
                var (grain, state, _, _) = CreateGrainWithSnapshotAndCoordinator(
                    preloadedSnapshot: null, persistedCheckpoint: 0, walHead: 0);
                state.State.TreeId = UniqueReplayPermitTree();
                await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
                var replay = grain.ReplayBarrierForTest!;
                try
                {
                    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    while (BPlusLeafGrain.QueuedReplayPermitWaitersForTest == 0 && !replay.IsCompleted)
                        await Task.Delay(1, timeout.Token);
                    Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.EqualTo(1),
                        "each replacement must reach the actual semaphore wait");
                    Assert.That(gate.CurrentCount, Is.Zero);
                    grain.CancelReplayBarrierForTest();
                    Assert.ThrowsAsync(Is.InstanceOf<OperationCanceledException>(),
                        async () => await replay.WaitAsync(TimeSpan.FromSeconds(10)));
                    Assert.Multiple(() =>
                    {
                        Assert.That(BPlusLeafGrain.QueuedReplayPermitWaitersForTest, Is.Zero,
                            "a settled attempt must not contribute residue to its replacement");
                        Assert.That(gate.CurrentCount, Is.Zero,
                            "a cancelled waiter never acquired a permit and cannot return one");
                    });
                }
                finally
                {
                    grain.CancelReplayBarrierForTest();
                    try
                    {
                        await replay.WaitAsync(TimeSpan.FromSeconds(10));
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
            }
        }
        finally
        {
            gate.Release(held);
        }
        Assert.That(gate.CurrentCount, Is.EqualTo(baseline));
    }
}
