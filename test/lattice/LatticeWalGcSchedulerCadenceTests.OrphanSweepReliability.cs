using System.Collections.Immutable;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task A_stalled_sweep_releases_admission_on_cancellation_or_timeout(bool timeout, bool queuedRelease)
    {
        var clock = new ManualTimeProvider();
        var storage = new BlockedLeafStateStorage(1, initialReadCompletes: false,
            synchronousRelease: true, queueReleaseContinuations: queuedRelease);
        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, OrphanConsumerId(0));
        using var scheduler = SchedulerSweeping(pins, storage, clock);
        using var cancellation = new CancellationTokenSource();
        IWalOrphanPinSweepRunner runner = scheduler;
        var sweep = runner.RunAsync(OrphanSweepTree, true, cancellation.Token);
        await storage.FirstBatch.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            if (timeout)
            {
                clock.Advance(LatticeWalGcScheduler.OrphanSweepOperationBudget);
                Assert.ThrowsAsync<TimeoutException>(() => sweep.WaitAsync(TimeSpan.FromSeconds(5)));
            }
            else
            {
                cancellation.Cancel();
                Assert.CatchAsync<OperationCanceledException>(() => sweep.WaitAsync(TimeSpan.FromSeconds(5)));
            }

            Assert.That(pins.Removals, Is.Empty, "A timeout is not evidence that the publisher is gone.");
            ReleaseAndDrainOrphanReads(storage, scheduler);
            var recovered = await runner.RunAsync(OrphanSweepTree, true, CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Multiple(() =>
            {
                Assert.That(recovered, Is.Not.Null);
                Assert.That(pins.RemainingPins, Is.Zero);
                Assert.That(storage.Outstanding, Is.Zero);
            });
        }
        finally
        {
            ReleaseAndDrainOrphanReads(storage, scheduler);
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Concurrent_trees_share_silo_admission_and_abandoned_reads_keep_their_slots(bool queuedRelease)
    {
        const int Concurrency = 16;
        var clock = new ManualTimeProvider();
        var storage = new BlockedLeafStateStorage(Concurrency, initialReadCompletes: false,
            synchronousRelease: true, queueReleaseContinuations: queuedRelease);
        var pins = new FakePinStore();
        for (var i = 0; i < Concurrency; i++)
        {
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i));
        }
        const string OtherTree = "other-sweep";
        pins.Seed(OtherTree, OrphanConsumerId(100, OtherTree));
        using var scheduler = SchedulerSweeping(pins, storage, clock);
        IWalOrphanPinSweepRunner runner = scheduler;
        using var queuedCancellation = new CancellationTokenSource();
        var first = runner.RunAsync(OrphanSweepTree, false, CancellationToken.None);
        await storage.FirstBatch.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            var queued = runner.RunAsync(OtherTree, false, queuedCancellation.Token);
            Assert.Multiple(() =>
            {
                Assert.That(queued.IsCompleted, Is.False);
                Assert.That(storage.Reads, Is.EqualTo(Concurrency));
            });
            queuedCancellation.Cancel();
            Assert.CatchAsync<OperationCanceledException>(() => queued.WaitAsync(TimeSpan.FromSeconds(5)));

            clock.Advance(LatticeWalGcScheduler.OrphanSweepOperationBudget);
            Assert.ThrowsAsync<TimeoutException>(() => first.WaitAsync(TimeSpan.FromSeconds(5)));
            for (var retry = 0; retry < 3; retry++)
            {
                Assert.ThrowsAsync<TimeoutException>(() => runner.RunAsync(OtherTree, false, CancellationToken.None));
            }

            Assert.Multiple(() =>
            {
                Assert.That(storage.Reads, Is.EqualTo(Concurrency), "Timed-out provider calls still consume admission.");
                Assert.That(storage.Outstanding, Is.EqualTo(Concurrency));
                Assert.That(storage.MaximumOutstanding, Is.EqualTo(Concurrency));
                Assert.That(pins.Removals, Is.Empty);
            });

            if (queuedRelease)
            {
                storage.Release();
                Assert.That(storage.Outstanding, Is.EqualTo(Concurrency),
                    "Posted continuations must remain pending until explicitly resumed.");
                Assert.That(scheduler.OrphanLeafReadsDrained, Is.False);
            }
            ReleaseAndDrainOrphanReads(storage, scheduler);
            await runner.RunAsync(OtherTree, false, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Multiple(() =>
            {
                Assert.That(storage.Reads, Is.EqualTo(Concurrency + 1));
                Assert.That(storage.Outstanding, Is.Zero);
                Assert.That(pins.Removals.Select(x => x.Key), Is.EquivalentTo(new[] { OtherTree }));
            });
        }
        finally
        {
            ReleaseAndDrainOrphanReads(storage, scheduler);
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Applying_an_old_outcome_mid_sweep_cannot_corrupt_the_new_census(bool queuedRelease)
    {
        var storage = new BlockedLeafStateStorage(1, initialReadCompletes: false,
            synchronousRelease: true, queueReleaseContinuations: queuedRelease);
        var pins = new FakePinStore();
        for (var i = 0; i < 3; i++)
        {
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i), HybridLogicalClock.Zero, 42);
        }
        var clock = new VirtualTimeProvider();
        var coordinator = Substitute.For<IWalOrphanPinSweepCoordinator>();
        coordinator.RequestSweepAsync(OrphanSweepTree, Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<WalOrphanPinSweepOutcome?>(new WalOrphanPinSweepOutcome
            {
                Epoch = Guid.NewGuid(),
                Generation = 99,
                PinPopulation = 999,
                OffsetPopulation = 777,
                PartitionPopulations = ImmutableDictionary<int, int>.Empty.Add(0, 888),
            }));
        using var scheduler = SchedulerSweeping(pins, storage, clock, orphanSweepCoordinator: coordinator);
        var sweep = ((IWalOrphanPinSweepRunner)scheduler).RunAsync(OrphanSweepTree, true, CancellationToken.None);
        await storage.FirstBatch.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            storage.CompleteNewReads = true;
            await StartAndRunFirstPassAsync(scheduler, clock);
            await coordinator.Received().RequestSweepAsync(
                OrphanSweepTree, Arg.Any<bool>(), Arg.Any<CancellationToken>());
            ReleaseAndDrainOrphanReads(storage, scheduler);
            var completed = await sweep.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Multiple(() =>
            {
                Assert.That(completed!.PinPopulation, Is.EqualTo(3));
                Assert.That(completed.OffsetPopulation, Is.EqualTo(3));
                Assert.That(completed.PartitionPopulations![0], Is.EqualTo(3));
            });
        }
        finally
        {
            ReleaseAndDrainOrphanReads(storage, scheduler);
            await scheduler.StopAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task Completed_leaf_memo_hits_allocate_zero_bytes()
    {
        var storage = new LeafStateBook();
        storage.PutHusk(OrphanLeafGrainId(0));
        using var scheduler = SchedulerSweeping(new FakePinStore(), storage, new ManualTimeProvider());
        var memo = new Dictionary<GrainId, LeafCheckpointSnapshot?>();
        var leaf = OrphanLeafGrainId(0);
        await scheduler.ReadBlockingPinStateMemoisedAsync(memo, leaf, 0, CancellationToken.None);
        for (var i = 0; i < 1024; i++)
        {
            scheduler.ReadBlockingPinStateMemoisedAsync(memo, leaf, 0, CancellationToken.None).GetAwaiter().GetResult();
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        var orphaned = 0;
        for (var i = 0; i < 100_000; i++)
        {
            var read = scheduler.ReadBlockingPinStateMemoisedAsync(memo, leaf, i % 4, CancellationToken.None);
            if (read.GetAwaiter().GetResult().State == WalGcBlockingPinState.Orphaned)
            {
                orphaned++;
            }
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Multiple(() =>
        {
            Assert.That(orphaned, Is.EqualTo(100_000));
            Assert.That(storage.Reads, Is.EqualTo(1));
            Assert.That(allocated, Is.Zero, "A completed memo hit must not recreate per-pin Task wrappers.");
        });
    }

    [Test]
    public void A_hung_coordinator_request_is_bounded_even_when_it_ignores_cancellation()
    {
        var clock = new ManualTimeProvider();
        var pending = new TaskCompletionSource<WalOrphanPinSweepOutcome?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var grain = Substitute.For<IWalOrphanPinSweepGrain>();
        grain.RequestSweepAsync(true).Returns(pending.Task);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalOrphanPinSweepGrain>(OrphanSweepTree).Returns(grain);
        var coordinator = new WalOrphanPinSweepCoordinator(factory, clock);
        var request = coordinator.RequestSweepAsync(OrphanSweepTree, true, CancellationToken.None);
        clock.Advance(LatticeWalGcScheduler.OrphanSweepOperationBudget);

        Assert.ThrowsAsync<TimeoutException>(() => request);
        pending.SetException(new InvalidOperationException("late RPC failure after the caller timed out"));
    }
}
