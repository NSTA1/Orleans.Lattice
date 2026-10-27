using System.Collections.Immutable;
using System.Reflection;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task A_stalled_sweep_releases_admission_on_cancellation_or_timeout(bool timeout)
    {
        var clock = new ManualTimeProvider();
        var storage = new BlockedLeafStateStorage(1, initialReadCompletes: false, synchronousRelease: true);
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
            storage.Release();
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
            storage.Release();
        }
    }

    [Test]
    public async Task Concurrent_trees_share_silo_admission_and_abandoned_reads_keep_their_slots()
    {
        const int Concurrency = 16;
        var clock = new ManualTimeProvider();
        var storage = new BlockedLeafStateStorage(Concurrency, initialReadCompletes: false, synchronousRelease: true);
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

            storage.Release();
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
            storage.Release();
        }
    }

    [Test]
    public async Task Applying_an_old_outcome_mid_sweep_cannot_corrupt_the_new_census()
    {
        var storage = new BlockedLeafStateStorage(1, initialReadCompletes: false, synchronousRelease: true);
        var pins = new FakePinStore();
        for (var i = 0; i < 3; i++)
        {
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i), HybridLogicalClock.Zero, 42);
        }
        using var scheduler = SchedulerSweeping(pins, storage, new ManualTimeProvider());
        var sweep = ((IWalOrphanPinSweepRunner)scheduler).RunAsync(OrphanSweepTree, true, CancellationToken.None);
        await storage.FirstBatch.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            var apply = typeof(LatticeWalGcScheduler).GetMethod(
                "ApplyCoordinatedOrphanSweep", BindingFlags.NonPublic | BindingFlags.Instance)!;
            apply.Invoke(scheduler, [OrphanSweepTree, new WalOrphanPinSweepOutcome
            {
                Epoch = Guid.NewGuid(),
                Generation = 99,
                PinPopulation = 999,
                OffsetPopulation = 777,
                PartitionPopulations = ImmutableDictionary<int, int>.Empty.Add(0, 888),
            }, false]);
            storage.Release();
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
            storage.Release();
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
