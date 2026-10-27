using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for issue #5097: the orphan-pin sweep's storage read amplification.
/// <para>
/// A leaf publishes one durable pin per WAL partition and every partition's
/// checkpoint lives on the same durable row, yet the sweep read that row once
/// per pin - and every silo ran the sweep. On a 4-silo rig that was about 217k
/// leaf-state reads in two minutes. These tests pin both halves of the fix: a
/// sweep reads each leaf once however many partitions it pins, and a silo with a
/// cluster coordinator asks it for the shared outcome rather than sweeping
/// itself, falling back to a local sweep only when the coordinator is unreachable.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    private const int CoordinatedLeaves = 3;

    private static FakePinStore SeedHuskLeaves(LeafStateBook storage, int partitions)
    {
        var pins = new FakePinStore();
        for (var leaf = 0; leaf < CoordinatedLeaves; leaf++)
        {
            storage.PutHusk(OrphanLeafGrainId(leaf));
            for (var p = 0; p < partitions; p++)
            {
                pins.Seed(
                    PinnedTree,
                    BPlusLeafGrain.MaterialiserConsumerIdFor(PinnedTree, OrphanLeafGrainId(leaf), p, partitions));
            }
        }

        return pins;
    }

    private static string FirstCoordinatedConsumerId(int partitions) =>
        BPlusLeafGrain.MaterialiserConsumerIdFor(PinnedTree, OrphanLeafGrainId(0), 0, partitions);

    private static async Task<(int Reads, FakePinStore Pins)> SweepHusksAsync(
        int partitions,
        IWalOrphanPinSweepCoordinator? coordinator = null)
    {
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = SeedHuskLeaves(storage, partitions);

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(FirstCoordinatedConsumerId(partitions))));

        var (factory, _) = FactoryWithBlockedLeaf(PinnedTree);
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(PinnedTree)
            .Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry { WalPartitions = partitions }));

        var options = OrphanSweepOptions(walPartitions: partitions);
        var resolver = new LatticeOptionsResolver(factory, Monitor(options));
        var scheduler = CreateScheduler(
            factory,
            gc,
            options,
            time,
            leafStateStorage: storage,
            optionsResolver: resolver,
            orphanSweepCoordinator: coordinator);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);
        return (storage.Reads, pins);
    }

    [Test]
    public async Task The_orphan_sweep_reads_each_leaf_once_however_many_partitions_it_pins()
    {
        var single = await SweepHusksAsync(partitions: 1);
        var quad = await SweepHusksAsync(partitions: 4);

        Assert.Multiple(() =>
        {
            Assert.That(single.Pins.RemainingPins, Is.Zero);
            Assert.That(quad.Pins.RemainingPins, Is.Zero,
                "every partition's pin of a reclaimed leaf must still be retired from the shared read.");
            Assert.That(quad.Reads, Is.EqualTo(single.Reads),
                "a leaf's checkpoints for every partition live on one durable row, so the sweep must read it "
                    + "once per leaf, not once per (leaf, partition) pin.");
        });
    }

    [Test]
    public async Task A_silo_with_a_coordinator_does_not_sweep_locally()
    {
        var coordinator = Substitute.For<IWalOrphanPinSweepCoordinator>();
        coordinator.RequestSweepAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<WalOrphanPinSweepOutcome?>(null));

        var (_, pins) = await SweepHusksAsync(partitions: 2, coordinator);

        Assert.That(pins.Removals, Is.Empty,
            "the cluster coordinator owns the sweep; a silo that also swept locally would restore the "
                + "per-silo read amplification the coordinator exists to remove.");
        await coordinator.Received().RequestSweepAsync(PinnedTree, Arg.Any<bool>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_silo_falls_back_to_a_local_sweep_when_the_coordinator_is_unreachable()
    {
        var coordinator = Substitute.For<IWalOrphanPinSweepCoordinator>();
        coordinator.RequestSweepAsync(Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns<Task<WalOrphanPinSweepOutcome?>>(_ => throw new TimeoutException("coordinator unreachable"));

        var (_, pins) = await SweepHusksAsync(partitions: 2, coordinator);

        Assert.That(pins.RemainingPins, Is.Zero,
            "an unreachable coordinator must not stop orphans draining; the silo sweeps locally instead.");
    }

    [Test]
    public async Task The_runner_sweeps_without_a_rate_limit_and_reports_the_outcome()
    {
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = SeedHuskLeaves(storage, partitions: 1);
        var scheduler = SchedulerWithPinnedPartitions(
            pins, storage, time, configured: 1, pinned: 1, FirstCoordinatedConsumerId(1));
        IWalOrphanPinSweepRunner runner = scheduler;

        var first = await runner.RunAsync(PinnedTree, classifyFloorHolders: true, CancellationToken.None);
        storage.PutHusk(OrphanLeafGrainId(CoordinatedLeaves));
        pins.Seed(PinnedTree, BPlusLeafGrain.MaterialiserConsumerIdFor(PinnedTree, OrphanLeafGrainId(CoordinatedLeaves), 0, 1));
        var second = await runner.RunAsync(PinnedTree, classifyFloorHolders: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Not.Null);
            Assert.That(first!.Classified, Is.True);
            Assert.That(second, Is.Not.Null);
            Assert.That(second!.Classified, Is.False);
            Assert.That(pins.RemainingPins, Is.Zero,
                "the coordinator enforces the interval, so the runner must sweep every time it is asked.");
        });
    }
}
