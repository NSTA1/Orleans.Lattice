using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the permit-free first tier of the dormant floor-holder touch (issue
/// #3599).
/// <para>
/// A floor holder whose pin froze below its persisted checkpoint is owed only
/// the drive's tail - flush, capture, publish - and not its replay, which is
/// the part that takes a permit from the per-silo replay gate. The sweep
/// therefore asks the leaf to bank its pin first, grades that on the same #3185
/// offset axis the drive is graded on, and escalates to
/// <see cref="IBPlusLeafGrain.DriveStarvedCheckpointAsync"/> only when this
/// consumer's own pin did not move.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Serves a distinct leaf substitute per grain id whose bank step either
    /// lifts the seeded pin offset or leaves it where it was, recording every
    /// bank and every drive so the tiering is observable by identity.
    /// </summary>
    private sealed class BankingLeafBook(FakePinStore pins, bool bankLifts, Exception? bankThrows = null)
    {
        private readonly Dictionary<GrainId, IBPlusLeafGrain> _leaves = [];

        public List<GrainId> Banked { get; } = [];

        public List<GrainId> Driven { get; } = [];

        public IBPlusLeafGrain For(GrainId leafGrainId)
        {
            if (_leaves.TryGetValue(leafGrainId, out var leaf))
            {
                return leaf;
            }

            leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.BankDurablePinAsync().Returns(_ =>
            {
                Banked.Add(leafGrainId);
                if (bankThrows is { } ex)
                {
                    return Task.FromException(ex);
                }

                if (bankLifts)
                {
                    pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, AdvancedOffset);
                }

                return Task.CompletedTask;
            });
            leaf.DriveStarvedCheckpointAsync().Returns(_ =>
            {
                Driven.Add(leafGrainId);
                return Task.FromResult(LeafStarvationDriveOutcome.Lifted);
            });

            _leaves[leafGrainId] = leaf;
            return leaf;
        }
    }

    private static (LatticeWalGcScheduler Scheduler, BankingLeafBook Leaves) SchedulerBanking(
        FakePinStore pins,
        IGrainStorage? storage,
        VirtualTimeProvider time,
        bool bankLifts,
        Exception? bankThrows = null)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var leaves = new BankingLeafBook(pins, bankLifts, bankThrows);
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        return (
            CreateScheduler(factory, gc, OrphanSweepOptions(walPartitions: 1), time, leafStateStorage: storage),
            leaves);
    }

    private static async Task<(BankingLeafBook Leaves, InstrumentRecorder Recorder)> RunBankingSweepAsync(
        bool bankLifts, Exception? bankThrows = null)
    {
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerBanking(pins, storage, time, bankLifts, bankThrows);

        var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        return (leaves, recorder);
    }

    [Test]
    public async Task ExecuteAsync_does_not_drive_a_floor_holder_whose_pin_lifts_from_the_permit_free_bank()
    {
        var (leaves, recorder) = await RunBankingSweepAsync(bankLifts: true);
        using var _ = recorder;

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Banked, Does.Contain(LivenessLeafGrainId(0)),
                "the floor holder must be banked at all, or the no-drive assertion below is vacuous.");
            Assert.That(leaves.Driven, Is.Empty,
                "a bank that lifted the admitted consumer's own durable pin offset already did what the "
                    + "drive would have done, so spending a replay permit on a drive after it is the waste "
                    + "issue #3599 exists to remove.");
            Assert.That(Outcomes(recorder, "drove_lifted"), Is.GreaterThan(0),
                "a lift from the bank is graded on the same offset axis as a drive's, so it is recorded "
                    + "on the same success arm.");
            Assert.That(Outcomes(recorder, "drove_no_advance"), Is.Zero,
                "a pin that moved must not be recorded as not moving.");
            Assert.That(Outcomes(recorder, "healed"), Is.GreaterThan(0),
                "the consumer's pin moved off the floor, so the repair must be credited.");
        });
    }

    [Test]
    public async Task ExecuteAsync_escalates_to_a_drive_when_the_permit_free_bank_does_not_lift_the_pin()
    {
        var (leaves, recorder) = await RunBankingSweepAsync(bankLifts: false);
        using var _ = recorder;

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Banked, Does.Contain(LivenessLeafGrainId(0)),
                "the permit-free tier must be tried before any permit is spent.");
            Assert.That(leaves.Driven, Does.Contain(LivenessLeafGrainId(0)),
                "a bank that did not move this consumer's pin has proven only that the tail alone cannot "
                    + "heal it, so the sweep must escalate to the permit-taking drive.");
            Assert.That(Outcomes(recorder, "drove_no_advance"), Is.GreaterThan(0),
                "the escalated drive is still graded on the offset axis, and in this book it does not move "
                    + "the pin either.");
        });
    }

    [Test]
    public async Task ExecuteAsync_escalates_to_a_drive_when_the_permit_free_bank_faults()
    {
        var (leaves, recorder) = await RunBankingSweepAsync(
            bankLifts: false, bankThrows: new InvalidOperationException("bank fault"));
        using var _ = recorder;

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Banked, Does.Contain(LivenessLeafGrainId(0)),
                "the bank must have been attempted for its fault to mean anything.");
            Assert.That(leaves.Driven, Does.Contain(LivenessLeafGrainId(0)),
                "a bank fault is not a verdict on the leaf. It must fall through to the drive exactly as a "
                    + "bank that moved nothing does - the pre-#3599 behaviour - rather than suppress it.");
        });
    }
}
