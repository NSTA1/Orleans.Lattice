using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the permit-free first tier of the dormant floor-holder touch (issue
/// #3599), and for grading that tier against the partition head (issue #3649).
/// <para>
/// A floor holder whose pin froze below its persisted checkpoint is owed only
/// the drive's tail - flush, capture, publish - and not its replay, which is
/// the part that takes a permit from the per-silo replay gate. The sweep
/// therefore asks the leaf to bank its pin first, grades that on the same #3185
/// offset axis the drive is graded on, and escalates to
/// <see cref="IBPlusLeafGrain.DriveStarvedCheckpointAsync"/> when this
/// consumer's own pin did not move, or moved but is still behind the head of
/// its WAL partition.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// A persisted checkpoint a few entries above <see cref="FloorOffset"/>: the
    /// live shape of issue #3649, where the last pin publish trailed the last
    /// checkpoint persist by a handful of entries.
    /// </summary>
    private const long StaleCheckpointOffset = FloorOffset + 7;

    /// <summary>
    /// An exclusive partition head far above <see cref="StaleCheckpointOffset"/>,
    /// standing in for the ~2,400 entries written since the leaf went dormant.
    /// </summary>
    private const long FarHead = FloorOffset + 2_400;

    /// <summary>
    /// Serves a distinct leaf substitute per grain id whose bank step raises the
    /// seeded pin to <paramref name="bankTo"/> (or leaves it where it was when
    /// null), and whose drive raises it to <paramref name="driveTo"/> (or leaves
    /// it when null), recording every bank and every drive so the tiering is
    /// observable by identity.
    /// </summary>
    private sealed class BankingLeafBook(
        FakePinStore pins, long? bankTo, long? driveTo = null, Exception? bankThrows = null)
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

                if (bankTo is { } offset)
                {
                    pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, offset);
                }

                return Task.CompletedTask;
            });
            leaf.DriveStarvedCheckpointAsync().Returns(_ =>
            {
                Driven.Add(leafGrainId);
                if (driveTo is { } offset)
                {
                    pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, offset);
                }

                return Task.FromResult(LeafStarvationDriveOutcome.Lifted);
            });

            _leaves[leafGrainId] = leaf;
            return leaf;
        }
    }

    /// <summary>
    /// A WAL shard substitute that reports <paramref name="head"/> as its next
    /// sequence, or faults when <paramref name="headThrows"/> is supplied, and
    /// records the grain keys it was resolved under.
    /// </summary>
    private sealed class HeadBook(long head, Exception? headThrows = null)
    {
        public List<string> Keys { get; } = [];

        public IWalShardGrain For(string key)
        {
            Keys.Add(key);
            var shard = Substitute.For<IWalShardGrain>();
            shard.GetNextSequenceAsync(Arg.Any<CancellationToken>()).Returns(_ =>
                headThrows is { } ex ? ValueTask.FromException<long>(ex) : ValueTask.FromResult(head));
            return shard;
        }
    }

    private static (LatticeWalGcScheduler Scheduler, BankingLeafBook Leaves) SchedulerBanking(
        FakePinStore pins,
        IGrainStorage? storage,
        VirtualTimeProvider time,
        BankingLeafBook leaves,
        HeadBook heads)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>())
            .Returns(call => heads.For(call.ArgAt<string>(0)));

        return (
            CreateScheduler(factory, gc, OrphanSweepOptions(walPartitions: 1), time, leafStateStorage: storage),
            leaves);
    }

    private static async Task<(BankingLeafBook Leaves, HeadBook Heads, InstrumentRecorder Recorder)> RunBankingSweepAsync(
        long? bankTo,
        long head,
        long? driveTo = null,
        Exception? bankThrows = null,
        Exception? headThrows = null)
    {
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        var time = new VirtualTimeProvider();
        var heads = new HeadBook(head, headThrows);
        var (scheduler, leaves) = SchedulerBanking(
            pins, storage, time, new BankingLeafBook(pins, bankTo, driveTo, bankThrows), heads);

        var recorder = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        return (leaves, heads, recorder);
    }

    [Test]
    public async Task ExecuteAsync_does_not_drive_a_floor_holder_whose_bank_lifts_its_pin_to_the_partition_head()
    {
        // A pin at head - 1 has read its whole partition (the pin is inclusive,
        // the head exclusive), so the bank released the floor outright.
        var (leaves, heads, recorder) = await RunBankingSweepAsync(
            bankTo: AdvancedOffset, head: AdvancedOffset + 1);
        using var _ = recorder;

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Banked, Does.Contain(LivenessLeafGrainId(0)),
                "the floor holder must be banked at all, or the no-drive assertion below is vacuous.");
            Assert.That(heads.Keys, Does.Contain($"{OrphanSweepTree}/0"),
                "a lift is graded against the head of the consumer's own partition, so the head must be read.");
            Assert.That(leaves.Driven, Is.Empty,
                "a bank that carried the admitted consumer's pin to the partition head already did what the "
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
    public async Task ExecuteAsync_banks_and_then_drives_a_dormant_floor_holder_whose_bank_lift_stops_far_below_the_head()
    {
        // Issue #3649 as a test. The pin is a few entries below the leaf's
        // persisted checkpoint, which is itself ~2,400 entries behind the head.
        // The bank lifts the pin to that checkpoint - it moved - and the
        // consumer is still the floor holder. Before the fix this was graded
        // drove_lifted and the pass spent its visit without a drive.
        var (leaves, _, recorder) = await RunBankingSweepAsync(
            bankTo: StaleCheckpointOffset, head: FarHead, driveTo: FarHead - 1);
        using var __ = recorder;

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Banked, Does.Contain(LivenessLeafGrainId(0)),
                "the permit-free tier must still be tried before any permit is spent.");
            Assert.That(leaves.Driven, Does.Contain(LivenessLeafGrainId(0)),
                "a lift to a stale checkpoint leaves the consumer holding the floor, so the pass must "
                    + "escalate to the drive in the same pass rather than stop at the bank.");
            Assert.That(Outcomes(recorder, "drove_lifted"), Is.EqualTo(1),
                "only the drive, which carried the pin to the head, may be recorded on the success arm; "
                    + "the short bank lift must not be recorded there as well.");
        });
    }

    [Test]
    public async Task ExecuteAsync_grades_the_drive_after_a_short_bank_lift_from_the_banked_offset()
    {
        // The drive after a short lift does not move the pin at all. Graded
        // from the pre-bank offset, the bank's own lift would be credited to
        // it as a Lifted drive - issue #3185 rebuilt on the bank path.
        var (leaves, _, recorder) = await RunBankingSweepAsync(
            bankTo: StaleCheckpointOffset, head: FarHead, driveTo: null);
        using var _ = recorder;

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Driven, Does.Contain(LivenessLeafGrainId(0)),
                "the short lift must escalate, or the grading below is vacuous.");
            Assert.That(Outcomes(recorder, "drove_no_advance"), Is.GreaterThan(0),
                "a drive that did not move the pin past where the bank left it is a drive that did not "
                    + "advance this floor holder.");
            Assert.That(Outcomes(recorder, "drove_lifted"), Is.Zero,
                "neither the short bank lift nor a drive that moved nothing may be recorded as lifted.");
            Assert.That(Outcomes(recorder, "healed"), Is.Zero,
                "the consumer still holds the floor behind the head, so no repair may be credited.");
        });
    }

    [Test]
    public async Task ExecuteAsync_escalates_to_a_drive_when_the_partition_head_cannot_be_read_after_a_lift()
    {
        var (leaves, _, recorder) = await RunBankingSweepAsync(
            bankTo: AdvancedOffset,
            head: AdvancedOffset + 1,
            headThrows: new InvalidOperationException("head read fault"));
        using var _ = recorder;

        Assert.That(leaves.Driven, Does.Contain(LivenessLeafGrainId(0)),
            "an unreadable head leaves the release unproven, and an unproven release must not suppress "
                + "the drive (fail closed).");
    }

    [Test]
    public async Task ExecuteAsync_escalates_to_a_drive_when_the_permit_free_bank_does_not_lift_the_pin()
    {
        var (leaves, heads, recorder) = await RunBankingSweepAsync(bankTo: null, head: FarHead);
        using var _ = recorder;

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Banked, Does.Contain(LivenessLeafGrainId(0)),
                "the permit-free tier must be tried before any permit is spent.");
            Assert.That(leaves.Driven, Does.Contain(LivenessLeafGrainId(0)),
                "a bank that did not move this consumer's pin has proven only that the tail alone cannot "
                    + "heal it, so the sweep must escalate to the permit-taking drive.");
            Assert.That(heads.Keys, Is.Empty,
                "the head is read only to grade a lift, so a bank that moved nothing costs no head read.");
            Assert.That(Outcomes(recorder, "drove_no_advance"), Is.GreaterThan(0),
                "the escalated drive is still graded on the offset axis, and in this book it does not move "
                    + "the pin either.");
        });
    }

    [Test]
    public async Task ExecuteAsync_escalates_to_a_drive_when_the_permit_free_bank_faults()
    {
        var (leaves, _, recorder) = await RunBankingSweepAsync(
            bankTo: null, head: FarHead, bankThrows: new InvalidOperationException("bank fault"));
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

    [Test]
    public void IsWithinHeadLagTolerance_accepts_a_pin_exactly_at_the_tolerance_below_the_head()
    {
        const long head = 10_000;

        Assert.That(
            LatticeWalGcScheduler.IsWithinHeadLagTolerance(
                head - 1 - LatticeWalGcScheduler.BankLiftHeadLagTolerance, head),
            Is.True);
    }

    [Test]
    public void IsWithinHeadLagTolerance_rejects_a_pin_one_entry_beyond_the_tolerance()
    {
        const long head = 10_000;

        Assert.That(
            LatticeWalGcScheduler.IsWithinHeadLagTolerance(
                head - 2 - LatticeWalGcScheduler.BankLiftHeadLagTolerance, head),
            Is.False);
    }

    [Test]
    public void IsWithinHeadLagTolerance_accepts_a_pin_that_has_read_the_whole_partition()
    {
        Assert.That(LatticeWalGcScheduler.IsWithinHeadLagTolerance(pinOffset: 99, head: 100), Is.True);
    }

    [Test]
    public void BankLiftHeadLagTolerance_is_far_below_the_measured_stale_checkpoint_lag()
    {
        // The #3649 shape: lifts of <= 50 entries to a checkpoint ~2,400
        // behind the head. A tolerance that admitted that lag would grade the
        // defect as healed again.
        Assert.That(
            LatticeWalGcScheduler.IsWithinHeadLagTolerance(StaleCheckpointOffset, FarHead),
            Is.False);
    }
}
