using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the share-bounded touch fan-out of issue #3761 item 1(a).
/// <para>
/// A pass used to launch every admitted touch at once. The leaf's silo admits a
/// sweep drive only while its starvation share has a free slot, and it tests
/// that without queueing, so every touch beyond the share was refused at once -
/// 88% of them on the live container - and a refused touch was still counted as
/// an attempt. The pass now holds no more touches in flight than the share, and
/// a refusal is counted on <c>admission_refused</c> alone.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Serves a distinct leaf substitute per grain id behind a gate with room for
    /// exactly one drive at a time, as a starvation share of one has. A touch
    /// holds its place from its bank step to its drive, and the bank step takes a
    /// little real time, so touches launched together overlap and all but one of
    /// them is refused - the concurrent launch the live estate lost to.
    /// </summary>
    private sealed class OneAtATimeLeafBook
    {
        private readonly Dictionary<GrainId, IBPlusLeafGrain> _leaves = [];
        private readonly object _gate = new();
        private int _inFlight;

        public List<GrainId> Driven { get; } = [];

        public int Refused { get; private set; }

        public int MaxInFlight { get; private set; }

        public IBPlusLeafGrain For(GrainId leafGrainId)
        {
            lock (_gate)
            {
                if (_leaves.TryGetValue(leafGrainId, out var leaf))
                {
                    return leaf;
                }

                leaf = Substitute.For<IBPlusLeafGrain>();
                leaf.BankDurablePinAsync().Returns(_ => BankAsync());
                leaf.DriveStarvedCheckpointAsync().Returns(_ => Drive(leafGrainId));

                _leaves[leafGrainId] = leaf;
                return leaf;
            }
        }

        private Task BankAsync()
        {
            lock (_gate)
            {
                _inFlight++;
                MaxInFlight = Math.Max(MaxInFlight, _inFlight);
            }

            // Real time, not the virtual clock: the heal path awaits the bank
            // unbounded, so this arms no scheduler timer and cannot release the
            // harness mid-pass. It only has to outlast a synchronous launch loop.
            return Task.Delay(TimeSpan.FromMilliseconds(50));
        }

        private Task<LeafStarvationDriveOutcome> Drive(GrainId leafGrainId)
        {
            lock (_gate)
            {
                var others = _inFlight > 1;
                _inFlight--;
                if (others)
                {
                    Refused++;
                    return Task.FromException<LeafStarvationDriveOutcome>(new LatticeSaturatedException(
                        "no immediate starvation-drive capacity", OrphanSweepTree, LatticeSaturationSource.ReplayPermitAdmission));
                }

                Driven.Add(leafGrainId);
                return Task.FromResult(LeafStarvationDriveOutcome.Lifted);
            }
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_pass_holds_no_more_touches_in_flight_than_the_sweep_share_and_is_never_refused_by_its_own_drives()
    {
        // Four dormant floor holders on four distinct offsets, so arm 2 admits
        // all four in one sweep (issue #3310), behind a share of exactly one
        // drive. Launched together, three of the four are refused by the fourth.
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < 4; i++)
        {
            storage.PutLive(LivenessLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, LivenessConsumerId(i), UsablePin, FloorOffset + (i * 10));
        }

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var leaves = new OneAtATimeLeafBook();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        // A ceiling of two circulating permits is a GC share of one.
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 2, queued: 0);
        try
        {
            Assert.That(BPlusLeafGrain.SweepStarvationShare, Is.EqualTo(1), "the share this run is sized for.");

            var time = new VirtualTimeProvider();
            var scheduler = CreateScheduler(
                factory, gc, OrphanSweepOptions(walPartitions: 1), time, leafStateStorage: storage);

            using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);
            await StartAndRunFirstPassAsync(scheduler, time);
            await AdvanceAtLeastAsync(time, PastMinBlockAge);
            await scheduler.StopAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(leaves.Driven.Distinct(), Is.EquivalentTo(Enumerable.Range(0, 4).Select(LivenessLeafGrainId)),
                    "every floor holder must be driven.");
                Assert.That(leaves.MaxInFlight, Is.EqualTo(1),
                    "the pass must hold no more touches in flight than the share. More is the #3761 defect: every "
                        + "touch was launched at once and all but one of them was refused by its own siblings.");
                Assert.That(leaves.Refused, Is.Zero,
                    "no touch may be refused admission by a drive of the same pass.");
                Assert.That(Outcomes(recorder, "admission_refused"), Is.Zero);
                Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(Outcomes(recorder, "completed")),
                    "every attempted touch reached the leaf and lifted it.");
                Assert.That(Outcomes(recorder, "attempted"), Is.GreaterThanOrEqualTo(4));
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }

    /// <summary>
    /// Serves leaves whose drives all lift, except that leaf 0's first drive is
    /// refused admission, as a share held by a drive this pass does not own
    /// refuses it.
    /// </summary>
    private sealed class FirstDriveRefusedLeafBook(GrainId refusedOnce)
    {
        private readonly Dictionary<GrainId, IBPlusLeafGrain> _leaves = [];
        private readonly object _gate = new();
        private bool _refused;

        public List<GrainId> Driven { get; } = [];

        public IBPlusLeafGrain For(GrainId leafGrainId)
        {
            lock (_gate)
            {
                if (_leaves.TryGetValue(leafGrainId, out var leaf))
                {
                    return leaf;
                }

                leaf = Substitute.For<IBPlusLeafGrain>();
                leaf.BankDurablePinAsync().Returns(Task.CompletedTask);
                leaf.DriveStarvedCheckpointAsync().Returns(_ => Drive(leafGrainId));

                _leaves[leafGrainId] = leaf;
                return leaf;
            }
        }

        private Task<LeafStarvationDriveOutcome> Drive(GrainId leafGrainId)
        {
            lock (_gate)
            {
                if (leafGrainId == refusedOnce && !_refused)
                {
                    _refused = true;
                    return Task.FromException<LeafStarvationDriveOutcome>(new LatticeSaturatedException(
                        "no immediate starvation-drive capacity", OrphanSweepTree, LatticeSaturationSource.ReplayPermitAdmission));
                }

                Driven.Add(leafGrainId);
                return Task.FromResult(LeafStarvationDriveOutcome.Lifted);
            }
        }
    }

    [Test]
    [NonParallelizable]
    public async Task A_pass_re_drives_a_refused_floor_holder_once_a_sibling_frees_the_slot_instead_of_leaving_it_to_a_later_pass()
    {
        // Three floor holders behind a share of one, and the floor holder's first
        // drive is refused by a drive the pass does not own. The pass lets the
        // next holder take the slot, then gives it straight back to the floor
        // holder, because that is the slot the pass knows is free. Leaving it to
        // a later pass is the #3761 shape: the leaves above the floor are driven
        // and the one in the way is not.
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < 3; i++)
        {
            storage.PutLive(LivenessLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, LivenessConsumerId(i), UsablePin, FloorOffset + (i * 10));
        }

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var leaves = new FirstDriveRefusedLeafBook(LivenessLeafGrainId(0));
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 2, queued: 0);
        try
        {
            var time = new VirtualTimeProvider();
            var scheduler = CreateScheduler(
                factory, gc, OrphanSweepOptions(walPartitions: 1), time, leafStateStorage: storage);

            using var recorder = new InstrumentRecorder(LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);
            await StartAndRunFirstPassAsync(scheduler, time);
            await AdvanceAtLeastAsync(time, PastMinBlockAge);
            await scheduler.StopAsync(CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(leaves.Driven.Take(3), Is.EqualTo(new[] { LivenessLeafGrainId(1), LivenessLeafGrainId(0), LivenessLeafGrainId(2) }),
                    "the refused floor holder must take the first slot a sibling frees, ahead of the leaves above it.");
                Assert.That(Outcomes(recorder, "admission_refused"), Is.EqualTo(1),
                    "the refused try is counted, on its own arm.");
                Assert.That(Outcomes(recorder, "attempted"), Is.EqualTo(Outcomes(recorder, "completed")),
                    "and it is not an attempt: every attempted touch reached the leaf and lifted it.");
            });
        }
        finally
        {
            BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        }
    }
}
