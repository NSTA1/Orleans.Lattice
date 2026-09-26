using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the floor-first ordering of arm 2's dormant floor-holder touches
/// (issue #3610).
/// <para>
/// A pass used to launch every admitted touch concurrently, so the free replay
/// permit went to whichever touch reached the leaf's replay gate first. On the
/// live estate that was always a leaf above the floor: 57 lifts in 30 passes,
/// none of them on a floor holder, and the floor stayed pinned. The sweep now
/// touches the holder nearest the floor first and alone, and only then the
/// rest.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Serves a distinct leaf substitute per grain id behind a replay gate with
    /// exactly one free slot for the whole run: the first drive to arrive takes
    /// it and every later drive is refused admission, as a full GC share refuses
    /// it. The floor holder's first bank step completes a little later than the
    /// others', which is what gives a concurrent launch the chance to hand the
    /// slot to a leaf above the floor - the race the live estate lost every time.
    /// </summary>
    private sealed class SingleSlotLeafBook(GrainId floorHolder)
    {
        private readonly Dictionary<GrainId, IBPlusLeafGrain> _leaves = [];
        private readonly object _gate = new();
        private bool _slotTaken;
        private bool _floorBankDelayed;

        public List<GrainId> Driven { get; } = [];

        public int Refused { get; private set; }

        public IBPlusLeafGrain For(GrainId leafGrainId)
        {
            lock (_gate)
            {
                if (_leaves.TryGetValue(leafGrainId, out var leaf))
                {
                    return leaf;
                }

                leaf = Substitute.For<IBPlusLeafGrain>();
                leaf.BankDurablePinAsync().Returns(_ => Bank(leafGrainId));
                leaf.DriveStarvedCheckpointAsync().Returns(_ => Drive(leafGrainId));

                _leaves[leafGrainId] = leaf;
                return leaf;
            }
        }

        private Task Bank(GrainId leafGrainId)
        {
            lock (_gate)
            {
                if (leafGrainId != floorHolder || _floorBankDelayed)
                {
                    return Task.CompletedTask;
                }

                _floorBankDelayed = true;
            }

            // Real time, not the virtual clock: the heal path awaits the bank
            // unbounded, so this arms no scheduler timer and cannot release the
            // harness mid-pass. It only has to outlast a synchronous launch loop.
            return Task.Delay(TimeSpan.FromMilliseconds(200));
        }

        private Task<LeafStarvationDriveOutcome> Drive(GrainId leafGrainId)
        {
            lock (_gate)
            {
                if (_slotTaken)
                {
                    Refused++;
                    return Task.FromException<LeafStarvationDriveOutcome>(new LatticeSaturatedException(
                        "no immediate starvation-drive capacity", OrphanSweepTree, LatticeSaturationSource.ReplayPermitAdmission));
                }

                _slotTaken = true;
                Driven.Add(leafGrainId);
                return Task.FromResult(LeafStarvationDriveOutcome.Lifted);
            }
        }
    }

    [Test]
    public async Task A_pass_with_one_free_replay_slot_spends_it_on_the_floor_holder_not_a_leaf_above_it()
    {
        // Four dormant, healthy leaves on four distinct offsets, so arm 2 admits
        // all four in one sweep (issue #3310) with leaf 0 on the floor. One slot,
        // four candidates: whoever reaches the gate first is the only leaf this
        // run ever drives.
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

        var leaves = new SingleSlotLeafBook(LivenessLeafGrainId(0));
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(
            factory, gc, OrphanSweepOptions(walPartitions: 1), time, leafStateStorage: storage);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Driven, Is.EqualTo(new[] { LivenessLeafGrainId(0) }),
                "the only free replay slot must go to the leaf holding the floor. A leaf above it here is "
                    + "the #3610 defect: the pass launched every touch at once, the floor holder's bank step "
                    + "was a moment slower, and a leaf that was not in the way took the permit.");
            Assert.That(leaves.Refused, Is.GreaterThan(0),
                "the other candidates must have reached the gate and been refused, or this run never "
                    + "contended for the slot and the ordering assertion above is vacuous.");
        });
    }
}
