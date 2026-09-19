using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the orphan sweep's reachability from the byte-ceiling arm
/// (issue #3154).
/// <para>
/// Every remedy the scheduler has for a stale durable materialiser pin - the
/// bulk sweep of issue #3105 and the per-consumer retirement it supersedes -
/// sits inside <c>if (floorBlocked)</c>, a predicate that asks why the
/// <i>consumer cursor</i> floor cannot move. A tree can equally be stranded by
/// the durable <i>materialiser offset</i> floor, which
/// <c>ComputeMaterialiserOffsetFloorAsync</c> takes as a minimum over the
/// leaves that reported an offset rather than over the leaves that owe entries.
/// One stale pin there holds the trim frontier indefinitely while
/// <see cref="WalGcCursorFloorState"/> stays <c>Available</c>, so the pass
/// classifies <c>over_ceiling</c>, names no blocking consumer, and took the
/// branch that did nothing at all.
/// </para>
/// <para>
/// This is the third application of the lesson issue #3119 recorded when it
/// added the arm - the blocked predicate encodes a <i>cause</i> while a breach
/// is a <i>condition</i> - and the first that reaches the remedy rather than the
/// cadence. #3119 held such a tree at the cadence floor on the reasoning that
/// "pass frequency is the only lever the policy has left", which was true only
/// because this lever was unreachable. A stale pin is not an operator's ceiling
/// being legitimately unreachable; it is a reclaimable backlog that presents
/// identically, and the arm that could tell the two apart never ran.
/// </para>
/// <para>
/// Measured on the live repocontext container as tree
/// <c>repo-context-vector-payload</c>: 234 consecutive <c>over_ceiling</c>
/// passes, zero reclaimed, every <c>wal_gc_blocking_pin_state</c> arm at zero,
/// and <c>wal_gc_orphan_pin_sweep</c> absent entirely against 1.07 GiB of
/// retained WAL. The series primes below the sweep's early returns, so its
/// absence is proof the method was never entered rather than a missing
/// instrument - its healthy sibling in the same process reported it throughout.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// A scheduler whose single tree is over its byte ceiling behind a floor
    /// that reports <c>Available</c>, wired to the same fake pin store and leaf
    /// state book the blocked-arm sweep tests use. The only difference from
    /// <c>SchedulerSweeping</c> is the report, which is the whole point: the
    /// pin population and the storage are identical, so any difference in
    /// outcome is attributable to the arm alone.
    /// </summary>
    private static LatticeWalGcScheduler SchedulerOverCeiling(
        FakePinStore pins,
        IGrainStorage? storage,
        VirtualTimeProvider time,
        bool overCeiling = true)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(overCeiling ? OverCeilingReport() : Report(0)));

        var (factory, _) = FactoryWithBlockedLeaf(OrphanSweepTree);
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        return CreateScheduler(factory, gc, OrphanSweepOptions(), time, leafStateStorage: storage);
    }

    [Test]
    public async Task A_tree_over_its_ceiling_with_an_available_floor_retires_its_orphaned_pins()
    {
        // The defect, stated as a test. This tree is breaching its ceiling and
        // its pins are husks, so there is both a reason to act and something
        // to retire - and before the fix the sweep was not merely slow here, it
        // was unreachable, because the only call sites hang off a floor state
        // this tree does not report.
        const int Orphans = 12;

        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Orphans; i++)
        {
            storage.PutHusk(OrphanLeafGrainId(i));
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i));
        }

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerOverCeiling(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var retired = sweep.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == "retired")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(sweep.Measurements, Is.Not.Empty,
                "the sweep primes its series below its own early returns, so an empty recorder means the "
                    + "method was never entered - which is the defect, not a missing instrument.");
            Assert.That(retired, Is.EqualTo(Orphans));
            Assert.That(pins.RemainingPins, Is.Zero,
                "the trim frontier is a minimum over every pin, so a backlog that is 11/12 drained releases "
                    + "exactly as much WAL as an untouched one.");
        });
    }

    [Test]
    public async Task The_over_ceiling_sweep_leaves_a_live_leafs_pin_in_place()
    {
        // Fail-closed on the new arm. Being over a ceiling is pressure to
        // reclaim, and pressure is precisely the condition under which a remedy
        // must not start guessing: retiring a live leaf's pin would authorise a
        // trim over a prefix it has not replayed, trading a bounded disk cost
        // for unbounded data loss.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        storage.PutLive(OrphanLeafGrainId(0), OrphanSweepTree);
        storage.PutHusk(OrphanLeafGrainId(1));

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, OrphanConsumerId(0));
        pins.Seed(OrphanSweepTree, OrphanConsumerId(1));

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerOverCeiling(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var removed = pins.Removals.Select(r => r.ConsumerId).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(removed, Does.Contain(OrphanConsumerId(1)),
                "the husk must still be retired, or the safety guard has simply disabled the arm.");
            Assert.That(removed, Does.Not.Contain(OrphanConsumerId(0)),
                "a leaf that still holds its tree id is live, and its pin must survive the sweep however "
                    + "far over its ceiling the tree is.");
            Assert.That(pins.RemainingPins, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_tree_under_its_ceiling_with_an_available_floor_does_not_sweep()
    {
        // The scope guard, and the reason the new call site is conditioned on
        // the breach rather than simply hoisted out of the branch. A healthy
        // tree has no symptom to explain, and sweeping it would put a
        // fan-out of pin-store reads on every tree on every pass in exchange
        // for nothing. The sweep's own rate limiter would bound that cost but
        // would not justify it.
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        storage.PutHusk(OrphanLeafGrainId(0));
        pins.Seed(OrphanSweepTree, OrphanConsumerId(0));

        using var sweep = new InstrumentRecorder(LatticeMetrics.WalGcOrphanPinSweep, OrphanSweepTree);
        var scheduler = SchedulerOverCeiling(pins, storage, time, overCeiling: false);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sweep.Measurements, Is.Empty,
                "a tree that is neither blocked nor breaching must not pay for the sweep.");
            Assert.That(pins.KeysRead, Is.Empty,
                "and must not read the durable pin store at all.");
        });
    }
}
