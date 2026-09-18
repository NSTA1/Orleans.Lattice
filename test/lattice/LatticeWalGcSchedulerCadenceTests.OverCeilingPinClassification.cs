using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the blocking-pin classifier's reachability from the byte-ceiling
/// arm (issue #3158).
/// <para>
/// <see cref="LatticeMetrics.WalGcBlockingPinStates"/> is the only signal that
/// names <i>which</i> durable pin holds a tree's WAL floor, and it was reachable
/// from exactly one place: the floor-blocked heal path. A tree classified
/// <c>over_ceiling</c> never produces a blocking report, so the classifier was
/// never called for it - and <c>over_ceiling</c> is precisely the classification
/// a tree earns by having a WAL that will not shrink. The diagnostic was
/// structurally unavailable on the population it was built for.
/// </para>
/// <para>
/// <b>Two independent things had to change, and fixing one is indistinguishable
/// from fixing neither.</b> Reachability is the call site, which sat inside
/// <c>ObserveAndHealBlockedTreeAsync</c>. Input is
/// <c>blockingConsumerIds</c>, derived from the floor-blocked report: a tree
/// with no report supplies an empty list, so a hoisted call would have iterated
/// nothing and recorded nothing while looking entirely correct in a diff. The
/// fix draws its candidates from the sweep's own enumeration of the durable pin
/// store instead, which owes the report nothing.
/// </para>
/// <para>
/// Measured on the live repocontext container as tree
/// <c>repo-context-vector-payload</c>: 1,148,061,900 bytes of WAL reclaiming
/// nothing, and five <c>blocking_pin_state</c> arms all at zero, all under the
/// reserved partition value <c>none</c>. That is the signature of priming at
/// registration and nothing since. Its floor-blocked sibling
/// <c>repo-context-vector-index</c>, in the same process, carried 40 series with
/// real partition labels and real counts throughout - so the absence was a
/// property of the arm, not of the deployment.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Mirrors <c>LatticeWalGcScheduler.MaxFloorHolderClassificationsPerSweep</c>.
    /// Deliberately restated rather than reached for: the cap is the safety
    /// property under test, and a test that read the production constant would
    /// still pass if that constant were raised to the population size, which is
    /// the exact regression these fixtures exist to catch.
    /// </summary>
    private const int FloorHolderCap = 8;

    /// <summary>
    /// A frontier at an explicit ordinal, so a seeded population has a
    /// deterministic minimum. Higher ordinals are strictly later, so the floor
    /// is held by the lowest.
    /// </summary>
    private static HybridLogicalClock FrontierAt(int ordinal) =>
        new() { WallClockTicks = 1_000 + ordinal, Counter = 0 };

    private static string[] PositiveBlockingPinPartitions(InstrumentRecorder states) =>
        [.. states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagPartition) as string ?? string.Empty)];

    private static double FloorHolderArm(InstrumentRecorder coverage, string status) =>
        coverage.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStatus) as string) == status)
            .Sum(m => m.Value);

    [Test]
    public async Task A_tree_over_its_ceiling_classifies_the_pin_holding_its_floor()
    {
        // Acceptance criterion 1, and the defect stated as a test. This tree is
        // breaching its byte ceiling behind a floor that reports Available, so
        // it names no blocking consumer and took the branch that classified
        // nothing at all. Before the fix every measurement on this instrument
        // carried partition 'none' - the reserved value the per-tree reachability
        // priming mints - which is why a live scrape showing five zeroes was read
        // as "no pin is in a notable state" when it meant "nothing ever looked".
        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        storage.PutLive(OrphanLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, OrphanConsumerId(0));

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);
        var scheduler = SchedulerOverCeiling(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var partitions = PositiveBlockingPinPartitions(states);
        var statuses = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(partitions, Is.Not.Empty,
                "a tree that cannot shrink its WAL must produce at least one real classification; an empty "
                    + "set here is the defect, in which the only arm that names the holder is unreachable "
                    + "from the only arm the holder's tree ever takes.");
            Assert.That(partitions, Has.All.EqualTo("0"),
                "a real classification carries the numeric partition it resolved. 'none' is the reserved "
                    + "reachability prime and 'unknown' is an unparseable consumer id; either would mean the "
                    + "series exists without anything having been measured, which is the condition being fixed.");
            Assert.That(statuses, Does.Contain("checkpointed_uncovered"),
                "the leaf holds a bound tree id and a durable checkpoint, so its state is knowable and must "
                    + "be named rather than folded into 'unreadable'.");
        });
    }

    [Test]
    public async Task The_classification_is_capped_however_many_pins_the_tree_holds()
    {
        // Acceptance criterion 2, and the constraint that makes this diagnostic
        // safe to reach at all. Each classification is a durable storage read,
        // and the population is unbounded: the tree this exists for was measured
        // at 52,224 pins on one sweep. Classifying a population would turn a
        // diagnostic into a storage-read storm on a tree that is already
        // unhealthy, and would dwarf the trimming work the pass exists to do.
        //
        // The bound is asserted through the read counter rather than through the
        // recorded sample, because the read is the cost. The sweep itself reads
        // every pin once to decide what to retire - that is issue #3105's
        // behaviour and is not what is under test - so an unbounded
        // classification would exactly double the count. Removing the cap turns
        // 68 reads into 128.
        const int Population = 60;

        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(OrphanLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i), FrontierAt(i));
        }

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);
        var scheduler = SchedulerOverCeiling(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var classifications = states.Measurements.Count(m => m.Value > 0);

        Assert.Multiple(() =>
        {
            Assert.That(storage.Reads, Is.EqualTo(Population + FloorHolderCap),
                "the classification must add exactly its cap in durable reads, not one per pin. This is the "
                    + "assertion that fails if the bound is removed: the sweep's own per-pin read is already "
                    + "counted, so an uncapped classification reads the population a second time.");
            Assert.That(classifications, Is.EqualTo(FloorHolderCap),
                "and must record exactly the sample it paid for, so the recorded population and the read "
                    + "budget cannot drift apart.");
        });
    }

    [Test]
    public async Task The_classification_reports_how_much_of_the_population_it_saw()
    {
        // Acceptance criterion 3. A bounded sample is only honest if its
        // boundedness is visible: eight classifications on a 52,224-pin tree
        // read as a complete census unless something carries the denominator.
        // That is the same class of misreading - a partial observation taken for
        // a total one - that made this defect invisible in the first place, so
        // the fix must not reintroduce it one level up.
        const int Population = 20;

        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(OrphanLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i), FrontierAt(i));
        }

        using var coverage = new InstrumentRecorder(
            LatticeMetrics.WalGcFloorHolderClassification, OrphanSweepTree);
        var scheduler = SchedulerOverCeiling(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var classified = FloorHolderArm(coverage, "classified");
        var unclassified = FloorHolderArm(coverage, "unclassified");

        Assert.Multiple(() =>
        {
            Assert.That(classified, Is.EqualTo(FloorHolderCap));
            Assert.That(unclassified, Is.EqualTo(Population - FloorHolderCap));
            Assert.That(classified + unclassified, Is.EqualTo(Population),
                "the arms must partition the enumerated population, so sum by tree is the tree's whole "
                    + "durable pin count and classified / sum is the coverage fraction outright.");
        });
    }

    [Test]
    public async Task The_sample_is_the_lowest_frontier_pins_rather_than_an_arbitrary_handful()
    {
        // The sample has to be the pins that are actually holding the floor, or
        // the cap buys nothing: the durable materialiser offset floor is a
        // minimum over every pin, so a pin above the minimum is by construction
        // not the answer to "what is pinning this tree". An enumeration-ordered
        // sample of eight from tens of thousands would almost never contain the
        // holder, and would report a confident classification of the wrong pin.
        //
        // The holders here are the only leaves with no durable record at all, so
        // picking them is observable in the recorded status rather than inferred.
        const int Population = 40;

        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            // Ordinals 0..7 hold the floor and have never persisted; everything
            // above them is a live, checkpointed leaf sitting at a later
            // frontier.
            if (i < FloorHolderCap)
            {
                storage.PutMissing(OrphanLeafGrainId(i));
            }
            else
            {
                storage.PutLive(OrphanLeafGrainId(i), OrphanSweepTree);
            }

            pins.Seed(OrphanSweepTree, OrphanConsumerId(i), FrontierAt(i));
        }

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);
        var scheduler = SchedulerOverCeiling(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var statuses = states.Measurements
            .Where(m => m.Value > 0)
            .Select(m => m.Tag(LatticeMetrics.TagStatus) as string)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(statuses, Has.Length.EqualTo(FloorHolderCap));
            Assert.That(statuses, Has.All.EqualTo("no_durable_state"),
                "every pin the sample classified must be one of the eight lowest frontiers, which are the "
                    + "ones holding the floor. A single 'checkpointed_uncovered' here means the sample "
                    + "reached a pin above the minimum and therefore described something that is not the "
                    + "blocker.");
        });
    }

    [Test]
    public void The_floor_holder_sample_never_exceeds_its_cap()
    {
        // The bound at its enforcement point, exercised directly so the property
        // is pinned independently of the scheduler that consumes it. This is
        // where the read budget is actually spent: the classification reads
        // exactly the candidates this selector admitted, so a cap that leaked
        // here would leak reads no assertion downstream could recover.
        const int Offered = 1_000;

        var candidates = new List<WalGcFloorHolderCandidate>();
        for (var i = Offered - 1; i >= 0; i--)
        {
            LatticeWalGcScheduler.OfferFloorHolderCandidate(
                candidates, FloorHolderCap, UnusableCandidate(OrphanConsumerId(i), FrontierAt(i)));
        }

        var selected = candidates.Select(c => c.ConsumerId).ToArray();
        var expected = Enumerable.Range(0, FloorHolderCap).Select(i => OrphanConsumerId(i)).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(candidates, Has.Count.EqualTo(FloorHolderCap),
                "a thousand offers must leave a constant-size sample, or enumerating a 52,224-pin tree "
                    + "would retain the population in memory as well as read it.");
            Assert.That(selected, Is.EqualTo(expected),
                "and the sample must be the lowest frontiers in ascending order, whatever order they were "
                    + "offered in - the enumeration walks shard keys, not frontiers.");
        });
    }

    [Test]
    public void A_pin_offered_under_two_read_keys_occupies_one_place_in_the_sample()
    {
        // A pin duplicated across read keys by an earlier routing is one pin to
        // a reader, and the sweep already treats it as one. Admitting it twice
        // would spend two of eight slots describing a single holder and evict a
        // genuine one - and it is the deduplicated frontier that must survive,
        // because the floor is a minimum.
        var candidates = new List<WalGcFloorHolderCandidate>();

        LatticeWalGcScheduler.OfferFloorHolderCandidate(
            candidates, FloorHolderCap, UnusableCandidate(OrphanConsumerId(0), FrontierAt(500)));
        LatticeWalGcScheduler.OfferFloorHolderCandidate(
            candidates, FloorHolderCap, UnusableCandidate(OrphanConsumerId(0), FrontierAt(1)));
        LatticeWalGcScheduler.OfferFloorHolderCandidate(
            candidates, FloorHolderCap, UnusableCandidate(OrphanConsumerId(1), FrontierAt(100)));

        Assert.Multiple(() =>
        {
            Assert.That(candidates, Has.Count.EqualTo(2));
            Assert.That(candidates[0].ConsumerId, Is.EqualTo(OrphanConsumerId(0)));
            Assert.That(candidates[0].Frontier, Is.EqualTo(FrontierAt(1)),
                "the lower of a pin's two recorded frontiers is the one that describes the floor, and it "
                    + "must also re-sort the sample rather than be written over the old key in place.");
            Assert.That(candidates[1].ConsumerId, Is.EqualTo(OrphanConsumerId(1)));
        });
    }

    /// <summary>
    /// A candidate that constrains no offset floor, which is what every pin in
    /// these pre-#3178 fixtures is: the fake pin grain publishes no offsets, so
    /// the sweep reads them all as <c>-1</c> and the sample degenerates to the
    /// ascending-frontier ordering these fixtures were written against.
    /// <see cref="WalGcFloorHolderCandidate.LeafResolved"/> is false so identity
    /// falls back to the consumer id, which is the property under test here.
    /// </summary>
    private static WalGcFloorHolderCandidate UnusableCandidate(
        string consumerId, HybridLogicalClock frontier) =>
        new(consumerId, default, LeafResolved: false, Offset: -1, frontier);

    [Test]
    public async Task A_floor_blocked_tree_still_classifies_only_the_consumers_its_report_named()
    {
        // Acceptance criterion 4, and the scope guard. The sweep runs on both
        // arms, so a classification wired into it without a condition would
        // start sampling every blocked tree as well. That would contradict the
        // instrument's own contract - 'once per consumer per blocked episode',
        // latched on the consumer's budget - and would mix the pins the floor
        // actually reported as blocking with an arbitrary eight it did not,
        // under identical tags and with no way to separate them afterwards.
        const int Population = 6;

        var time = new VirtualTimeProvider();
        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(OrphanLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, OrphanConsumerId(i), FrontierAt(i));
        }

        using var states = new InstrumentRecorder(LatticeMetrics.WalGcBlockingPinStates, OrphanSweepTree);
        using var coverage = new InstrumentRecorder(
            LatticeMetrics.WalGcFloorHolderClassification, OrphanSweepTree);

        // The report names one blocker out of six pins, which is the shape the
        // capped floor report actually produces.
        var scheduler = SchedulerSweeping(pins, storage, time, blockingConsumerId: OrphanConsumerId(0));
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(states.Measurements.Count(m => m.Value > 0), Is.EqualTo(1),
                "the blocked arm classifies the blockers its report named and nothing else, so six pins "
                    + "behind a one-consumer report must still yield exactly one classification.");
            Assert.That(FloorHolderArm(coverage, "classified"), Is.Zero,
                "and must not spend the floor-holder read budget at all - the blocked arm already has a "
                    + "named blocker, which is a better answer than a sample of the same question.");
        });
    }
}
