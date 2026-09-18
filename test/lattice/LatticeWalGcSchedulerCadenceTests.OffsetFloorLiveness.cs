using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the liveness bound on the WAL materialiser <b>offset</b> floor
/// (issue #3178) - the defect that PR #3176 uncovered once the offset axis
/// became the trim-entitlement axis.
/// <para>
/// <b>The mechanism.</b> A leaf's projection checkpoint is scanned-through, not
/// applied-through (issue #2270): replay advances it over entries the leaf does
/// not own, and taking the MINIMUM over every leaf is what makes that safe.
/// <c>LatticeWalGc.cs</c> spells the safety argument out, and it is sound. It is
/// silent on liveness. Scan-through advance happens only DURING replay, which
/// happens only while the leaf is activated, so a leaf that deactivates freezes
/// its checkpoint at its exit position. On a converged corpus nothing
/// reactivates it, and the lowest frozen pin holds the whole tree's WAL
/// indefinitely - while the floor reports usable, no leaf is blocked, and no
/// pass fails.
/// </para>
/// <para>
/// <b>Measured on the live repocontext container</b>, tree
/// <c>repo-context-vector-index</c>: 3,066,864,131 bytes of WAL, static;
/// <c>tree_collected</c> 46, <c>passes{over_ceiling}</c> 46 with every other
/// pass arm at 0, and <c>trim_stop{offset_floor}</c> 368 = 46 x 8 with every
/// other stop arm at 0. Every collection of the tree ended over ceiling and
/// every one of its 8 partitions stopped on the offset floor every time, across
/// a pass population the advancing reach layer independently proves executed.
/// Meanwhile <c>materialiser_pin_advances{offset_only}</c> rose 17,152 -> 17,244
/// over 120s in the same process: durable offset pins are advancing at ~46/min
/// while the tree's WAL is byte-identical, which is exactly what a minimum held
/// by leaves that are NOT in the advancing set predicts.
/// </para>
/// <para>
/// <b>Why nothing already in the process reached it.</b> The floor-holder
/// sample ranks candidates ascending by HLC <i>frontier</i>, while the method it
/// feeds is documented - correctly - as classifying the pins holding the
/// <i>offset</i> floor. Those are two independent minima over the same
/// population, and the mismatch is stated in the source twice over rather than
/// inferred: <c>LatticeMetrics.WalGcBlockingPinStates</c>'s own description says
/// the sample "runs only when the cursor floor reports usable and samples by
/// lowest frontier rather than by usability". A sample on the wrong axis does
/// not contain the pins holding the binding floor, so widening any gate
/// downstream of it would only have driven the wrong leaves.
/// </para>
/// <para>
/// <b>What this change is not.</b> It does not revert issue #3174. That change
/// is right, and its own note on <c>CheckpointedCoverageUnknown</c> names what
/// is left over: such a tree's "WAL floor is held by a pin that is healthy and
/// simply old, which is a frontier-advance question rather than a coverage one".
/// This is that question answered on the offset axis. Nothing here asserts a
/// coverage hole, and a coverage-unknown pin sitting ABOVE the floor is still
/// not driven at all - it is not in the way.
/// </para>
/// <para>
/// <b>What it costs not to fix.</b> <c>LatticeWalGcScheduler</c>'s cadence rule
/// pins a tree that is blocked or over ceiling to <c>WalGcMinInterval</c>
/// regardless of whether it reclaimed anything, and the comment above it accepts
/// "a tree that is over its ceiling and cannot reclaim, polling at the floor for
/// as long as that holds". That presumes the condition eventually clears. Under
/// this defect it cannot, so the tree polls at the 30-second floor forever, and
/// pays a durable storage read for floor-holder classification on every one of
/// those passes, with <c>reclaimed</c> structurally zero.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The pin a dormant but entirely healthy leaf publishes: a real frontier,
    /// which is what makes it <c>checkpointed_coverage_unknown</c> rather than
    /// <c>checkpointed_uncovered</c>, and therefore the population issue #3168
    /// correctly excluded from the coverage repair.
    /// </summary>
    private static readonly HybridLogicalClock UsablePin = FrontierAt(5);

    /// <summary>
    /// An offset low enough to be the floor in these fixtures. The value is
    /// arbitrary; what matters is only that it is <c>&gt;= 0</c>, so it
    /// constrains an offset floor at all, and that it is the minimum of the
    /// seeded population.
    /// </summary>
    private const long FloorOffset = 100;

    /// <summary>
    /// An offset strictly above <see cref="FloorOffset"/>, for a pin that is
    /// therefore not what the trim stops at.
    /// </summary>
    private const long AboveFloorOffset = 900;

    private static GrainId LivenessLeafGrainId(int ordinal) =>
        GrainId.Create(
            "bplusleaf", "leaf-3178-" + ordinal.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>
    /// The unsuffixed consumer id a single-partition tree publishes under.
    /// </summary>
    private static string LivenessConsumerId(int ordinal) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{OrphanSweepTree}_{LivenessLeafGrainId(ordinal)}";

    /// <summary>
    /// The partition-suffixed consumer id a multi-partition tree publishes
    /// under, built exactly as <c>ILeafCursorReporter</c> builds it - one per WAL
    /// partition, all for the same leaf.
    /// </summary>
    private static string LivenessConsumerId(int ordinal, int partition) =>
        $"{LivenessConsumerId(ordinal)}_{partition.ToString(System.Globalization.CultureInfo.InvariantCulture)}";

    // ------------------------------------------------- the liveness bound

    [Test]
    public async Task A_dormant_leaf_frozen_on_the_offset_floor_is_driven_although_its_pin_is_healthy()
    {
        // Acceptance criteria 1 and 5, and the defect stated as a test. This
        // leaf is in every respect fine: it has durably checkpointed, its pin
        // carries a real frontier, it is not blocked and it is not orphaned. It
        // is merely DORMANT, and its checkpoint is therefore frozen at whatever
        // offset it last replayed to. That frozen offset is the minimum, so it
        // holds the tree's whole WAL, and before this change no path in the
        // process could reach it: the floor reports usable so no blocking report
        // names it, and the classifier declined it as coverage_unknown - which
        // was the right call about COVERAGE and left liveness unaddressed.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        var leaves = await DriveAsync(pins, storage);

        Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(0)),
            "a usable, durably-checkpointed pin sitting exactly on the tree's offset floor must be driven. "
                + "An empty set here is the defect: the leaf has consumed everything addressed to it, its "
                + "checkpoint can only advance inside an activation it will never get on a converged "
                + "corpus, and so the tree's WAL is pinned forever while every instrument reads healthy.");
    }

    [Test]
    public async Task A_usable_floor_holder_above_the_offset_floor_is_not_driven()
    {
        // The narrowness guard, and the assertion that separates this change
        // from simply driving the coverage_unknown population - which is what
        // issue #3168 measured costing 4,424 wasted activations. A pin above the
        // floor is by definition not what the trim stopped at, so driving it
        // spends an activation to move something that was never in the way.
        //
        // Equality with the floor is the discriminator #3168 did not have: its
        // sample applied no usability filter and no floor filter at all.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);
        storage.PutLive(LivenessLeafGrainId(1), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);
        pins.Seed(OrphanSweepTree, LivenessConsumerId(1), UsablePin, AboveFloorOffset);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(0)),
                "the holder ON the floor must be driven, or the guard has simply disabled the remedy.");
            Assert.That(leaves.Touched, Does.Not.Contain(LivenessLeafGrainId(1)),
                "and the holder above it must not be. Both pins are identical in every respect the "
                    + "classifier can see - same state, same frontier, same live leaf - so only the offset "
                    + "comparison can tell them apart, which is what makes this the arm that reddens if "
                    + "the gate is widened from the floor to the whole coverage_unknown population.");
        });
    }

    [Test]
    public async Task A_never_checkpointed_holder_on_the_offset_floor_is_never_driven()
    {
        // Acceptance criterion 2, carried into the new gate. This is the silent
        // data loss shape, and it must stay declined however low its offset is.
        // The leaf has applied NOTHING - ProjectionCheckpointOffsetAssigned is
        // false, so its partition-0 zero is the type default rather than
        // progress (issue #2703) - and driving it toward a durable claim for a
        // partition that replayed nothing would convert a correct block into a
        // trim entitlement the leaf never earned.
        //
        // The gate cannot reach it, and the reason is structural rather than a
        // second check: the offset branch fires only on
        // CheckpointedCoverageUnknown, which is produced ONLY by downgrading
        // CheckpointedUncovered, which ClassifyCheckpoint returns only when the
        // checkpoint is proven >= 0. A never-checkpointed leaf never enters that
        // state at all.
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(LivenessLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);

        var leaves = await DriveAsync(pins, storage);

        Assert.That(leaves.Touched, Is.Empty,
            "a leaf with no proven durable checkpoint must never be driven, whatever its pin offset says. "
                + "This is the arm that reddens if the offset comparison is ever hoisted above the state "
                + "classification - which is the obvious way to make the WAL number move, and is silent "
                + "data loss.");
    }

    [Test]
    public async Task A_holder_at_the_offset_floor_is_driven_while_a_never_checkpointed_one_beside_it_is_not()
    {
        // The discrimination test. Each fixture above can be satisfied by a
        // change that disables the new gate entirely; this one cannot. Both pins
        // sit at the same offset, both are dormant, both are sampled, and the
        // two must be treated oppositely.
        var storage = new LeafStateBook();
        storage.PutLive(LivenessLeafGrainId(0), OrphanSweepTree);
        storage.PutNeverCheckpointed(LivenessLeafGrainId(1), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, LivenessConsumerId(0), UsablePin, FloorOffset);
        pins.Seed(OrphanSweepTree, LivenessConsumerId(1), UsablePin, FloorOffset);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(LivenessLeafGrainId(0)));
            Assert.That(leaves.Touched, Does.Not.Contain(LivenessLeafGrainId(1)),
                "and the never-checkpointed holder beside it must not be, however much WAL its pin is "
                    + "holding. Reclaiming by trimming past an unproven checkpoint is the one outcome "
                    + "worse than not reclaiming at all.");
        });
    }

    // --------------------------------------------- the axis the sample ranks on

    [Test]
    public void The_sample_ranks_on_the_offset_axis_before_the_frontier()
    {
        // The root cause, at its enforcement point. The floor that is actually
        // stopping the trim is a minimum over OFFSETS, so a sample ordered by
        // frontier does not contain its holders. Here the two axes disagree
        // outright: the pin with the lower offset carries the HIGHER frontier,
        // so a frontier-ranked selector puts them in exactly the wrong order.
        var candidates = new List<WalGcFloorHolderCandidate>();

        LatticeWalGcScheduler.OfferFloorHolderCandidate(candidates, FloorHolderCap,
            new(LivenessConsumerId(1), LivenessLeafGrainId(1), true, AboveFloorOffset, FrontierAt(1)));
        LatticeWalGcScheduler.OfferFloorHolderCandidate(candidates, FloorHolderCap,
            new(LivenessConsumerId(0), LivenessLeafGrainId(0), true, FloorOffset, FrontierAt(9)));

        Assert.Multiple(() =>
        {
            Assert.That(candidates[0].ConsumerId, Is.EqualTo(LivenessConsumerId(0)),
                "the lowest OFFSET must sort first, because that is the axis "
                    + "ComputeMaterialiserOffsetFloorAsync minimises and therefore the axis whose lowest "
                    + "pin is the one the trim stops at. Ranking by frontier here returns the other pin, "
                    + "which is a confident description of something that is not the blocker.");
            Assert.That(candidates[1].ConsumerId, Is.EqualTo(LivenessConsumerId(1)));
        });
    }

    [Test]
    public void The_frontier_still_orders_pins_that_constrain_no_offset_floor()
    {
        // The secondary key, which is what keeps every pre-#3178 fixture
        // meaningful. ComputeMaterialiserOffsetFloorAsync SKIPS a -1, so a -1 is
        // the weakest pin on the offset axis rather than the strongest (issue
        // #2699). A tree that has never trimmed carries thousands of them, all
        // tied, and the frontier is the only thing left that can order them.
        //
        // The lower frontier is deliberately given the HIGHER ordinal consumer
        // id, so that the final ordinal tiebreak would order these two the other
        // way round. Without that the fixture cannot tell a working frontier key
        // from no frontier key at all - the consumer-id fallback would agree
        // with it by accident and the arm would never redden.
        var candidates = new List<WalGcFloorHolderCandidate>();

        LatticeWalGcScheduler.OfferFloorHolderCandidate(candidates, FloorHolderCap,
            new(LivenessConsumerId(0), LivenessLeafGrainId(0), true, -1, FrontierAt(9)));
        LatticeWalGcScheduler.OfferFloorHolderCandidate(candidates, FloorHolderCap,
            new(LivenessConsumerId(1), LivenessLeafGrainId(1), true, -1, FrontierAt(1)));

        Assert.That(candidates[0].ConsumerId, Is.EqualTo(LivenessConsumerId(1)),
            "with both offsets tied the lower frontier must win, or the ordering the floor-holder sample "
                + "has had since issue #3158 is silently lost on exactly the population that carries it.");
    }

    // ---------------------------------------- throughput expressed in leaves

    [Test]
    public void A_leaf_publishing_a_pin_per_partition_occupies_one_place_in_the_sample()
    {
        // Acceptance criterion 4, at the selector. A leaf publishes one pin per
        // WAL partition, and FlushDurableMaterialiserFrontierAsync reads its
        // clock ONCE for the whole batch - so every partition pin of a leaf
        // carries a byte-identical frontier, structurally rather than
        // probabilistically. Live proof: blocking_pin_state reads exactly 8 on
        // each of partitions 0-7 of repo-context-vector-index, which is one leaf
        // described eight times.
        //
        // Keyed by consumer id, a sample of eight on an eight-partition tree is
        // therefore ONE leaf, and every reactivation it licenses resolves to that
        // same leaf grain. The budget says leaves; without this it buys
        // leaves/partitions.
        var candidates = new List<WalGcFloorHolderCandidate>();

        for (var leaf = 0; leaf < 3; leaf++)
        {
            for (var partition = 0; partition < 8; partition++)
            {
                LatticeWalGcScheduler.OfferFloorHolderCandidate(candidates, FloorHolderCap,
                    new(LivenessConsumerId(leaf, partition), LivenessLeafGrainId(leaf), true,
                        FloorOffset, UsablePin));
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(candidates, Has.Count.EqualTo(3),
                "twenty-four pins across three leaves must leave three candidates, not eight. Eight is the "
                    + "cap being spent on the first leaf's partitions while the other two leaves never "
                    + "enter the sample at all.");
            Assert.That(
                candidates.Select(c => c.ConsumerId),
                Is.EquivalentTo(new[]
                {
                    LivenessConsumerId(0, 0), LivenessConsumerId(1, 0), LivenessConsumerId(2, 0),
                }),
                "and the surviving pin per leaf must be its lowest, which is the one describing the floor.");
        });
    }

    [Test]
    public async Task One_pass_drives_its_budget_in_distinct_leaves_when_each_leaf_reports_every_partition()
    {
        // Acceptance criterion 4, end to end through the scheduler, on the real
        // shape: five dormant leaves each publishing a pin for all eight WAL
        // partitions, all at the same frozen offset. Forty pins, five leaves.
        //
        // This is the fixture that observes the defect rather than the unit
        // above: the cap must bind on LEAVES, so one pass drives
        // MaxReactivationTouchesPerPass distinct leaves. Without the
        // deduplication the sample is eight pins of leaf 000, the budget is
        // stamped four times against four partitions of that one leaf, and the
        // pass drives exactly one - which is the measured field behaviour, where
        // 3 of every 4 touches came back already-driving.
        const int Leaves = 5;
        const int Partitions = 8;

        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var leaf = 0; leaf < Leaves; leaf++)
        {
            storage.PutLive(LivenessLeafGrainId(leaf), OrphanSweepTree);
            for (var partition = 0; partition < Partitions; partition++)
            {
                pins.Seed(
                    OrphanSweepTree, LivenessConsumerId(leaf, partition), UsablePin, FloorOffset);
            }
        }

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time, walPartitions: Partitions);
        await StartAndRunFirstPassAsync(scheduler, time);

        var guard = 0;
        while (leaves.Touched.Count == 0)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(500), "the sweep never drove anything at all.");
        }

        var distinctOnFirstDrivingPass = leaves.Touched.Distinct().Count();
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(distinctOnFirstDrivingPass, Is.EqualTo(TouchesPerPass),
            "a budget of four must buy four leaves. One here is the defect: the budget was spent four "
                + "times over on four partitions of a single leaf, so a tree of 2,552 dormant leaves "
                + "converges eight times slower than its own rate limits say it does.");
    }

    [Test]
    public async Task One_pass_reaches_both_blocked_leaves_when_each_reports_a_blocking_pin_per_partition()
    {
        // Acceptance criterion 4 on the OTHER supply of consumer ids. The
        // floor-holder sample now deduplicates by leaf, so the over-ceiling path
        // can no longer hand the touch loop two ids for one leaf - but the
        // blocked report still can, and does: a blocked tree names every
        // consumer id its floor is held by, which on an eight-partition tree is
        // eight ids per leaf. The touch loop therefore has to deduplicate too,
        // and it has to do so BEFORE stamping the budget, or the scarce thing
        // is spent on a call that is going to be collapsed anyway.
        //
        // Two leaves, eight partitions each. The budget of four is larger than
        // the number of leaves, so with the deduplication one pass reaches both;
        // without it, four of leaf-a's eight partition ids consume the whole
        // budget and leaf-b waits for a later pass.
        const int Partitions = 8;

        var consumers = new[] { "floor-a", "floor-b" }
            .SelectMany(key => Enumerable.Range(0, Partitions).Select(p => $"{ThroughputConsumerId(key)}_{p}"))
            .ToArray();

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNamingAll(consumers)));

        var time = new VirtualTimeProvider();
        var (factory, leaves) = FactoryWithBlockedLeaves(StrandedTree, "floor-a", "floor-b");

        var options = Adaptive();
        options.WalPartitions = Partitions;

        var scheduler = CreateScheduler(factory, gc, options, time);
        await StartAndRunFirstPassAsync(scheduler, time);

        var guard = 0;
        while (!leaves.Values.Any(HasBeenDriven))
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(500), "no blocked leaf was ever touched.");
        }

        var drivenOnFirstDrivingPass = leaves.Values.Count(HasBeenDriven);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(drivenOnFirstDrivingPass, Is.EqualTo(2),
            "both blocked leaves must be reached in the pass that reaches either. One here means the "
                + "per-pass budget was consumed by four partition ids of the same leaf - which is the "
                + "shape measured in the field as drove_already_driving = 39 = 13 x 3 against a budget "
                + "of 4, three wasted stamps for every useful one.");
    }

    private static bool HasBeenDriven(IBPlusLeafGrain leaf) =>
        leaf.ReceivedCalls().Any(
            c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.DriveStarvedCheckpointAsync));
}
