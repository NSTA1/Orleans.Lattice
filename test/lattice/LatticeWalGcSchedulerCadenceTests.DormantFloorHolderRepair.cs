using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the coverage repair's reachability from the GC sweep
/// (issue #3164), which is the root cause of the unbounded, unreclaimable WAL
/// growth first reported as issue #3094.
/// <para>
/// <b>The remedy and its target were disjoint by construction.</b>
/// <c>TryRepairZeroCoverageAsync</c> repairs exactly one pin state -
/// <c>checkpointed_uncovered</c>, a leaf with a proven durable checkpoint whose
/// pin is not covered by it - and it works: it is measured healing the
/// floor-blocked sibling tree in the same process, 912 repairs. But it runs only
/// from the leaf's activation and post-persist hooks, so it only ever sees a
/// leaf that has a LIVE activation. A durable pin can only hold the floor when
/// <c>ApplyDurableMaterialiserFloorAsync</c> consults it, and that happens only
/// for a consumer MISSING from the live cursor registry - a DORMANT leaf. The
/// two populations therefore could not intersect, so the repairer could never
/// reach a single pin that was actually holding a floor. This arm is what makes
/// them intersect; see the #3168 correction below for what that then exposed.
/// </para>
/// <para>
/// <b>Measured on the live repocontext container</b>, tree
/// <c>repo-context-vector-payload</c>: 1,152,579,964 bytes of WAL reclaiming
/// nothing; <c>trim_stop{reason="cursor_floor"}</c> refusing at the oldest entry
/// on all 8 partitions of every pass; all 32 sampled floor holders classified
/// <c>checkpointed_uncovered</c> and zero in any other state;
/// <c>coverage_repairs_total{outcome="repaired"}</c> absent against 2,761
/// <c>no_checkpointed_uncovered_partition</c> declines - the signature of a
/// repairer that only ever meets leaves that do not need it.
/// </para>
/// <para>
/// <b>Corrected by issue #3168 - the classification quoted above was wrong.</b>
/// Making the two populations intersect is what made the next layer legible:
/// the repairer, now reachable and running inside the activation this arm
/// creates, declined every one of those leaves with
/// <c>no_checkpointed_uncovered_partition</c>, 4,424 times. One of the two had
/// to be wrong, and it was the classifier. <c>ClassifyCheckpoint</c> returns
/// <c>checkpointed_uncovered</c> from <c>checkpoint &gt;= 0</c> alone; it never
/// measures coverage and structurally cannot, because coverage lives in a
/// private per-activation field that is absent from the persisted state. The
/// "uncovered" half is an INFERENCE from the premise that the pin is unusable -
/// the published pin is <c>min(checkpoint, covered)</c>, so unusable plus a
/// checkpoint entails no coverage. The blocked arm holds that premise by
/// construction. This arm never did: it runs only when NO dormant pin is
/// unusable, and it samples candidates with no usability filter at all. So it
/// was applying the blocked arm's inference to exactly the population the
/// premise excludes. A floor holder whose frontier is above the sentinel now
/// classifies <c>checkpointed_coverage_unknown</c> and is not driven; the leaves
/// these fixtures model are the ones that genuinely carry the sentinel.
/// </para>
/// <para>
/// <b>Consequently the pin seeds here are the blocking sentinel, not a
/// frontier.</b> A genuinely repairable holder necessarily carries
/// <c>HybridLogicalClock.Zero</c> - there is no other shape, since the pin is a
/// minimum that includes the missing coverage. Seeding a positive frontier and
/// then asserting the leaf must be driven, as these fixtures originally did,
/// modelled a leaf that cannot exist and encoded the #3168 defect as the
/// expectation. With every frontier now equal, <c>OfferFloorHolderCandidate</c>
/// falls through to its ordinal consumer-id tiebreak, which is why the ids here
/// are zero-padded.
/// </para>
/// <para>
/// <b>What these tests assert, and in what proportion.</b> The repair is a
/// durable write on a population measured at ~17,408 live pins per sweep, so
/// "does not repair" is under test at least as hard as "does". Four of these
/// fixtures exist only to prove a state is DECLINED, and
/// <c>never_checkpointed</c> is the one that matters: its leaf has applied
/// nothing, so its Zero pin is a correct block rather than a coverage hole, and
/// stamping coverage on it would convert that block into a trim entitlement the
/// leaf never earned. That is silent data loss, and it is worse than the bug
/// this fixes.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// Mirrors <c>LatticeWalGcScheduler.MaxReactivationTouchesPerPass</c>.
    /// Restated rather than read, for the same reason <see cref="FloorHolderCap"/>
    /// is: a test that read the production constant would still pass if that
    /// constant were raised to the population size, which is the regression this
    /// exists to catch.
    /// </summary>
    private const int TouchesPerPass = 4;

    /// <summary>
    /// Comfortably past <c>ReactivationMinBlockAge</c> (5 minutes), which every
    /// consumer must serve before its first touch, and comfortably inside
    /// <c>ReactivationRetryCooldown</c> (15 minutes), so no consumer can be
    /// touched twice inside the window and a touch count is a distinct-leaf
    /// count.
    /// </summary>
    private static readonly TimeSpan PastMinBlockAge = TimeSpan.FromMinutes(7);

    /// <summary>
    /// Serves a distinct leaf substitute per grain id and records which ones
    /// were actually driven.
    /// </summary>
    /// <remarks>
    /// The shared-substitute helper the blocked-arm fixtures use cannot express
    /// these tests: the whole safety property is that a repairable leaf is
    /// driven while a never-checkpointed one beside it is not, and one
    /// substitute standing in for every grain id cannot tell those two apart.
    /// </remarks>
    private sealed class LeafTouchBook
    {
        private readonly Dictionary<GrainId, IBPlusLeafGrain> _leaves = [];

        public List<GrainId> Touched { get; } = [];

        public IBPlusLeafGrain For(GrainId leafGrainId)
        {
            if (_leaves.TryGetValue(leafGrainId, out var leaf))
            {
                return leaf;
            }

            leaf = Substitute.For<IBPlusLeafGrain>();
            leaf.DriveStarvedCheckpointAsync().Returns(_ =>
            {
                Touched.Add(leafGrainId);
                return Task.FromResult(LeafStarvationDriveOutcome.Lifted);
            });

            _leaves[leafGrainId] = leaf;
            return leaf;
        }
    }

    /// <summary>
    /// A scheduler whose single tree is over its byte ceiling behind a floor
    /// that reports <c>Available</c> - the exact shape payload reports - with a
    /// per-leaf touch book so the driven set is observable by identity.
    /// </summary>
    private static (LatticeWalGcScheduler Scheduler, LeafTouchBook Leaves) SchedulerRepairing(
        FakePinStore pins,
        IGrainStorage? storage,
        VirtualTimeProvider time,
        int walPartitions = 1)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(OverCeilingReport()));

        var leaves = new LeafTouchBook();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        return (
            CreateScheduler(
                factory, gc, OrphanSweepOptions(walPartitions: walPartitions), time, leafStateStorage: storage),
            leaves);
    }

    /// <summary>
    /// The pin a leaf publishes when it cannot resolve a usable offset - the
    /// blocking sentinel, and the <b>only</b> frontier a genuinely repairable
    /// floor holder can carry.
    /// </summary>
    /// <remarks>
    /// <para>
    /// These fixtures originally seeded a repairable holder at a positive
    /// frontier, which modelled a leaf that cannot exist.
    /// <c>ResolveDurablePinForPartition</c> publishes
    /// <c>min(checkpoint, covered)</c>, so a leaf whose coverage is absent
    /// publishes <c>(Zero, -1)</c> and nothing else; a positive frontier is
    /// therefore proof that coverage was present when the pin was written. The
    /// classifier now says so (issue #3168), and a fixture that asserts a
    /// positive-frontier holder must be driven is asserting the defect.
    /// </para>
    /// <para>
    /// Note what this does <i>not</i> say. A floor-holding pin at the sentinel
    /// is still reachable on a report whose cursor floor is usable, because
    /// <c>ApplyDurableMaterialiserFloorAsync</c> skips a consumer that is
    /// present in the live registry before it ever evaluates the pin. That is
    /// exactly the population these fixtures model, and it is why the arm
    /// remains live rather than being disabled by the fix.
    /// </para>
    /// </remarks>
    private static readonly HybridLogicalClock UnusablePin = HybridLogicalClock.Zero;

    /// <summary>
    /// A leaf grain id whose ordinal is zero-padded so that a population seeded
    /// at a single frontier still has a deterministic selection order.
    /// </summary>
    /// <remarks>
    /// Every repairable holder carries <see cref="UnusablePin"/>, so the
    /// frontier can no longer separate one candidate from another and
    /// <c>OfferFloorHolderCandidate</c> falls through to its ordinal consumer-id
    /// tiebreak. Unpadded ids order <c>10</c> before <c>2</c> under that
    /// comparison, which would leave "the eight lowest" meaning something other
    /// than ordinals 0-7 and turn a deterministic assertion into a confusing
    /// one. Padding makes ordinal order and numeric order the same thing.
    /// </remarks>
    private static GrainId RepairLeafGrainId(int ordinal) =>
        GrainId.Create(
            "bplusleaf", "leaf-3164-" + ordinal.ToString("D3", System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>
    /// The materialiser consumer id <see cref="RepairLeafGrainId"/> publishes
    /// under, built exactly as <c>ILeafCursorReporter</c> builds it.
    /// </summary>
    private static string RepairConsumerId(int ordinal) =>
        $"{ILeafCursorReporter.MaterialiserConsumerIdPrefix}{OrphanSweepTree}_{RepairLeafGrainId(ordinal)}";

    /// <summary>
    /// Runs a tree past the minimum block age and returns the leaves the sweep
    /// drove, so each fixture differs only in the population it seeded.
    /// </summary>
    private static async Task<LeafTouchBook> DriveAsync(FakePinStore pins, LeafStateBook storage)
    {
        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);
        return leaves;
    }

    /// <summary>
    /// Seeds one pin whose leaf state is written by <paramref name="seed"/>, so
    /// the four declined-state fixtures differ by one line.
    /// </summary>
    private static async Task<LeafTouchBook> DriveOneAsync(Action<LeafStateBook, GrainId> seed)
    {
        var storage = new LeafStateBook();
        seed(storage, RepairLeafGrainId(0));

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        return await DriveAsync(pins, storage);
    }

    // ------------------------------------------------------- the defect itself

    [Test]
    public async Task A_dormant_repairable_floor_holder_is_driven_through_the_existing_repair()
    {
        // Acceptance criterion 1, and the founding defect stated as a test. The
        // leaf holds a bound tree id and a proven durable checkpoint, so its pin
        // is repairable - and it is dormant, which is the only way its pin can
        // be consulted for the floor at all. Before the fix nothing in the
        // process could reach it: the floor reports Available, so no blocking
        // report names it, so the sweep took the arm that classified it and then
        // did nothing with the classification.
        var leaves = await DriveOneAsync((s, leaf) => s.PutLive(leaf, OrphanSweepTree));

        Assert.That(leaves.Touched, Does.Contain(RepairLeafGrainId(0)),
            "a checkpointed-but-uncovered dormant pin is precisely what TryRepairZeroCoverageAsync exists "
                + "to repair, and the repair can only run inside an activation. An empty set here is the "
                + "defect: the remedy and its target never meet, and the WAL floor is held forever.");
    }

    [Test]
    public async Task The_repair_is_reached_without_the_floor_ever_reporting_blocked()
    {
        // The reachability claim, isolated. The remedy used to hang exclusively
        // off BlockedByUnusablePin, and payload's floor is not blocked - it is
        // usable and pinned at the oldest entry, which the three-member
        // WalGcCursorFloorState has no way to express. This asserts the drive
        // happens on a report that says Available, so the fix cannot be
        // satisfied by anything that merely re-routes the blocked arm.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);

        using var reactivations = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        var attempted = reactivations.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagOutcome) as string) == "attempted")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Is.Not.Empty);
            Assert.That(attempted, Is.EqualTo(1),
                "the arm must report a real attempt. This instrument zero-primes all fourteen arms on a "
                    + "tree's first collection, and that priming sits inside the branch this tree never "
                    + "entered - so the series COMING INTO EXISTENCE proves only that the branch was "
                    + "entered, not that anything was driven. 'attempted' is the arm that cannot be "
                    + "satisfied by priming.");
        });
    }

    // --------------------------------------------------- the declined states

    [Test]
    public async Task A_never_checkpointed_floor_holder_is_never_driven()
    {
        // Acceptance criterion 2, and the one clause in this change that is
        // load-bearing for correctness rather than for reclamation. This leaf
        // has applied NOTHING: its partition-0 offset is the type default, not
        // progress, and ProjectionCheckpointOffsetAssigned is what separates the
        // two (issue #2703). Its Zero pin is therefore correct by design - it is
        // the block that keeps the WAL prefix this leaf still owes. Driving it
        // toward coverage would stamp a durable claim for a partition that
        // replayed nothing and convert that correct block into a trim
        // entitlement, which is silent data loss and strictly worse than the
        // unbounded WAL this change exists to fix.
        var leaves = await DriveOneAsync((s, leaf) => s.PutNeverCheckpointed(leaf, OrphanSweepTree));

        Assert.That(leaves.Touched, Is.Empty,
            "a pin whose leaf has no proven durable checkpoint must never be driven toward coverage. This "
                + "is the assertion that fails if the repairable predicate is ever widened from an exact "
                + "equality to a set - and widening it is the obvious way to make the WAL number move.");
    }

    [Test]
    public async Task A_floor_holder_with_no_durable_state_is_never_driven()
    {
        // Acceptance criterion 2. No state record at all means no checkpoint to
        // build a snapshot from, so there is nothing for an activation to
        // repair. This population belongs to the bulk orphan sweep of issue
        // #3105, which retires the row outright - a different remedy, off this
        // budget entirely, and it must not be duplicated here.
        var leaves = await DriveOneAsync((s, leaf) => s.PutMissing(leaf));

        Assert.That(leaves.Touched, Is.Empty,
            "a pin with no durable leaf record has nothing to activate and nothing to repair; spending a "
                + "reactivation attempt on it would burn the budget the repairable population needs.");
    }

    [Test]
    public async Task An_orphaned_floor_holder_is_never_driven()
    {
        // Acceptance criterion 2. A husk retains its checkpoint offset but has
        // lost its tree id, which proves the leaf was reclaimed AFTER the pin
        // was written - pin registration is birth-gated on a persisted tree id.
        // The retained offset is exactly why this must be excluded explicitly:
        // read by checkpoint alone it looks repairable, and before issue #3105
        // an estate of thousands of orphans presented as a coverage problem.
        var leaves = await DriveOneAsync((s, leaf) => s.PutHusk(leaf));

        Assert.That(leaves.Touched, Is.Empty,
            "there is no leaf left to activate, so a touch could only ever time out. The husk's retained "
                + "checkpoint must not readmit it to the repairable set by the back door.");
    }

    [Test]
    public async Task An_unreadable_floor_holder_is_never_driven()
    {
        // Acceptance criterion 2, and the fail-closed case. A provider failure
        // is an UNKNOWN, not a repairable state, and pressure to reclaim is
        // precisely the condition under which a remedy must not start guessing.
        var storage = new LeafStateBook { Throws = new InvalidOperationException("provider down") };
        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var leaves = await DriveAsync(pins, storage);

        Assert.That(leaves.Touched, Is.Empty,
            "an unreadable pin state must fail closed. Treating a provider outage as repairable would make "
                + "the remedy fire hardest exactly when the evidence for it is weakest.");
    }

    [Test]
    public async Task A_repairable_holder_is_driven_while_a_never_checkpointed_one_beside_it_is_not()
    {
        // The discrimination test, and the strongest form of criterion 2. Each
        // fixture above can be satisfied by a change that disables the arm
        // entirely; this one cannot. Both pins hold the floor, both are dormant,
        // both are sampled - and the two must be treated oppositely.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);
        storage.PutNeverCheckpointed(RepairLeafGrainId(1), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);
        pins.Seed(OrphanSweepTree, RepairConsumerId(1), UnusablePin);

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Does.Contain(RepairLeafGrainId(0)),
                "the repairable holder must still be driven, or the safety guard has simply disabled the "
                    + "remedy and the WAL stays pinned.");
            Assert.That(leaves.Touched, Does.Not.Contain(RepairLeafGrainId(1)),
                "and the never-checkpointed holder beside it must not be, however much WAL its pin is "
                    + "holding. Reclaiming by trimming past an unproven checkpoint is the one outcome "
                    + "worse than not reclaiming at all.");
        });
    }

    // ---------------------------------------------------------------- bounding

    [Test]
    public async Task The_number_of_leaves_one_pass_drives_is_capped()
    {
        // Acceptance criterion 3, at the first of the two existing enforcement
        // points. Each drive is an activation that ends in a durable write, and
        // the population is ~17,408 live pins per sweep on the tree this exists
        // for, so an unbounded arm would be catastrophic rather than merely
        // expensive. Asserted on the FIRST pass that drives anything, because
        // that is where the per-pass cap actually binds.
        const int Population = 60;

        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(RepairLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, RepairConsumerId(i), UnusablePin);
        }

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);

        var guard = 0;
        while (leaves.Touched.Count == 0)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(500), "the sweep never drove anything at all.");
        }

        var firstPass = leaves.Touched.Count;
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(firstPass, Is.EqualTo(TouchesPerPass),
            "one pass must drive exactly its cap, not the population. Sixty repairable pins behind one "
                + "pass is the shape of the real tree in miniature, and an uncapped arm would activate "
                + "every one of them from a background service.");
    }

    [Test]
    public async Task The_driven_set_cannot_exceed_the_classification_sample_however_many_pins_the_tree_holds()
    {
        // Acceptance criterion 3, at the second existing enforcement point. The
        // per-pass cap alone would only slow an unbounded arm down: given enough
        // passes it would still walk the whole population. What actually bounds
        // the arm is that its input is a subset of the floor-holder sample, and
        // that sample is capped where the candidates are SELECTED, in
        // OfferFloorHolderCandidate.
        //
        // No third bound is added at the drive site deliberately. Both bounds
        // above already exist and each is enforced at exactly one point; a
        // compensating guard at the call site would mask a regression in either
        // and leave all three untestable by perturbation.
        const int Population = 60;

        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(RepairLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, RepairConsumerId(i), UnusablePin);
        }

        var leaves = await DriveAsync(pins, storage);
        var distinct = leaves.Touched.Distinct().ToArray();
        var expected = Enumerable.Range(0, FloorHolderCap).Select(RepairLeafGrainId).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(distinct, Has.Length.EqualTo(FloorHolderCap),
                "the arm may never drive more leaves than the sample it was handed, whatever the "
                    + "population.");
            Assert.That(distinct, Is.EquivalentTo(expected),
                "and the leaves it drives must be the ones OfferFloorHolderCandidate actually selects. "
                    + "Every repairable holder carries the blocking sentinel, so the frontier cannot "
                    + "separate them and the selector falls through to its ordinal consumer-id tiebreak - "
                    + "which the zero-padded ids make the same order as the numeric one. Driving a pin "
                    + "outside that sample spends a durable write on something that was never sampled.");
        });
    }

    [Test]
    public async Task A_leaf_already_driven_does_not_absorb_a_later_passs_budget()
    {
        // The drain-rate property, and the reason the stale cache is safe. The
        // repairable set is refreshed only once per OrphanSweepInterval while
        // passes run at the cadence floor, so a leaf driven on one pass stays in
        // the cache for several more. If it could re-consume a touch slot, a
        // handful of already-driven ids would absorb the whole per-pass budget
        // and starve the leaves still holding the floor - the arm would spin
        // rather than drain.
        //
        // It cannot, and this pins why: ReactivationRetryCooldown skips a
        // touched consumer by `continue` BEFORE it reaches the touch list, and
        // MaxReactivationTouchesPerPass is checked against that list rather than
        // against the iteration index, so a skipped consumer costs nothing. Eight
        // sampled holders against a per-pass cap of four therefore complete in
        // two paying passes, not in one pass repeated forever.
        const int Population = 8;

        var storage = new LeafStateBook();
        var pins = new FakePinStore();
        for (var i = 0; i < Population; i++)
        {
            storage.PutLive(RepairLeafGrainId(i), OrphanSweepTree);
            pins.Seed(OrphanSweepTree, RepairConsumerId(i), UnusablePin);
        }

        var leaves = await DriveAsync(pins, storage);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Has.Count.EqualTo(Population),
                "every sampled holder must eventually be driven. A count stuck at the per-pass cap would "
                    + "mean the first four are being re-offered while the rest starve.");
            Assert.That(leaves.Touched.Distinct().Count(), Is.EqualTo(Population),
                "and no leaf may be driven twice inside its retry cooldown, or the arm is spending its "
                    + "budget on work it has already done.");
        });
    }

    // ------------------------------------------------- the three-valued return

    [Test]
    public async Task The_episode_survives_passes_on_which_no_classification_ran()
    {
        // The most fragile part of this change, and the part most likely to be
        // "simplified" by a later reader who sees a three-valued return where a
        // list would do.
        //
        // The classifying sweep is rate limited to OrphanSweepInterval (2
        // minutes) while a pressured tree passes at the cadence floor (30
        // seconds), so roughly three passes in four produce NO classification at
        // all. That is reported as null, and it means "no new verdict", not
        // "nothing to repair". Collapsing the two would clear the blocked
        // episode on those passes - and the episode carries the attempt budget,
        // the abandoned flag and the backoff cycle. A consumer re-admitted every
        // 30 seconds restarts its 5-minute minimum block age every 30 seconds
        // and is therefore never driven at all. That is issue #2772 rebuilt one
        // level up, and it presents as a remedy that looks wired in and silently
        // never fires.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);
        await StartAndRunFirstPassAsync(scheduler, time);

        // Long enough to span several classifying sweeps AND the far greater
        // number of passes between them, but inside one retry cooldown, so a
        // correct arm drives exactly once.
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(leaves.Touched, Has.Count.EqualTo(1),
            "the consumer must age across the non-classifying passes and be driven once. Zero here means "
                + "the episode is being cleared between sweeps, so the minimum block age never accrues - "
                + "the exact silent failure the null-versus-empty distinction exists to prevent.");
    }

    [Test]
    public async Task A_tree_with_no_repairable_holder_still_ends_its_episode()
    {
        // The other half of the three-valued return, and the guard against
        // fixing the above by simply never clearing. An EMPTY list is a measured
        // verdict - the floor holders were read and none is repairable - and it
        // must retire the episode exactly as before, or a tree that healed would
        // keep a blocked episode forever and its budgets would never be pruned.
        var storage = new LeafStateBook();
        storage.PutNeverCheckpointed(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var time = new VirtualTimeProvider();
        var (scheduler, leaves) = SchedulerRepairing(pins, storage, time);

        using var reactivations = new InstrumentRecorder(
            LatticeMetrics.WalGcBlockedLeafReactivations, OrphanSweepTree);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        var attempted = reactivations.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagOutcome) as string) == "attempted")
            .Sum(m => m.Value);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Is.Empty);
            Assert.That(attempted, Is.Zero,
                "a measured 'nothing repairable' must leave the tree exactly as it was before this change: "
                    + "no episode, no budgets, no attempts.");
        });
    }

    [Test]
    public async Task A_tree_under_its_ceiling_is_not_driven()
    {
        // The scope guard, stated as design rather than as a limitation. The arm
        // inherits the byte-pressure gate that licenses the classification, and
        // that is the right population: a tree inside its ceiling is not
        // exhibiting unbounded growth, so there is no symptom to explain and no
        // justification for spending durable reads and activations on it. The
        // consequence to state plainly is that a deployment with no ceiling
        // configured gets no repair from this arm.
        var storage = new LeafStateBook();
        storage.PutLive(RepairLeafGrainId(0), OrphanSweepTree);

        var pins = new FakePinStore();
        pins.Seed(OrphanSweepTree, RepairConsumerId(0), UnusablePin);

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(Report(0)));

        var leaves = new LeafTouchBook();
        var factory = FactoryWithTrees(OrphanSweepTree);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(call => leaves.For(call.ArgAt<GrainId>(0)));
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>())
            .Returns(call => pins.For(call.ArgAt<string>(0)));

        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(factory, gc, OrphanSweepOptions(), time, leafStateStorage: storage);
        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, PastMinBlockAge);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaves.Touched, Is.Empty,
                "a healthy tree must not be swept for repairable holders at all.");
            Assert.That(pins.KeysRead, Is.Empty,
                "and must not read the durable pin store, which is what keeps the cost of this arm "
                    + "proportional to the population that has the problem.");
        });
    }
}
