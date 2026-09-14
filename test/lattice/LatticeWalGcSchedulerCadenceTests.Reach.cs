using NSubstitute;
using Orleans.Lattice.Testing.Hygiene;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the WAL GC reachability layer added by issue #3075.
/// <para>
/// <see cref="LatticeMetrics.WalGcPasses"/> and
/// <see cref="LatticeMetrics.WalGcInterval"/> are both recorded inside the
/// per-tree collection, which a pass reaches only after clearing several
/// earlier exits. When one of those exits is taken the instruments do not
/// advance - and a series that exists and does not advance is byte-identical
/// to one being measured as zero. The usual remedy, siting the instrument
/// outside the failing region, is unavailable: the quantity being counted only
/// exists inside it.
/// </para>
/// <para>
/// <b>Why every arm advances instead of priming a zero.</b> The construction
/// this generalises (the <c>partition="none"</c> layer of issue #3042) mints
/// zeros, and a zero-priming layer <i>cannot</i> answer the question asked
/// here. <c>Add(0)</c> is idempotent on a counter's exported value, so a series
/// primed once and a series primed ten thousand times are byte-identical and no
/// sample count exists on a counter to separate them. Priming can establish
/// that a region was reached <i>at least once</i> and nothing more. That makes
/// a priming layer an instance of the very defect class it was prescribed to
/// cure - a series that exists and does not advance - which is why these arms
/// increment by one.
/// </para>
/// <para>
/// Every assertion below that concerns advancement therefore reads a <b>sum</b>
/// and never a measurement <i>count</i>. A count of three zero-primed
/// measurements is three, exactly as a count of three advancing ones is, so a
/// fixture asserting the count would hold under both designs and prove nothing
/// about the one that matters.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The pass-level arms, which every terminating path out of a pass
    /// accounts for. Inventoried from the exits of <c>RunPassAsync</c> rather
    /// than counted from a pattern: occurrence-counting reported four exits
    /// where the source has eight, missing both <c>continue</c> statements.
    /// </summary>
    /// <remarks>
    /// <c>registry_timed_out</c> was absent from the layer as first shipped,
    /// and this list is not the thing that will catch the next one like it -
    /// see <c>Every_terminating_path_out_of_a_pass_carries_a_reach_arm</c>,
    /// which reads the source rather than this array.
    /// </remarks>
    private static readonly string[] EveryPassExitArm =
    [
        "registry_cancelled",
        "registry_failed",
        "registry_timed_out",
        "loop_cancelled",
        "no_due_tree",
        "pass_completed_immediate",
        "pass_completed_scheduled",
    ];

    private static double SumOfStage(InstrumentRecorder reach, string stage) =>
        reach.Measurements
            .Where(m => (m.Tag(LatticeMetrics.TagStage) as string) == stage)
            .Sum(m => m.Value);

    private static InstrumentRecorder PassLevelReach() =>
        new(LatticeMetrics.WalGcPassReach, LatticeMetrics.TreeNone);

    // ------------------------------------------------------- pass-level arms

    [Test]
    public async Task A_pass_that_completes_records_entry_and_exactly_one_exit_arm()
    {
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        using var reach = PassLevelReach();
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-reach-complete"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var entered = SumOfStage(reach, "pass_entered");
        var exits = EveryPassExitArm.Sum(arm => SumOfStage(reach, arm));

        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.GreaterThanOrEqualTo(1),
                "a pass that ran must record pass_entered, which is the denominator every other "
                    + "pass-level arm is read against.");
            Assert.That(exits, Is.EqualTo(entered),
                "at quiescence every pass that entered must have recorded exactly one exit arm.");
        });
    }

    [Test]
    public async Task A_registry_that_throws_records_the_exit_taken_above_the_collect_loop()
    {
        // The arm the whole retrofit exists for. This exit is above the collect
        // loop, so wal.gc.passes and wal.gc.interval record nothing at all -
        // and before this layer that silence was indistinguishable from a
        // healthy silo with nothing to collect.
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        var factory = FactoryFor(() => throw new InvalidOperationException("registry is wedged"));

        using var reach = PassLevelReach();
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SumOfStage(reach, "registry_failed"), Is.GreaterThanOrEqualTo(1),
                "a pass that could not enumerate the registry must say so, because every per-tree "
                    + "instrument below it is silent and silence is what this layer exists to qualify.");
            Assert.That(SumOfStage(reach, "pass_entered"), Is.GreaterThanOrEqualTo(1),
                "the entry arm is taken above the registry call, so it must survive the registry failing.");
            Assert.That(SumOfStage(reach, "pass_completed_scheduled"), Is.Zero,
                "a pass that died in the registry must not also report completing.");
        });
    }

    [Test]
    public async Task A_registry_with_no_tree_records_the_no_due_tree_exit()
    {
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        using var reach = PassLevelReach();
        var scheduler = CreateScheduler(FactoryWithTrees(), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(SumOfStage(reach, "no_due_tree"), Is.GreaterThanOrEqualTo(1),
            "an empty registry is a distinct exit from a wedged one, and conflating them would report "
                + "a silo that has nothing to do as a silo that is broken.");
    }

    [Test]
    public async Task The_reach_layer_advances_across_passes_where_a_zero_prime_would_not()
    {
        // The arm that discriminates this design from the one it generalises.
        // A zero-priming layer would emit a measurement on each of these passes
        // too, so a fixture counting measurements would pass under both. Only
        // the SUM separates them: three advancing passes sum to three, three
        // primed passes sum to zero.
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        using var reach = PassLevelReach();
        var scheduler = CreateScheduler(FactoryWithTrees("walgc-reach-advances"), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.That(SumOfStage(reach, "pass_entered"), Is.EqualTo(3),
            "three passes must sum to three. A zero-primed layer emits three measurements here too and "
                + "sums to zero, which is byte-identical to one pass and to none - that is precisely the "
                + "reading a non-advancing layer cannot provide.");
    }

    [Test]
    public async Task The_completeness_relation_balances_at_quiescence()
    {
        // The guard against an uncovered sibling exit. It fails when the
        // POPULATION of exits changes rather than when a known member changes,
        // which is what occurrence-counting cannot do: counting returned a
        // clean four where the source has eight.
        //
        // It guards only the exits it actually DRIVES, which is why this arm
        // runs every drivable pass shape under one recorder rather than the
        // single healthy one. Established by perturbation: an earlier draft
        // drove only a collectable tree, and deleting the no_due_tree arm left
        // this assertion green - a guard against uncovered exits that was
        // itself blind to the uncovered exit. The three shapes below are the
        // three a fixture can reach; the two cancellation arms are only
        // reachable during shutdown and are driven separately.
        //
        // Asserted only at quiescence, and that restriction is load-bearing.
        // pass_entered is taken before the work and an exit arm after it, so a
        // pass in flight is legitimately in the first and not yet in the
        // second. The true relation is
        //     0 <= entered - sum(exits) <= passes in flight
        // which is one here. Asserted as an equality while the scheduler is
        // running - or promoted to a production alert in that form - it would
        // flap once per pass forever and be muted, at which point the guard is
        // gone and nothing says so.
        using var reach = PassLevelReach();

        // Shape 1: a healthy tree, collected, pass scheduled onward.
        await RunToQuiescenceAsync(FactoryWithTrees("walgc-reach-balance"), extraTicks: 1);

        // Shape 2: an empty registry - nothing due, exit above the collection.
        await RunToQuiescenceAsync(FactoryWithTrees(), extraTicks: 0);

        // Shape 3: a registry that throws - exit above the loop entirely.
        await RunToQuiescenceAsync(
            FactoryFor(() => throw new InvalidOperationException("registry is wedged")),
            extraTicks: 0);

        var entered = SumOfStage(reach, "pass_entered");
        var exits = EveryPassExitArm.Sum(arm => SumOfStage(reach, arm));

        Assert.Multiple(() =>
        {
            Assert.That(entered, Is.GreaterThanOrEqualTo(4),
                "the relation is only meaningful over passes that actually ran, across every shape.");
            Assert.That(exits, Is.EqualTo(entered),
                "every terminating path out of a pass carries exactly one exit arm, so at quiescence the "
                    + "exit arms must account for every entry. An early return added without an arm "
                    + "unbalances this sum, which is the point of asserting it.");
        });
    }

    [Test]
    public async Task A_pass_cancelled_between_trees_records_the_loop_cancelled_exit()
    {
        // The only exit a fixture can drive that is taken because the silo is
        // shutting down. Without it the cancellation arms would be present in
        // source, absent from every test, and indistinguishable from arms that
        // do not work.
        var blocking = "walgc-reach-cancel-blocking";
        var trailing = "walgc-reach-cancel-trailing";
        var time = new VirtualTimeProvider();

        var inCollection = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<LatticeWalGcReport>(TaskCreationOptions.RunContinuationsAsynchronously);

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(blocking, Arg.Any<CancellationToken>()).Returns(_ =>
        {
            inCollection.TrySetResult();
            return release.Task;
        });
        gc.RunOnceAsync(trailing, Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        using var reach = PassLevelReach();
        var scheduler = CreateScheduler(FactoryWithTrees(blocking, trailing), gc, Adaptive(), time);

        await StartArmedAsync(scheduler, time);
        time.Advance(time.LastScheduledDelay);
        await Parked(inCollection.Task);

        // Cancel while the pass is between its entry arm and its exit arm, then
        // let the in-flight collection finish so the loop reaches the trailing
        // tree and finds the token cancelled.
        var stopping = scheduler.StopAsync(CancellationToken.None);
        release.SetResult(Report(0));
        await Parked(stopping);

        Assert.Multiple(() =>
        {
            Assert.That(SumOfStage(reach, "loop_cancelled"), Is.GreaterThanOrEqualTo(1),
                "a pass abandoned mid-loop must account for itself, or the trees it never reached would "
                    + "be indistinguishable from trees the scheduler chose not to collect.");
            Assert.That(
                EveryPassExitArm.Sum(arm => SumOfStage(reach, arm)),
                Is.EqualTo(SumOfStage(reach, "pass_entered")),
                "the relation must hold across a cancelled pass too: quiescence is reached here because "
                    + "StopAsync awaits the execute task, so the cancelled pass has recorded its exit.");
        });
    }

    /// <summary>
    /// Starts a scheduler, drives it through its first pass plus
    /// <paramref name="extraTicks"/> more, and stops it - establishing the
    /// quiescence the completeness relation is asserted at.
    /// </summary>
    private static async Task RunToQuiescenceAsync(IGrainFactory factory, int extraTicks)
    {
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < extraTicks; i++)
        {
            await TickAsync(time);
        }

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task Pass_level_arms_never_carry_a_real_tree_and_per_tree_arms_never_carry_the_sentinel()
    {
        // The sentinel is structurally required rather than tidy: two of the
        // exits are the catch arms of the registry enumeration itself, where
        // obtaining the tree list is the operation that failed, so no tree id
        // exists to label them with and none ever can.
        var tree = "walgc-reach-sentinel-domain";
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        using var passLevel = PassLevelReach();
        using var perTree = new InstrumentRecorder(LatticeMetrics.WalGcTreeReach, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        var stagesAtSentinel = passLevel.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagStage) as string)
            .Distinct()
            .ToArray();
        var stagesAtTree = perTree.Measurements
            .Select(m => m.Tag(LatticeMetrics.TagStage) as string)
            .Distinct()
            .ToArray();

        Assert.Multiple(() =>
        {
            // Both sets must be non-empty before the domain claims below mean
            // anything. Without this the assertions hold vacuously on an empty
            // recorder: an absent instrument satisfies "is a subset of" and
            // "has no member" perfectly, so the arm would stay green with the
            // whole layer deleted. Established by perturbation, not by review.
            Assert.That(stagesAtTree, Is.Not.Empty,
                "a per-tree series must actually have been recorded, or every domain claim below is a "
                    + "statement about the empty set.");
            Assert.That(stagesAtSentinel, Is.Not.Empty,
                "likewise for the reserved series.");
            Assert.That(stagesAtTree, Is.SubsetOf(new[] { "tree_seen", "tree_collected" }),
                "a per-tree series must carry only per-tree stages; a pass-level stage filed under a real "
                    + "tree would attribute a silo-wide event to one tree.");
            Assert.That(stagesAtSentinel, Has.No.Member("tree_seen").And.No.Member("tree_collected"),
                "a per-tree stage under the reserved tree would be uncountable - it names no tree and "
                    + "would be double-counted by any aggregate that forgot the filter.");
            Assert.That(LatticeMetrics.TreeNone, Does.StartWith("_").And.EndWith("_"),
                "the sentinel takes the underscore-delimited reserved form because a tree id is a "
                    + "caller-supplied string and a tree could legitimately be named 'none'.");
        });
    }

    // -------------------------------------------------------- per-tree arms

    [Test]
    public async Task A_collected_tree_records_tree_collected_even_when_its_collection_throws()
    {
        // Proves the arm survives a failing collection. It does NOT prove the
        // arm sits above the two cancellation-guarded early exits of
        // CollectTreeAsync, and that gap was established by perturbation rather
        // than assumed away: moving the call site down to the tail, beside the
        // wal.gc.interval record, reddens none of the arms in this fixture.
        // Those two exits are reachable only while the token is cancelled -
        // that is, during shutdown, after the loop-cancelled arm has already
        // accounted for the pass - so no fixture here can drive them. The
        // placement is correct and is defended by the comment at the call site,
        // not by this test; saying so is cheaper than a green arm that reads
        // like a proof and is not one.
        var tree = "walgc-reach-throwing-tree";
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<Task<LatticeWalGcReport>>(_ => throw new InvalidOperationException("tree is wedged"));

        using var reach = new InstrumentRecorder(LatticeMetrics.WalGcTreeReach, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SumOfStage(reach, "tree_collected"), Is.GreaterThanOrEqualTo(1),
                "the collection was entered, so the arm that licenses reading a flat interval series as "
                    + "measured must advance even though the collection then failed.");
            Assert.That(SumOfStage(reach, "tree_seen"), Is.GreaterThanOrEqualTo(1),
                "a tree the pass enumerated is seen regardless of what its collection did.");
        });
    }

    [Test]
    public async Task A_tree_that_is_not_yet_due_is_seen_but_not_collected()
    {
        // The exit the issue's inventory omitted, and the one that matters
        // most: on any given pass the majority of registered trees are not yet
        // due, so this is the healthy steady state. A layer that rendered it as
        // a non-reach would report a healthy estate as a stopped one.
        var busy = "walgc-reach-busy";
        var quiet = "walgc-reach-quiet";
        var time = new VirtualTimeProvider();

        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(busy, Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(5)));
        gc.RunOnceAsync(quiet, Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        using var quietReach = new InstrumentRecorder(LatticeMetrics.WalGcTreeReach, quiet);
        var scheduler = CreateScheduler(FactoryWithTrees(busy, quiet), gc, Adaptive(), time);

        // Pass 1 collects both. The busy tree holds the floor; the quiet one
        // relaxes past it, so pass 2 is scheduled on the busy tree's due time
        // and the quiet tree is skipped by the not-yet-due continue.
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        var seen = SumOfStage(quietReach, "tree_seen");
        var collected = SumOfStage(quietReach, "tree_collected");

        Assert.Multiple(() =>
        {
            Assert.That(seen, Is.GreaterThan(collected),
                "the not-yet-due tree must still be seen, or the skip would be indistinguishable from the "
                    + "tree having been de-registered.");
            Assert.That(collected, Is.GreaterThanOrEqualTo(1),
                "it was collected on the pass it first appeared, so the pair is a genuine difference "
                    + "rather than one half never having fired.");
        });
    }

    [Test]
    public async Task The_seen_and_collected_pair_advances_per_pass_rather_than_latching()
    {
        // The per-tree arms must not inherit the latch that makes
        // PrimeRetentionSeries fire once per tree per process. A latched arm
        // would read identically on a scheduler that stopped after its first
        // pass, which is the failure mode this instrument is the remedy for.
        var tree = "walgc-reach-no-latch";
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(5)));

        using var reach = new InstrumentRecorder(LatticeMetrics.WalGcTreeReach, tree);
        var scheduler = CreateScheduler(FactoryWithTrees(tree), gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await TickAsync(time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SumOfStage(reach, "tree_seen"), Is.EqualTo(3),
                "a reclaiming tree is enumerated on every pass, so three passes must sum to three.");
            Assert.That(SumOfStage(reach, "tree_collected"), Is.EqualTo(3),
                "a tree held at the floor is due on every pass, so its collection arm must advance with "
                    + "them rather than latching on the first.");
        });
    }

    // --------------------------------------------------- the inserted-exit arm

    [Test]
    public async Task A_registry_that_times_out_records_an_exit_distinct_from_one_that_throws()
    {
        // The arm that was MISSING from this layer as first shipped, and the
        // reason the structural guard below exists.
        //
        // The scheduler catches TimeoutException ahead of the general fault
        // arm, because a fault is a property of the registry and a timeout is
        // a property of the bound we chose. That catch was inserted between
        // two arms this layer already covered, and an insertion is invisible
        // to every detector that reasons about the arms already present - the
        // balance relation sees only the exits a fixture drives, and no
        // fixture drove this one.
        //
        // The second assertion is the load-bearing half. Asserting only that
        // registry_timed_out advances would still pass if both catch arms
        // recorded it, which is precisely the conflation the split exists to
        // prevent.
        var time = new VirtualTimeProvider();
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult(Report(0)));

        var factory = FactoryFor(() => throw new TimeoutException("registry fan-out did not complete"));

        using var reach = PassLevelReach();
        var scheduler = CreateScheduler(factory, gc, Adaptive(), time);
        await StartAndRunFirstPassAsync(scheduler, time);
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SumOfStage(reach, "registry_timed_out"), Is.GreaterThanOrEqualTo(1),
                "a pass that abandoned the registry on its own budget must say so, because every "
                    + "per-tree instrument below it is silent and silence is what this layer qualifies.");
            Assert.That(SumOfStage(reach, "registry_failed"), Is.Zero,
                "our own bound firing is a decision of ours, not a finding about the registry. An arm "
                    + "that absorbed it into registry_failed would report a timeout we chose as a "
                    + "wedged registry.");
            Assert.That(SumOfStage(reach, "pass_entered"), Is.GreaterThanOrEqualTo(1),
                "the entry arm is taken above the registry call, so it must survive the budget firing.");
        });
    }

    [Test]
    public void Every_terminating_path_out_of_a_pass_carries_a_reach_arm()
    {
        // The guard that would have caught the missing registry_timed_out arm,
        // and the one the balance relation structurally cannot be.
        //
        // The balance relation quantifies over the exits a FIXTURE DRIVES, so
        // an exit no fixture drives contributes neither an entry nor an exit
        // and the relation stays green with the hole open. That is not a
        // weakness of the assertion, it is the scope of it - and it is why the
        // arm list in this file is not the detector either: a list enumerates
        // what someone remembered, and the defect is always the one nobody did.
        //
        // This reads the SOURCE instead. It fails when the POPULATION of
        // terminating paths changes, whether or not any fixture can reach the
        // new one, which is the only predicate that covers an exit inserted
        // between two covered exits.
        //
        // Scope, stated so it is not mistaken for more than it is:
        //   - `return` only. A `continue` terminates a TREE VISIT, not a pass,
        //     and the tree_seen/tree_collected pair accounts for those; the
        //     difference between that pair IS the not-yet-due population.
        //   - Only RunPassCoreAsync. The wrapper takes no exit of its own.
        var source = File.ReadAllLines(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src", "lattice", "LatticeWalGcScheduler.cs"));

        var start = Array.FindIndex(source, l => l.Contains("private async Task<PassDecision> RunPassCoreAsync("));
        Assert.That(start, Is.GreaterThanOrEqualTo(0),
            "RunPassCoreAsync was not found. This guard reads the source by signature, so a rename "
                + "must update it rather than silently disable it.");

        var (bodyStart, bodyEnd) = EnclosingBlock(source, start);

        var uncovered = new List<string>();
        var terminators = 0;
        for (var i = bodyStart; i <= bodyEnd; i++)
        {
            if (!System.Text.RegularExpressions.Regex.IsMatch(source[i], @"^\s*return\b"))
            {
                continue;
            }

            terminators++;
            if (!PathIsAccountedFor(source, i, bodyStart))
            {
                uncovered.Add($"line {i + 1}: {source[i].Trim()}");
            }
        }

        // Vacuity guard. A scan that matched nothing would report a clean
        // result for a file it never read, which is the shape of failure this
        // whole layer exists to make impossible.
        Assert.That(terminators, Is.GreaterThanOrEqualTo(7),
            "the scan found fewer terminating paths than the arms this layer declares, so it is not "
                + "reading the method it claims to read.");

        Assert.That(uncovered, Is.Empty,
            "every terminating path out of a pass must record a reach arm before it returns. An exit "
                + "added without one is a silent hole: the pass ends, the per-tree instruments below "
                + "it stay silent, and nothing distinguishes that silence from a healthy idle silo.");
    }

    /// <summary>
    /// Walks backwards from <paramref name="returnLine"/> through the block
    /// that encloses it, reporting whether a <c>RecordPassReach</c> call is
    /// taken on the same path.
    /// </summary>
    /// <remarks>
    /// Nesting is tracked so that a call inside a sibling block cannot be
    /// credited to this path: walking backwards, <c>}</c> enters a nested
    /// block and <c>{</c> leaves one, and the walk stops when it would leave
    /// the enclosing block entirely.
    /// </remarks>
    private static bool PathIsAccountedFor(string[] source, int returnLine, int bodyStart)
    {
        var depth = 0;
        for (var i = returnLine - 1; i >= bodyStart; i--)
        {
            var line = source[i];
            if (depth == 0 && line.Contains("RecordPassReach("))
            {
                return true;
            }

            depth += line.Count(c => c == '}') - line.Count(c => c == '{');
            if (depth < 0)
            {
                return false;
            }
        }

        return false;
    }

    [Test]
    public void The_entry_arm_is_the_first_statement_of_the_pass_body()
    {
        // A SOURCE guard for a placement no fixture in this harness can
        // defend, and it is written that way deliberately rather than dressed
        // up as a behavioural proof.
        //
        // Relocating pass_entered into RunPassAsync (the wrapper) used to
        // redden five arms here. It now reddens NONE, and the reason is a
        // change nobody made to this layer: the wrapper calls the body exactly
        // once, so entry-in-the-wrapper and entry-in-the-body currently emit
        // an identical series. The hazard did not go away, it went LATENT -
        // the moment the wrapper gains a retry, one entry would be counted
        // against two exits and the completeness relation would run negative,
        // which is the one direction the inequality cannot absorb.
        //
        // A behavioural arm that cannot distinguish the two placements would
        // be a green clause proving nothing. A source assertion can, so the
        // defence moves register rather than being dropped and quietly
        // recorded as an uncoverable gap.
        var source = File.ReadAllLines(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src", "lattice", "LatticeWalGcScheduler.cs"));

        var declaration = Array.FindIndex(
            source, l => l.Contains("private async Task<PassDecision> RunPassCoreAsync("));
        Assert.That(declaration, Is.GreaterThanOrEqualTo(0),
            "RunPassCoreAsync was not found, so this guard is not reading the method it names.");

        var (bodyStart, bodyEnd) = EnclosingBlock(source, declaration);

        var firstStatement = -1;
        for (var i = bodyStart; i <= bodyEnd; i++)
        {
            var line = source[i].Trim();
            if (line.Length == 0 || line.StartsWith("//", StringComparison.Ordinal))
            {
                continue;
            }

            firstStatement = i;
            break;
        }

        Assert.That(firstStatement, Is.GreaterThanOrEqualTo(0),
            "the pass body scanned as empty, so this guard is vacuous rather than satisfied.");

        Assert.That(
            source[firstStatement].Contains("RecordPassReach(LatticeMetrics.ReachPassEntered)"),
            Is.True,
            "the entry arm must be the first statement of the pass body, above every exit it is "
                + "counted against. Taken in the wrapper instead it would survive a retry that the "
                + "exits do not, and one entry against two exits drives the completeness relation "
                + $"negative. First statement was: {source[firstStatement].Trim()}");
    }

    [Test]
    public void The_collected_arm_is_taken_above_the_priming_latch()
    {
        // The gap this fixture carried as a DOCUMENTED HOLE until the entry-arm
        // guard above showed which register could close it.
        //
        // tree_collected must be emitted above PrimeRetentionSeries, because
        // that method latches on _primedTrees and is silent from a tree's
        // second collection onward. Sited below it the arm would still fire -
        // it is a different instrument - but it would sit under a latch that a
        // later refactor could easily widen, and the comment asserting the
        // ordering would be the only thing holding it.
        //
        // No fixture in this harness can tell the two placements apart: both
        // emit once per collection today, so the behavioural arms are green
        // either way. That is why this was reported as an uncovered placement
        // rather than as a passing arm. A source assertion CAN tell them
        // apart, so the ordering is now checked rather than merely asserted in
        // prose next to the line it describes.
        var source = File.ReadAllLines(Path.Combine(
            HygieneRepository.FindRepoRoot(),
            "src", "lattice", "LatticeWalGcScheduler.cs"));

        var collected = Array.FindIndex(
            source, l => l.Contains("LatticeMetrics.ReachTreeCollected"));
        var priming = Array.FindIndex(
            source, l => l.Contains("PrimeRetentionSeries(treeId, treeTag, tenantTag);"));

        Assert.Multiple(() =>
        {
            Assert.That(collected, Is.GreaterThanOrEqualTo(0),
                "the tree_collected emission was not found, so this guard is vacuous.");
            Assert.That(priming, Is.GreaterThanOrEqualTo(0),
                "the PrimeRetentionSeries call was not found, so this guard is vacuous.");
        });

        Assert.That(collected, Is.LessThan(priming),
            "tree_collected must be recorded above PrimeRetentionSeries. That method latches per "
                + "tree per process, so an arm sited below it inherits a guard which is silent from "
                + "the second collection onward - and no behavioural fixture here can see the "
                + $"difference. collected at line {collected + 1}, priming at line {priming + 1}.");
    }

    /// <summary>
    /// Returns the inclusive line range of the block opened at or after
    /// <paramref name="declaration"/>.
    /// </summary>
    private static (int Start, int End) EnclosingBlock(string[] source, int declaration)
    {
        var depth = 0;
        var opened = false;
        var start = declaration;
        for (var i = declaration; i < source.Length; i++)
        {
            var opens = source[i].Count(c => c == '{');
            var closes = source[i].Count(c => c == '}');
            if (!opened && opens > 0)
            {
                opened = true;
                start = i + 1;
            }

            depth += opens - closes;
            if (opened && depth <= 0)
            {
                return (start, i);
            }
        }

        return (start, source.Length - 1);
    }
}
