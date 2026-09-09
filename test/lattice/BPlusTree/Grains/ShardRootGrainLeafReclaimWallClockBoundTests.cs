using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue 2131: the empty-leaf reclaim walk was bounded by probe count alone,
/// and a probe count cannot bound the thing that actually fails.
/// <para>
/// The cost of a probe is not a constant. A probe activates a leaf and counts
/// its rows, so warm and co-located it is sub-millisecond, and cold against
/// remote storage it is a state read. The same 1024-probe budget is therefore
/// a sub-second pass in one deployment and a pass that outruns the caller's
/// Orleans response timeout in another, and no value of the count is correct
/// for both. Past the timeout the failure is a cliff rather than a gradient:
/// the pass does not reclaim less, it fails outright and reclaims nothing, on
/// precisely the large cold trees reclaim exists to tidy. A pass observed in
/// the field ran 27.15s, past the 25s page-fill ceiling.
/// </para>
/// <para>
/// <b>Every test here fixes the probe budget generously and varies only the
/// wall clock</b>, so none of them can be satisfied by the count bound. That is
/// the point of the item rather than an incidental property of the fixture: a
/// predicate on a count is arithmetically incapable of expressing "this has
/// taken too long", at any threshold.
/// </para>
/// <para>
/// <b>The control is
/// <see cref="A_disabled_deadline_leaves_the_walk_running_to_the_end_of_the_chain"/></b>,
/// and it is what makes the other assertions mean anything. It runs the
/// identical harness with the deadline switched off and walks the chain to its
/// tail, so a later "stopped early" result is evidence about the clock and not
/// about a fixture that never walks far. Without that arm a broken harness
/// would pass everything else in this file.
/// </para>
/// <para>
/// <b>Timing dependence is one-directional in every test.</b> Each probe pays a
/// real delay, and the tests are split so that no single assertion needs the
/// clock to land inside a window. The stand-down tests use a deadline shorter
/// than one probe, so they are decided before the walk loop is entered at all
/// and a slower machine only makes that more certain. The fold test uses a
/// deadline with well over an order of magnitude of headroom above the handful
/// of probes a fold costs, and asserts against the whole chain length rather
/// than against a particular probe count.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainLeafReclaimWallClockBoundTests
{
    private const string TreeId = "reclaim-clock-tree";
    private const string ShardKey = TreeId + "/0";

    /// <summary>
    /// Long enough that walking it whole is unmistakable next to a pass that
    /// stands down, short enough that the control arm stays under two seconds.
    /// </summary>
    private const int ChainLength = 120;

    /// <summary>
    /// The one foldable leaf, at the head end so that a pass with any real
    /// budget reaches it immediately. Its position is what lets the fold test
    /// distinguish "stopped early" from "stopped before doing anything".
    /// </summary>
    private const int CandidateIndex = 1;

    /// <summary>
    /// Stands in for the state read a cold leaf activation pays. It is what
    /// makes elapsed time rather than probe count the binding quantity, which
    /// is the deployment difference this item is about. The real figure is
    /// larger than the nominal one on Windows, whose default timer granularity
    /// is around 15ms; every test here is written so that a longer than nominal
    /// delay strengthens its assertion rather than weakening it.
    /// </summary>
    private static readonly TimeSpan ProbeDelay = TimeSpan.FromMilliseconds(15);

    /// <summary>
    /// Shorter than a single probe, so a pass configured with it has already
    /// overrun by the time it reaches its walk loop. This makes the stand-down
    /// tests deterministic rather than a race: they do not depend on WHERE the
    /// deadline lands, only on the fact that it has already passed.
    /// </summary>
    private static readonly TimeSpan ExpiredBeforeFirstProbe = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// Room for the handful of probes a single fold costs, with well over an
    /// order of magnitude of headroom, while remaining a small fraction of the
    /// <see cref="ChainLength"/> probes a full walk costs.
    /// </summary>
    private static readonly TimeSpan RoomForAFoldButNotTheChain = TimeSpan.FromMilliseconds(500);

    private static string LeafKey(int index) => $"k{index:D4}";

    private sealed class ChainHarness
    {
        public ShardRootGrain Grain { get; set; } = null!;
        public required GrainId[] LeafIds { get; init; }
        public required Dictionary<GrainId, LeafReclaimProbe> Probes { get; init; }
        public required List<GrainId> ProbeOrder { get; init; }
        public required List<GrainId> ChildIds { get; init; }
        public required List<string?> Separators { get; init; }
        public required RecordingLoggerFactory Logs { get; init; }

        public int ProbeCount => ProbeOrder.Count;

        /// <summary>
        /// The single pass-summary line, which must be present after every
        /// pass. Asserting on exactly one entry is deliberate: it pins that a
        /// pass reports once, so neither the gate this change removed nor a
        /// duplicate line can creep back unnoticed.
        /// </summary>
        public RecordedLogEntry PassSummary =>
            Logs.Entries.Single(e => e.Message.Contains(
                "finished an empty-leaf reclaim pass", StringComparison.Ordinal));

        public RoutingTableSnapshot Snapshot() => new()
        {
            SeparatorKeys = [.. Separators],
            ChildIds = [.. ChildIds],
            ChildrenAreLeaves = true,
        };

        public bool RemoveChild(GrainId childId)
        {
            var index = ChildIds.IndexOf(childId);
            if (index < 0) return false;
            ChildIds.RemoveAt(index);
            Separators.RemoveAt(index);
            return true;
        }
    }

    /// <summary>
    /// A single internal root over <see cref="ChainLength"/> leaves that tile
    /// the keyspace, all holding live rows except the one candidate.
    /// <para>
    /// The substitutes are a live model rather than fixed returns. An unlink, a
    /// range widen, a back-pointer repair and a child removal all mutate the
    /// modelled chain, so a walk that folds a leaf really does go on to see a
    /// shortened chain, and a resumed walk really does route through the
    /// post-fold routing table. Against frozen stubs the walk would re-probe
    /// and re-fold the same leaf until its budget ran out, and every assertion
    /// here would be measuring the stub instead of the grain.
    /// </para>
    /// </summary>
    private static ChainHarness CreateHarness(
        TimeSpan backgroundDrainMaxDuration,
        int? candidateIndex = CandidateIndex)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var rootId = GrainId.Create("internal", "clock-root");
        var leafIds = new GrainId[ChainLength];
        for (var i = 0; i < ChainLength; i++)
            leafIds[i] = GrainId.Create("leaf", $"clock-leaf-{i:D4}");

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;

        var probes = new Dictionary<GrainId, LeafReclaimProbe>();
        var separators = new List<string?>();
        for (var i = 0; i < ChainLength; i++)
        {
            probes[leafIds[i]] = new LeafReclaimProbe
            {
                LiveRowCount = i == candidateIndex ? 0 : 1,
                PrevSibling = i == 0 ? null : leafIds[i - 1],
                NextSibling = i == ChainLength - 1 ? null : leafIds[i + 1],
                LowKeyInclusive = i == 0 ? null : LeafKey(i),
                HighKeyExclusive = i == ChainLength - 1 ? null : LeafKey(i + 1),
            };
            separators.Add(i == 0 ? null : LeafKey(i));
        }

        var harness = new ChainHarness
        {
            LeafIds = leafIds,
            Probes = probes,
            ProbeOrder = [],
            ChildIds = [.. leafIds],
            Separators = separators,
            Logs = new RecordingLoggerFactory(),
        };

        var factory = Substitute.For<IGrainFactory>();

        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(_ => Task.FromResult(harness.Snapshot()));
        root.GetChildIdsAsync().Returns(_ => Task.FromResult(new List<GrainId>(harness.ChildIds)));
        root.RemoveChildAsync(Arg.Any<GrainId>())
            .Returns(ci => Task.FromResult(harness.RemoveChild(ci.Arg<GrainId>())));
        factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(root);

        var leaves = new Dictionary<GrainId, IBPlusLeafGrain>();
        foreach (var id in leafIds)
        {
            var self = id;
            var leaf = Substitute.For<IBPlusLeafGrain>();

            leaf.GetReclaimProbeAsync().Returns(async _ =>
            {
                harness.ProbeOrder.Add(self);

                // The cold-activation cost, paid on the probe itself so that
                // elapsed time tracks work actually done rather than being
                // simulated alongside it.
                await Task.Delay(ProbeDelay);
                return probes[self];
            });

            leaf.TryBeginRetirementAsync().Returns(Task.FromResult(true));
            leaf.AbandonRetirementAsync().Returns(Task.CompletedTask);
            leaf.ClearGrainStateAsync().Returns(Task.CompletedTask);

            leaf.SetPrevSiblingAsync(Arg.Any<GrainId?>()).Returns(ci =>
            {
                probes[self] = probes[self] with { PrevSibling = ci.Arg<GrainId?>() };
                return Task.CompletedTask;
            });

            leaf.AbsorbSuccessorRangeAsync(Arg.Any<string?>()).Returns(ci =>
            {
                probes[self] = probes[self] with { HighKeyExclusive = ci.Arg<string?>() };
                return Task.CompletedTask;
            });

            leaf.TryUnlinkSuccessorAsync(Arg.Any<GrainId>(), Arg.Any<GrainId?>(), Arg.Any<string?>())
                .Returns(ci =>
                {
                    probes[self] = probes[self] with
                    {
                        NextSibling = ci.ArgAt<GrainId?>(1),
                        HighKeyExclusive = ci.ArgAt<string?>(2),
                    };
                    return Task.FromResult(true);
                });

            leaves[id] = leaf;
        }

        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>())
            .Returns(ci => leaves[ci.Arg<GrainId>()]);

        harness.Grain = new ShardRootGrain(
            context, state, factory,
            TestOptionsResolver.Create(
                baseOptions: new LatticeOptions
                {
                    BackgroundDrainMaxDuration = backgroundDrainMaxDuration,
                },
                factory: factory),
            new Logger<ShardRootGrain>(harness.Logs),
            TestMutationObservers.NoObservers());

        return harness;
    }
    /// <summary>
    /// The control, and the falsifier for every other test in this fixture.
    /// With the deadline switched off the identical harness walks the chain to
    /// its tail under the probe budget alone, so a later "stopped early" result
    /// is evidence about the wall clock rather than about the fixture.
    /// <para>
    /// It also pins the degradation direction. A non-positive duration must
    /// restore the historical walk that is unbounded in time, never silently
    /// truncate one: an operator who switches the shared net off gets exactly
    /// the behaviour that shipped before this bound existed, which is the only
    /// safe way for a misconfiguration to fail.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_disabled_deadline_leaves_the_walk_running_to_the_end_of_the_chain()
    {
        var h = CreateHarness(TimeSpan.Zero);

        // 8 folds x 16 probes per fold = a 128-probe budget over a 120-leaf
        // chain, so the probe bound cannot be what stops this walk either.
        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.EqualTo(1),
                "the harness must contain exactly one foldable leaf and must fold it");
            Assert.That(h.ProbeOrder, Does.Contain(h.LeafIds[ChainLength - 1]),
                "with no deadline the walk must reach the tail of the chain");
        });
    }

    /// <summary>
    /// The regression itself. The probe budget is left generous and only the
    /// wall clock binds, which is the case a count-based bound is structurally
    /// unable to express. Before this item the walk had no clock at all and ran
    /// the chain to its tail regardless of how long that took.
    /// </summary>
    [Test]
    public async Task A_pass_that_has_spent_its_wall_clock_budget_stops_before_the_end_of_the_chain()
    {
        var h = CreateHarness(ExpiredBeforeFirstProbe);

        await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(h.ProbeOrder, Does.Not.Contain(h.LeafIds[ChainLength - 1]),
                "a pass past its deadline must stand down rather than walk on to the tail");
            Assert.That(h.ProbeCount, Is.LessThan(ChainLength),
                "the walk must stop on elapsed time, not only on probe count");
        });
    }

    /// <summary>
    /// A pass that overruns before it can do any useful work must leave the
    /// tree exactly as it found it. Reclaim's safety argument is that it never
    /// trades a slow pass for a damaged chain, so a new stop condition has to
    /// be inert rather than merely early.
    /// <para>
    /// This is also the honest statement of the degenerate configuration: a
    /// deadline shorter than a single probe buys no folds at all. That is not a
    /// failure mode to hide, because the pass still advances its cursor (see
    /// the resumption test), so even here the walk makes forward progress
    /// rather than spinning on the same prefix.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_pass_that_stands_down_immediately_leaves_the_chain_untouched()
    {
        var h = CreateHarness(ExpiredBeforeFirstProbe);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.Zero);
            Assert.That(h.ChildIds, Has.Count.EqualTo(ChainLength),
                "no leaf may be unrouted by a pass that did no work");
            Assert.That(h.Probes[h.LeafIds[0]].NextSibling, Is.EqualTo(h.LeafIds[1]),
                "the chain must be left exactly as it was found");
        });
    }

    /// <summary>
    /// The useful middle of the range, and the case an operator actually runs:
    /// a deadline generous enough to do real work but not to walk a long chain.
    /// The pass must fold what it reached and then stop, rather than treating
    /// the deadline as a reason to abandon work already in progress.
    /// </summary>
    [Test]
    public async Task A_pass_bounded_mid_walk_completes_its_fold_and_then_stops()
    {
        var h = CreateHarness(RoomForAFoldButNotTheChain);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.EqualTo(1),
                "a deadline must not abandon a fold the pass had budget to make");
            Assert.That(h.ChildIds, Does.Not.Contain(h.LeafIds[CandidateIndex]),
                "the folded leaf must be unrouted from its parent");
            Assert.That(h.Probes[h.LeafIds[CandidateIndex - 1]].NextSibling,
                Is.EqualTo(h.LeafIds[CandidateIndex + 1]),
                "the predecessor must point past the folded leaf");
            Assert.That(h.Probes[h.LeafIds[CandidateIndex - 1]].HighKeyExclusive,
                Is.EqualTo(LeafKey(CandidateIndex + 1)),
                "the predecessor must have absorbed the folded leaf's range");
            Assert.That(h.ProbeCount, Is.LessThan(ChainLength),
                "having folded, the pass must still stop on the deadline");
        });
    }

    /// <summary>
    /// The acceptance criterion the resume cursor exists for. A pass stopped by
    /// the deadline must record where it stopped so the next pass continues
    /// from there. Without it a bounded walk would re-walk the same prefix
    /// forever and never reach the tail of a chain longer than one pass.
    /// <para>
    /// This is asserted at the least forgiving setting, where the deadline has
    /// expired before the walk loop is entered, because that is the only
    /// configuration in which a pass could plausibly record nothing at all.
    /// Progress here is progress everywhere.
    /// </para>
    /// </summary>
    [Test]
    public async Task Successive_passes_resume_rather_than_re_walking_the_same_prefix()
    {
        var h = CreateHarness(ExpiredBeforeFirstProbe);

        await h.Grain.ReclaimEmptyLeavesAsync(8);
        var firstPass = h.ProbeOrder.ToArray();

        h.ProbeOrder.Clear();
        await h.Grain.ReclaimEmptyLeavesAsync(8);
        var secondPass = h.ProbeOrder.ToArray();

        h.ProbeOrder.Clear();
        await h.Grain.ReclaimEmptyLeavesAsync(8);
        var thirdPass = h.ProbeOrder.ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(firstPass, Is.Not.Empty);
            Assert.That(firstPass[0], Is.EqualTo(h.LeafIds[0]),
                "the first pass starts at the head, having no cursor to resume from");
            Assert.That(secondPass, Is.Not.Empty);
            Assert.That(secondPass[0], Is.EqualTo(h.LeafIds[1]),
                "a pass stopped by the deadline must record where it stopped");
            Assert.That(thirdPass, Is.Not.Empty);
            Assert.That(thirdPass[0], Is.EqualTo(h.LeafIds[2]),
                "resumption must keep advancing rather than settling on one leaf");
        });
    }

    /// <summary>
    /// The two bounds are independent, and adding the deadline must not have
    /// disarmed the one that was already there. This is the specific hazard
    /// issue 1992 found in this very abstraction, where a gate on
    /// <c>resultsCollected &gt; 0</c> disarmed BOTH bounds on a sterile run, so
    /// it is worth pinning rather than assuming.
    /// </summary>
    [Test]
    public async Task A_generous_deadline_leaves_the_probe_budget_binding_as_before()
    {
        var h = CreateHarness(TimeSpan.FromHours(1));

        // 1 fold x 16 probes per fold = a 16-probe budget, far short of the
        // 120-leaf chain, so the count bound is the only thing that can stop
        // this walk.
        await h.Grain.ReclaimEmptyLeavesAsync(1);

        Assert.Multiple(() =>
        {
            Assert.That(h.ProbeOrder, Does.Not.Contain(h.LeafIds[ChainLength - 1]),
                "the probe budget must still stop a walk short of the tail");
            Assert.That(h.ProbeCount, Is.LessThan(ChainLength));
        });
    }

    /// <summary>
    /// The zero-yield reporting regression, found by the worker on issue 2278
    /// and fixed here because this method was already being changed.
    /// <para>
    /// The pass summary used to be gated on <c>reclaimed &gt; 0</c>, which
    /// silenced the most expensive pass there is. A fruitless pass does not
    /// stop early - the early break fires only when the fold budget is met - so
    /// it probes its entire budget and returns without a trace, while a pass
    /// that folded immediately and stopped logs a line. The instrument was
    /// anti-correlated with cost, which makes any count of reclaim lines in a
    /// log a floor on the passes that ran rather than a census of them, and
    /// understates long passes specifically.
    /// </para>
    /// <para>
    /// This is also what makes the wall-clock bound validatable at all. A limit
    /// nobody can observe firing is a limit nobody can confirm works.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_pass_that_folds_nothing_still_reports_itself()
    {
        // No candidate anywhere in the chain: every leaf holds a live row, so
        // the pass probes its whole budget and folds nothing.
        var h = CreateHarness(TimeSpan.Zero, candidateIndex: null);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.Zero, "the harness must offer nothing to fold");
            Assert.That(h.PassSummary.Int64("Reclaimed"), Is.Zero);
            Assert.That(h.PassSummary.Int64("Visited"), Is.GreaterThan(0),
                "the expensive case is precisely the one that probed and folded nothing");
        });
    }

    /// <summary>
    /// The three quantities an operator needs in order to size the bound,
    /// reported together on one line so that a field measurement can be read
    /// directly rather than reconstructed by correlating separate events.
    /// </summary>
    [Test]
    public async Task A_pass_reports_its_elapsed_time_probe_count_and_fold_count()
    {
        var h = CreateHarness(TimeSpan.Zero);

        await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(h.PassSummary.Level, Is.EqualTo(LogLevel.Information));
            Assert.That(h.PassSummary.Int64("Reclaimed"), Is.EqualTo(1));
            Assert.That(h.PassSummary.Int64("ElapsedMs"), Is.GreaterThan(0),
                "a pass that probed a long chain at a real per-probe cost took measurable time");
            Assert.That(h.PassSummary.Int64("ProbeBudget"), Is.EqualTo(128),
                "8 folds x 16 probes per fold");
            Assert.That(h.PassSummary.Int64("Visited"), Is.GreaterThan(0));
        });
    }

    /// <summary>
    /// The two bounds must be distinguishable in the log, because they call for
    /// opposite operator responses: a probe-budget stop says the batch size is
    /// too small for the tree, and a deadline stop says the shard is slow
    /// enough that no batch size would help. Collapsing them into one "budget"
    /// reason would leave the log unable to report the case this item exists
    /// for.
    /// </summary>
    [Test]
    public async Task The_reported_stop_reason_distinguishes_the_deadline_from_the_probe_budget()
    {
        var onTheClock = CreateHarness(ExpiredBeforeFirstProbe);
        await onTheClock.Grain.ReclaimEmptyLeavesAsync(8);

        var onProbes = CreateHarness(TimeSpan.FromHours(1), candidateIndex: null);
        await onProbes.Grain.ReclaimEmptyLeavesAsync(1);

        var onFolds = CreateHarness(TimeSpan.FromHours(1));
        await onFolds.Grain.ReclaimEmptyLeavesAsync(1);

        var toTheTail = CreateHarness(TimeSpan.Zero);
        await toTheTail.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(onTheClock.PassSummary.Value("StopReason"), Is.EqualTo("deadline"));
            Assert.That(onProbes.PassSummary.Value("StopReason"), Is.EqualTo("probe-budget"));
            Assert.That(onFolds.PassSummary.Value("StopReason"), Is.EqualTo("fold-budget"));
            Assert.That(toTheTail.PassSummary.Value("StopReason"), Is.EqualTo("end-of-chain"));
        });
    }
}
