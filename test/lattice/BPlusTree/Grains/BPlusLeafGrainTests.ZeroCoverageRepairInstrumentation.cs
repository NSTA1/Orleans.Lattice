using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text.RegularExpressions;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Instrumentation half of the zero-coverage repair (issue #2940). The sibling
/// file asserts that the repair DOES the right thing; this one asserts that it
/// SAYS what it did, on every path it can take.
/// <para>
/// <b>The defect being closed.</b> Before this,
/// <c>RecordCoverageRepairOutcome</c> had exactly two call sites, both BELOW the
/// entry guard, so the method had two exits that recorded nothing: the guard
/// rejecting on entry, and a repair that ran and left the partition uncovered.
/// Those two have OPPOSITE remedies - the first points at the pin/guard seam,
/// the second at the capture seam - and in production they were byte-identical
/// silence. On the two trees blocking epic #2368 the entire leaf-side seam
/// emitted nothing at all: six distinct log templates mentioned the tree and all
/// six came from the WAL GC scheduler.
/// </para>
/// <para>
/// <b>Method rules these fixtures are written to.</b> An absence is evidence
/// only if the detector is independently known to work, so every fixture below
/// asserts a NON-ZERO input count (evaluations actually reached the instrument)
/// before it reports any zero, and every fixture that asserts a zero carries a
/// POSITIVE CONTROL in the same run - a different arm observed at a non-zero
/// value through the same listener. A fixture that could only ever go green is
/// the very defect class this issue is about, so it is not built here.
/// </para>
/// <para>
/// <b>Why every fixture uses a unique tree id.</b> The zero-prime is keyed by
/// tree in a process-wide dictionary, deliberately, so that a tree is primed
/// once per process rather than once per activation. A shared id would let
/// whichever fixture ran first consume the prime and leave the rest asserting
/// against an already-primed tree - an order-dependent green, which is worse
/// than a red. <see cref="UniqueCoverageRepairTreeId"/> mints one per fixture.
/// </para>
/// <para>
/// <b>Activation is driven through <c>LeafActivationHarness</c>.</b> Since
/// #2909 the activation hook ARMS the WAL replay rather than running it, so
/// awaiting the hook means the replay started, not finished. A fixture asserting
/// an absence off a bare hook await would pass because the work had not run yet,
/// which is again the defect class under repair.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Mints a tree id no other fixture in this process has used, so the
    /// process-wide zero-prime is observable from inside the fixture that
    /// triggers it.
    /// </summary>
    private static string UniqueCoverageRepairTreeId(string discriminator)
        => $"tree-zero-coverage-repair-{discriminator}-{Guid.NewGuid():N}";

    /// <summary>
    /// The seven tag values <c>TryRepairZeroCoverageAsync</c> can record - the six
    /// terminal arms that partition every invocation, plus <c>rearmed</c>, a
    /// lifecycle transition that co-occurs with a terminal arm rather than
    /// excluding one. Spelled out here rather than read back off
    /// <c>LatticeMetrics</c> so that a rename or a silent drop of an arm reddens
    /// these fixtures instead of being carried along by them.
    /// </summary>
    private static readonly string[] CoverageRepairArms =
    {
        "repaired",
        "unsatisfied",
        "exhausted",
        "capture_in_flight",
        "no_checkpointed_uncovered_partition",
        "backing_off",
        "rearmed",
    };

    /// <summary>
    /// Collects <see cref="LatticeMetrics.LeafSnapshotCoverageRepairs"/>
    /// measurements for ONE tree, so concurrently-running fixtures on other trees
    /// cannot contaminate a count.
    /// <para>
    /// Sums and measurement counts are tracked separately on purpose. The
    /// zero-prime emits a real measurement whose VALUE is zero, so "the series
    /// exists" and "the arm fired" are different questions and a fixture that
    /// counted measurements alone could not tell them apart - which is precisely
    /// the distinction this issue exists to restore.
    /// </para>
    /// </summary>
    private sealed class CoverageRepairArmRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly string _treeId;
        private readonly ConcurrentDictionary<string, long> _sums = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, int> _counts = new(StringComparer.Ordinal);
        private int _measurements;

        internal CoverageRepairArmRecorder(string treeId)
        {
            _treeId = treeId;

            // Via the shared helper, never a hand-rolled listener: the helper
            // takes the instrument as a parameter, so LatticeMetrics' static
            // initialiser has necessarily completed before the listener exists
            // and the re-entrant publication hazard is not expressible.
            _listener = MeterListening.StartForInstrument(
                LatticeMetrics.LeafSnapshotCoverageRepairs,
                listener => listener.SetMeasurementEventCallback<long>(OnMeasurement));
        }

        /// <summary>Total measurements seen for this tree, primes included.</summary>
        internal int Measurements => Volatile.Read(ref _measurements);

        /// <summary>Arms that produced at least one measurement for this tree.</summary>
        internal IReadOnlyCollection<string> ArmsSeen => _counts.Keys.ToArray();

        /// <summary>Summed value recorded against <paramref name="arm"/>.</summary>
        internal long Sum(string arm) => _sums.TryGetValue(arm, out var v) ? v : 0L;

        /// <summary>Measurement count against <paramref name="arm"/>, primes included.</summary>
        internal int Count(string arm) => _counts.TryGetValue(arm, out var v) ? v : 0;

        public void Dispose() => _listener.Dispose();

        private void OnMeasurement(
            Instrument instrument,
            long measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            string? tree = null;
            string? outcome = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagTree, StringComparison.Ordinal))
                {
                    tree = tag.Value as string;
                }
                else if (string.Equals(tag.Key, LatticeMetrics.TagOutcome, StringComparison.Ordinal))
                {
                    outcome = tag.Value as string;
                }
            }

            if (!string.Equals(tree, _treeId, StringComparison.Ordinal) || outcome is null)
            {
                return;
            }

            Interlocked.Increment(ref _measurements);
            _counts.AddOrUpdate(outcome, 1, static (_, c) => c + 1);
            _sums.AddOrUpdate(outcome, measurement, (_, s) => s + measurement);
        }
    }

    /// <summary>
    /// Ask 3. Every arm is zero-primed the first time a tree evaluates the repair
    /// path, so a zero read off any arm is a MEASURED zero rather than an
    /// unpublished series.
    /// <para>
    /// The instrument's HELP text already promised this for <c>exhausted</c>, but
    /// that guarantee held only for a tree which had already emitted
    /// <c>repaired</c> - which is exactly the trees NOT under diagnosis. On
    /// <c>sys-auth-policy</c> and <c>sys-membership-edges</c> no series existed at
    /// all, so the promised measured zero did not exist for them either. Same
    /// defect class as issue #2938.
    /// </para>
    /// <para>
    /// RED pre-fix on two independent clauses: no priming at all (four arms
    /// missing), and no arm for the guard rejection this leaf actually takes.
    /// </para>
    /// </summary>
    [Test]
    public async Task Every_repair_arm_is_zero_primed_the_first_time_a_tree_evaluates_the_path()
    {
        var treeId = UniqueCoverageRepairTreeId("prime");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        // INPUT COUNT FIRST. An absence produced by machinery that never ran is
        // byte-identical to a measured absence, so nothing below is reportable
        // until the seam is known to have executed at all.
        Assert.That(recorder.Measurements, Is.GreaterThan(0),
            "the repair path must have been evaluated at least once: with zero measurements every "
            + "assertion below would be vacuous rather than true");

        Assert.That(recorder.ArmsSeen, Is.EquivalentTo(CoverageRepairArms),
            "a single evaluation must publish EVERY arm for this tree, not only the one it took - "
            + "that is the whole point of priming, and it is what makes a zero on the other four "
            + "a measured zero instead of an absent series");

        // POSITIVE CONTROL for the four zeros below: the same listener, the same
        // tree, the same instrument, observed at a non-zero value. A zero read
        // through a detector that has never been shown to fire proves nothing.
        Assert.That(recorder.Sum("no_checkpointed_uncovered_partition"), Is.EqualTo(1L),
            "positive control: this leaf has never checkpointed a partition, so the guard rejects "
            + "on its second conjunct and that arm - and only that arm - must carry a 1");

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum("repaired"), Is.Zero, "primed, not fired");
            Assert.That(recorder.Sum("unsatisfied"), Is.Zero, "primed, not fired");
            Assert.That(recorder.Sum("exhausted"), Is.Zero, "primed, not fired");
            Assert.That(recorder.Sum("capture_in_flight"), Is.Zero, "primed, not fired");
        });
    }

    /// <summary>
    /// Ask 1, second conjunct. A guard rejection because no checkpointed
    /// partition lacks coverage is recorded on its own arm.
    /// <para>
    /// This is the only POSITIVE observation of the shape where the repair is
    /// structurally unable to help a blocked leaf - the predicate it is gated on
    /// is simply not satisfied - and before this change it had no observable of
    /// any kind. A remedy aimed at the capture seam cannot fix it; the pin/guard
    /// seam is where it has to be addressed. Telling the two apart from outside
    /// the process is the entire deliverable.
    /// </para>
    /// </summary>
    [Test]
    public async Task Guard_rejection_with_no_uncovered_checkpointed_partition_is_recorded_on_its_own_arm()
    {
        var treeId = UniqueCoverageRepairTreeId("nouncovered");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        // Coverage already stamped at the checkpoint, so the predicate is false
        // for a reason other than "nothing is checkpointed" - the same arm has to
        // cover both readings of the conjunct.
        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: 5L,
            reClassifyEveryN: 1000,
            existingCoverage: new[] { 5L },
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(5, CancellationToken.None);

        Assert.That(recorder.Measurements, Is.GreaterThan(0),
            "input count: the repair path must have been evaluated for anything below to mean anything");

        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(5L),
            "precondition: coverage is already stamped, which is what makes the predicate false");

        Assert.That(recorder.Sum("no_checkpointed_uncovered_partition"), Is.GreaterThan(0L),
            "a guard rejection must be counted, not silent - this arm is the only positive "
            + "observation that the repair had nothing it could act on");

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum("repaired"), Is.Zero,
                "nothing was repaired: the predicate was already satisfied on entry");
            Assert.That(recorder.Sum("unsatisfied"), Is.Zero,
                "no capture ran, so this cannot be the attempted-and-failed arm");
            Assert.That(recorder.Sum("exhausted"), Is.Zero,
                "the budget is untouched - the guard rejected above it");
            Assert.That(saved, Is.Empty,
                "cross-check against the instrument: no capture was driven, so the arm chosen "
                + "is consistent with the work actually done");
        });
    }

    /// <summary>
    /// Ask 1, first conjunct. A guard rejection because a capture is already in
    /// flight is recorded on its own arm, distinct from the rejection above.
    /// <para>
    /// Reached the way production reaches it: a second write turn interleaves
    /// while a capture is awaiting its snapshot store. Here the re-entrant
    /// persist is driven from inside the save, which is exactly the window
    /// <c>_snapshotCaptureInFlight</c> exists to cover - it is set before the
    /// capture awaits and cleared in the finally after it.
    /// </para>
    /// <para>
    /// This arm and the previous one are the two halves of a single combined
    /// <c>if</c> before #2940, so a fixture that could not separate them would
    /// leave the issue's first ask unverified.
    /// </para>
    /// </summary>
    [Test]
    public async Task Guard_rejection_because_a_capture_is_in_flight_is_recorded_on_its_own_arm()
    {
        var treeId = UniqueCoverageRepairTreeId("inflight");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, snapshotStub, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            treeId: treeId);

        var reentered = 0;
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(async ci =>
            {
                saved.Add(ci.Arg<LeafSnapshotBlob>());
                if (Interlocked.Exchange(ref reentered, 1) == 0)
                {
                    // _snapshotCaptureInFlight is true for the duration of this
                    // call, so this nested persist drives the repair straight
                    // into the first conjunct of the guard.
                    await ((ILeafProjection)grain).SetCheckpointOffsetAsync(9, CancellationToken.None);
                }
            });

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);

        Assert.That(recorder.Measurements, Is.GreaterThan(0),
            "input count: nothing below is reportable until the seam is known to have executed");
        Assert.That(reentered, Is.EqualTo(1),
            "scanned count: the re-entrant persist must actually have run inside the capture, "
            + "otherwise the in-flight window was never entered and a zero on that arm would be "
            + "an artefact of the harness rather than a property of the code");

        Assert.That(recorder.Sum("capture_in_flight"), Is.EqualTo(1L),
            "the interleaved turn must be counted on its own arm: 'a capture is already running' "
            + "and 'there is nothing to repair' have different remedies");

        // POSITIVE CONTROL and discriminator in one: the outer capture DID land,
        // so the in-flight arm is not being credited with a run that failed.
        Assert.That(recorder.Sum("repaired"), Is.EqualTo(1L),
            "positive control: the outer capture completed and stamped coverage, so the same "
            + "listener observes a second, different arm at a non-zero value");
        Assert.That(recorder.Sum("unsatisfied"), Is.Zero,
            "the outer capture succeeded, so nothing here is attempted-and-failed");
    }

    /// <summary>
    /// Ask 2. A repair that RAN and left the partition uncovered is recorded,
    /// so it is distinguishable from one that never ran.
    /// <para>
    /// Before this the method fell through to <c>return true</c> recording
    /// nothing, which collapsed "the capture seam is failing" into the same
    /// silence as "the guard rejected on entry". The budget is deliberately not
    /// spent here - three failures against a budget of eight - so this is the
    /// <c>unsatisfied</c> arm and provably not <c>exhausted</c>.
    /// </para>
    /// </summary>
    [Test]
    public async Task Repair_that_runs_and_leaves_the_partition_uncovered_is_recorded_as_unsatisfied()
    {
        var treeId = UniqueCoverageRepairTreeId("unsatisfied");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: new InvalidOperationException("snapshot store is down"),
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        for (var i = 1; i <= 3; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(recorder.Measurements, Is.GreaterThan(0),
            "input count: the repair path must have been evaluated");
        Assert.That(saved, Has.Count.EqualTo(3),
            "scanned count: three captures must genuinely have been ATTEMPTED. A zero here would "
            + "mean the fixture never reached the code under test and every count below would be "
            + "an absence manufactured by the harness");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(-1L),
            "precondition: every capture failed, so the partition is still uncovered");

        Assert.That(recorder.Sum("unsatisfied"), Is.EqualTo(3L),
            "one per attempted-and-failed repair: a repair that ran and failed points at the "
            + "capture seam, and must not read as the silence of a repair that never ran");

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum("exhausted"), Is.Zero,
                "three attempts against a budget of eight: this is not exhaustion, and conflating "
                + "the two would point the remedy at the wrong seam");
            Assert.That(recorder.Sum("repaired"), Is.Zero,
                "coverage never landed");
            // POSITIVE CONTROL for the two zeros above.
            Assert.That(recorder.Sum("no_checkpointed_uncovered_partition"), Is.EqualTo(1L),
                "positive control: the activation evaluation found nothing checkpointed yet, so "
                + "the same listener observes a different arm at a non-zero value");
        });
    }

    /// <summary>
    /// The three asks together, on one activation, plus the boundary that used
    /// to be the instrument's documented weak point: exhaustion is reported at
    /// most once per activation, and every later evaluation inside the backoff
    /// is armed rather than silent.
    /// <para>
    /// This fixture previously recorded the opposite property, and its own
    /// arithmetic is what disproved it. Forty-one evaluations produced ten
    /// increments, and the thirty-one-increment gap was attributed to the
    /// once-per-activation exhaustion dedup. That attribution was wrong: those
    /// thirty-one evaluations never reached the dedup latch, they took the
    /// backoff-suppression branch below both entry guards, which recorded
    /// nothing at all. The dedup latch is unreachable in this flow.
    /// </para>
    /// <para>
    /// With that branch armed as <c>backing_off</c>, forty-one evaluations
    /// produce forty-one increments and the arms partition the path exactly, so
    /// the sum is no longer a lower bound needing a caveat. The equality below
    /// is kept deliberately tight because it is the only assertion in the suite
    /// that would catch a new silent return added below the entry guards.
    /// </para>
    /// </summary>
    [Test]
    public async Task Exhaustion_is_recorded_once_and_every_later_evaluation_is_armed_as_backing_off()
    {
        var treeId = UniqueCoverageRepairTreeId("exhausted");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: new InvalidOperationException("snapshot store is down"),
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        for (var i = 1; i <= 40; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(saved, Has.Count.EqualTo(8),
            "scanned count: the budget must be fully spent - eight attempts, not zero and not forty");

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum("no_checkpointed_uncovered_partition"), Is.EqualTo(1L),
                "the activation evaluation, before any partition was checkpointed");
            Assert.That(recorder.Sum("unsatisfied"), Is.EqualTo(8L),
                "one per spent attempt");
            Assert.That(recorder.Sum("exhausted"), Is.EqualTo(1L),
                "recorded ONCE for thirty-two post-budget evaluations: the arm counts stuck "
                + "activations, not retries");
            Assert.That(recorder.Sum("backing_off"), Is.EqualTo(31L),
                "the thirty-one post-budget evaluations after the one that abandoned. These "
                + "used to return in silence below both entry guards, which is what made the "
                + "documented partition false");
            Assert.That(recorder.Sum("repaired"), Is.Zero, "coverage never landed");
            Assert.That(recorder.Sum("capture_in_flight"), Is.Zero,
                "every capture is awaited to completion here, so no turn interleaves one");
        });

        // The boundary, asserted rather than described - and this number is the
        // whole point of the backing_off arm. 41 evaluations (1 at activation +
        // 40 persists) now produce 41 increments, so the partition is EXACT on
        // the path that previously under-counted it worst.
        //
        // Before the arm existed this asserted 10, and the 31-increment gap was
        // attributed to the once-per-activation exhaustion dedup. That
        // attribution was wrong on top of being incomplete: those 31
        // evaluations never reached the dedup latch at all. They took the
        // backoff-suppression branch, which recorded nothing. The dedup latch
        // is in fact unreachable in this flow - it has one call site, guarded by
        // a null re-arm deadline that the same branch immediately sets, and only
        // the re-arm clears it, in the same step as the latch.
        //
        // Keep this as an equality rather than a lower bound. It is the only
        // assertion in the suite that would catch a NEW silent return added
        // below the entry guards, which is precisely the defect class this arm
        // was added to close.
        var increments = CoverageRepairArms.Sum(recorder.Sum);
        Assert.That(increments, Is.EqualTo(41L),
            "41 evaluations, 41 increments: every evaluation below the entry guards now "
            + "records exactly one terminal arm. A shortfall here means a return path was "
            + "added without an arm and the documented partition has silently become false "
            + "again");
    }

    /// <summary>
    /// Issue #3194: a spent repair budget must be re-armed on a long-lived
    /// activation, not retired for the life of the process.
    /// <para>
    /// The budget is a plain instance field and was documented as a
    /// "per-activation ceiling", which reads as self-limiting only because
    /// activations normally turn over. Nothing in <c>src/</c> ever reset it, so
    /// on an activation that is never replaced - a leaf held continuously
    /// active by read traffic, which is exactly the population the coverage-lag
    /// bound added in this change exists to serve - eight failed captures
    /// retired the repair permanently. The recurring timer would then have
    /// driven a branch that could no longer do anything, and the whole remedy
    /// would have been a no-op on its own target after eight ticks.
    /// </para>
    /// <para>
    /// This drives ONE activation past the budget, confirms it is genuinely
    /// spent (no further capture is attempted), then expires the backoff and
    /// shows the same activation attempting captures again. Deliberately one
    /// activation throughout: re-activating would reset the field through the
    /// pre-existing route and prove nothing about the defect.
    /// </para>
    /// </summary>
    [Test]
    public async Task Spent_repair_budget_is_rearmed_on_the_same_activation_after_its_backoff()
    {
        var treeId = UniqueCoverageRepairTreeId("rearmed");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: new InvalidOperationException("snapshot store is down"),
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        for (var i = 1; i <= 20; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(saved, Has.Count.EqualTo(8),
            "the budget must be fully spent first, otherwise the re-arm below proves nothing");
        Assert.That(recorder.Sum("exhausted"), Is.EqualTo(1L), "budget spent and reported");
        Assert.That(recorder.Sum("rearmed"), Is.Zero,
            "the backoff has not elapsed, so nothing may be re-armed yet - a re-arm here would "
            + "mean the budget is not bounding anything");

        // Confirm the budget is genuinely holding, so the increase after the
        // re-arm below cannot be explained by attempts that were going to
        // happen anyway.
        var spentAt = saved.Count;
        for (var i = 21; i <= 30; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(saved, Has.Count.EqualTo(spentAt),
            "ten further persists against a spent budget must attempt no capture at all");

        // Bring the re-arm deadline forward rather than waiting it out. The
        // hook only moves an EXISTING deadline, so it cannot manufacture a
        // re-arm the production path would not itself have performed.
        grain.ExpireZeroCoverageRepairRearmForTest();

        for (var i = 31; i <= 40; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum("rearmed"), Is.EqualTo(1L),
                "the spent budget must be re-armed exactly once when its backoff elapses");
            Assert.That(saved.Count, Is.GreaterThan(spentAt),
                "and the re-armed budget must actually return the repair to service: without "
                + "this the counter would be reset while nothing ever used it again");
            Assert.That(saved, Has.Count.EqualTo(spentAt + 8),
                "a re-arm restores the full budget and no more - the ceiling still binds, so "
                + "this is a bounded retry on a slow cadence and not an unbounded retry loop");
        });
    }

    /// <summary>
    /// Issue #3194 follow-up: an evaluation suppressed by an outstanding re-arm
    /// backoff must record <c>backing_off</c> and NOTHING else.
    /// <para>
    /// Mutual exclusivity is the entire basis for calling the six terminal arms
    /// a partition, so it is asserted rather than assumed. The sibling arm
    /// <c>rearmed</c> is deliberately NOT mutually exclusive - it co-occurs with
    /// a terminal arm on the same invocation - so "this arm is recorded" is not
    /// on its own evidence that a partition holds. The property needed here is
    /// the stronger one: exactly one arm moves, and it is this one.
    /// </para>
    /// <para>
    /// Written as a before/after delta across EVERY documented arm rather than
    /// as an assertion about <c>backing_off</c> alone. An assertion that only
    /// inspects the arm under test cannot distinguish "this invocation recorded
    /// exactly this arm" from "this invocation recorded this arm and also
    /// something else", and the second is the failure that would quietly
    /// invalidate the partition claim in both documentation files.
    /// </para>
    /// </summary>
    [Test]
    public async Task Evaluation_inside_an_outstanding_backoff_records_backing_off_and_no_other_arm()
    {
        var treeId = UniqueCoverageRepairTreeId("backing-off");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: new InvalidOperationException("snapshot store is down"),
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        // Spend the budget so the next evaluation abandons and arms a backoff.
        for (var i = 1; i <= 20; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(saved, Has.Count.EqualTo(8),
            "the budget must be genuinely spent, or the evaluation below would take the "
            + "repair path instead of the backoff-suppression path and prove nothing");
        Assert.That(recorder.Sum("exhausted"), Is.EqualTo(1L),
            "the abandoning evaluation must have happened, so the backoff deadline is set");

        var before = CoverageRepairArms.ToDictionary(arm => arm, recorder.Sum);
        var savedBefore = saved.Count;

        // Exactly ONE further evaluation, inside the outstanding backoff.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(21, CancellationToken.None);

        var after = CoverageRepairArms.ToDictionary(arm => arm, recorder.Sum);

        Assert.Multiple(() =>
        {
            Assert.That(after["backing_off"] - before["backing_off"], Is.EqualTo(1L),
                "the suppressed evaluation must record exactly one backing_off. Zero is the "
                + "pre-fix behaviour: a repairable-population invocation returning in silence "
                + "below both entry guards");

            foreach (var arm in CoverageRepairArms.Where(a => a != "backing_off"))
            {
                Assert.That(after[arm] - before[arm], Is.Zero,
                    $"'{arm}' must not move on a backoff-suppressed evaluation. If it does, "
                    + "backing_off is not mutually exclusive and the six arms are not a "
                    + "partition, which would make the claim in metrics.md false");
            }

            Assert.That(saved.Count, Is.EqualTo(savedBefore),
                "positive control on the mechanism rather than the counter: a suppressed "
                + "evaluation must attempt no capture, which is what makes this branch "
                + "terminal and is the reason it is safe to call it mutually exclusive");
        });
    }

    /// <summary>
    /// Issue #3194: a coverage-lag timer tick must not advance the
    /// checkpoint-persist cadence counter.
    /// <para>
    /// <see cref="LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints"/> is
    /// documented as "every N checkpoint persists". The timer shares the same
    /// method as the persist driver, so an ungated increment would silently
    /// redefine that option as "every N persists OR ticks" - an operator's
    /// configured value would then mean something different on a read-held leaf
    /// than on a written one, with nothing anywhere saying so.
    /// </para>
    /// <para>
    /// There is a second, sharper reason. A tick that advanced the counter
    /// would reach the cadence on a write-quiet leaf, reach the capture core,
    /// and decline there on every tick for ever, minting a permanently rising
    /// decline series carrying a plausible reason label. That is a manufactured
    /// false signal, and this investigation has already lost days to exactly
    /// that shape.
    /// </para>
    /// <para>
    /// Asserted on the counter directly because it is otherwise unobservable:
    /// a leaf with no cadence deficit captures nothing either way, so an
    /// outcome-only assertion would pass whether or not the gate exists. That
    /// the timer really does call this method is proved separately and
    /// end-to-end by <c>CoverageLagBoundIntegrationTests</c>; this fixture
    /// covers what the parameter means once it is called.
    /// </para>
    /// </summary>
    [Test]
    public async Task Coverage_lag_tick_does_not_advance_the_checkpoint_persist_cadence()
    {
        var treeId = UniqueCoverageRepairTreeId("cadence-gate");

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: null,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);

        // Two persists. The first is consumed by the zero-coverage repair,
        // which returns before the cadence block; it also stamps coverage, so
        // the second reaches the cadence gate. Without that the counter would
        // sit at zero for a reason that has nothing to do with the gate under
        // test, and the assertion below would be vacuous.
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(1, CancellationToken.None);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(2, CancellationToken.None);
        var afterPersist = grain.CheckpointPersistCountSinceRecheckForTest;

        Assert.That(afterPersist, Is.GreaterThan(0),
            "positive control: a checkpoint persist MUST advance the cadence counter, otherwise "
            + "the zero asserted below would be a property of the harness rather than of the gate");

        for (var i = 0; i < 20; i++)
        {
            await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        }

        Assert.That(
            grain.CheckpointPersistCountSinceRecheckForTest,
            Is.EqualTo(afterPersist),
            "twenty coverage-lag ticks must leave the checkpoint-persist cadence exactly where "
            + "the persists left it. If this rises, LeafSnapshotReClassifyEveryNCheckpoints no "
            + "longer means what it says and a write-quiet leaf will decline at the capture core "
            + "on every tick for ever - issue #3194");
    }

    /// <summary>
    /// The per-leaf first-tick jitter spreads across the WHOLE configured
    /// period, at every supported lag - including the values that the original
    /// 32-bit arithmetic could not express.
    /// <para>
    /// This is a regression test for a real cliff rather than a property
    /// restated. A tick is 100ns, so one second is 10^7 ticks and
    /// <c>uint.MaxValue</c> is reached at 429.5 seconds. The first
    /// implementation narrowed the denominator to <c>uint</c>, so at a
    /// configured lag of 430 seconds - ONE second past the cliff - the modulus
    /// wrapped from 4,300,000,000 ticks to 5,032,704, collapsing the spread
    /// from the full period to 0.503 seconds. Every leaf in an activation burst
    /// would then have ticked within half a second of every other, for ever:
    /// precisely the synchronised blob-write stampede the jitter exists to
    /// prevent, reinstated in silence by a tuning value and invisible to the
    /// rest of the suite because the default is 300 and the integration fixture
    /// uses 2.
    /// </para>
    /// <para>
    /// The cases below straddle that cliff deliberately. Widening only the
    /// denominator would still fail the 3600s case, because a raw 32-bit hash
    /// caps the numerator at the same 429.5 seconds and the modulus becomes a
    /// no-op beyond it.
    /// </para>
    /// </summary>
    [TestCase(300)]
    [TestCase(429)]
    [TestCase(430)]
    [TestCase(900)]
    [TestCase(3600)]
    [TestCase(LatticeOptions.MaxLeafSnapshotCoverageLagSeconds)]
    public void Coverage_lag_jitter_spreads_across_the_whole_period_at_every_supported_lag(int lagSeconds)
    {
        var period = TimeSpan.FromSeconds(lagSeconds);

        var phases = Enumerable.Range(0, 4096)
            .Select(i => BPlusLeafGrain.ComputeCoverageLagJitter(HashCode.Combine("leaf", i), period))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(phases.Select(static p => p.Ticks), Is.All.InRange(0L, period.Ticks - 1),
                "a phase outside [0, period) would schedule the first tick outside the interval "
                + "it is supposed to jitter within - issue #3194");

            var spread = phases.Max() - phases.Min();
            Assert.That(spread.Ticks, Is.GreaterThan(period.Ticks * 0.9),
                $"at a configured lag of {lagSeconds}s the observed first-tick spread was only "
                + $"{spread.TotalSeconds:F3}s of a {period.TotalSeconds:F0}s period. The jitter has "
                + "collapsed, so every leaf in an activation burst ticks together and the capture "
                + "bound becomes a synchronised blob-write stampede against the storage account - "
                + "issue #3194");
        });
    }

    /// <summary>
    /// The jitter never throws on the activation path, whatever the period.
    /// The original form clamped the denominator with <c>Math.Max</c> in
    /// <see cref="long"/> and then narrowed to <see cref="uint"/>, so a period
    /// whose tick count is an exact multiple of 2^32 narrowed back to zero and
    /// divided by it - the clamp was not doing the job its placement implied.
    /// The value is far out of reach through the option (roughly 388 days) but
    /// the arithmetic is what is being pinned here, not the option's range.
    /// </summary>
    [Test]
    public void Coverage_lag_jitter_cannot_divide_by_zero_at_a_period_that_narrows_to_zero()
    {
        var wrapsToZero = TimeSpan.FromTicks(4294967296L * 8);

        Assert.Multiple(() =>
        {
            Assert.DoesNotThrow(
                () => BPlusLeafGrain.ComputeCoverageLagJitter(12345, wrapsToZero),
                "the jitter runs on the activation path and must never fail an activation - issue #3194");

            Assert.DoesNotThrow(
                () => BPlusLeafGrain.ComputeCoverageLagJitter(12345, TimeSpan.Zero),
                "a zero period must clamp rather than divide by zero - issue #3194");
        });
    }

    /// <summary>
    /// The declared arm set that zero-priming walks holds EVERY
    /// <c>CoverageRepair*</c> arm on <see cref="LatticeMetrics"/>, and nothing
    /// else.
    /// <para>
    /// This is what makes the instrument's "a zero is a MEASURED zero" claim
    /// enforceable rather than merely documented. Priming used to be a
    /// hand-written run of <c>Add(0, ...)</c> calls unrelated to the arms that
    /// exist, so an arm added later and omitted from that run would publish no
    /// zero - and an absent series reads as "the repair path never ran for this
    /// tree", which is the opposite of the truth and is the single most
    /// misleading thing this instrument could say to someone diagnosing a
    /// pinned WAL. Issue #3194 is the proof the hazard is real: adding
    /// <c>rearmed</c> needed a sixth priming line written by hand, and nothing
    /// would have caught its omission.
    /// </para>
    /// <para>
    /// The sibling <c>blocked_leaf_reactivations_total</c> earns the same claim
    /// by walking an enum through a switch that throws on an unmapped member.
    /// These arms are <see cref="KeyValuePair{TKey,TValue}"/> statics, so
    /// reflection over the declared fields is the equivalent total walk. It is
    /// deliberately derived from the CLASS rather than from the local
    /// <see cref="CoverageRepairArms"/> list, which is spelled out by hand for
    /// its own reasons and would only compare a hand-written list to another.
    /// </para>
    /// </summary>
    [Test]
    public void Zero_priming_walks_every_declared_coverage_repair_arm()
    {
        var declared = typeof(LatticeMetrics)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(static f => f.FieldType == typeof(KeyValuePair<string, object?>))
            .Where(static f => f.Name.StartsWith("CoverageRepair", StringComparison.Ordinal))
            .Select(f => ((KeyValuePair<string, object?>)f.GetValue(null)!).Value?.ToString())
            .OrderBy(static v => v, StringComparer.Ordinal)
            .ToArray();

        var primed = LatticeMetrics.CoverageRepairArms
            .Select(static a => a.Value?.ToString())
            .OrderBy(static v => v, StringComparer.Ordinal)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(declared, Is.Not.Empty,
                "the reflection scan found no CoverageRepair arms at all, so this guard would "
                + "pass vacuously. Fix the scan, not this assertion - issue #3194");

            Assert.That(primed, Is.EqualTo(declared),
                "LatticeMetrics.CoverageRepairArms must hold exactly the declared CoverageRepair "
                + "arms. An arm missing from it is never zero-primed, so its series is ABSENT "
                + "rather than zero on a tree that never recorded it - and absent is documented "
                + "to mean 'the repair path never ran', which is the opposite of the truth and "
                + "is read on exactly the trees under diagnosis - issue #3194");
        });
    }

    /// <summary>
    /// The instrument's own <c>description</c> must name every armed arm, and any
    /// arity claim it makes must match the armed count.
    /// <para>
    /// <b>This closes a SCOPE gap, not a regex gap, and the distinction is the
    /// point.</b> <c>MetricDocArmArityTests</c> is the arity guard for this
    /// repository, but it scans exactly two paths -
    /// <c>docs/lattice/metrics.md</c> and
    /// <c>docs/lattice.dashboards/metrics-to-panel-map.md</c>. <c>LatticeMetrics.cs</c>
    /// is outside its range entirely, so the <c>description:</c> string could -
    /// and did - keep asserting "All five arms are zero-primed" for a seven-arm
    /// instrument while both documentation files were correct and the build was
    /// green.
    /// </para>
    /// <para>
    /// That surface is not a derivative. A counter's description is published as
    /// the HELP text on the Prometheus endpoint, so it is what an operator reads
    /// while diagnosing a pinned WAL at 3am - ahead of either markdown file. The
    /// guard protecting the copy while leaving the original unguarded is exactly
    /// backwards, and the remedy is scope rather than diligence: this test lives
    /// beside the arm set it pins, so it cannot be outrun by a new documentation
    /// path or a renamed file.
    /// </para>
    /// <para>
    /// Deliberately asserted against <see cref="LatticeMetrics.CoverageRepairArms"/>
    /// rather than the local <see cref="CoverageRepairArms"/> literal: the class
    /// array is itself pinned to every declared <c>CoverageRepair*</c> field by
    /// <see cref="Zero_priming_walks_every_declared_coverage_repair_arm"/>, so a
    /// new arm reaches this assertion automatically instead of waiting for
    /// somebody to remember the fixture list.
    /// </para>
    /// </summary>
    [Test]
    public void Coverage_repair_description_names_every_armed_arm_and_claims_the_right_arity()
    {
        var description = LatticeMetrics.LeafSnapshotCoverageRepairs.Description;

        var armed = LatticeMetrics.CoverageRepairArms
            .Select(static a => a.Value?.ToString())
            .Where(static v => !string.IsNullOrEmpty(v))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(description, Is.Not.Null.And.Not.Empty,
                "the coverage_repairs instrument published no description at all, so this "
                + "guard would pass vacuously. Fix the instrument, not this assertion");

            Assert.That(armed, Is.Not.Empty,
                "no armed arms were found, so the enumeration check below would pass "
                + "vacuously - issue #3194");

            foreach (var arm in armed)
            {
                Assert.That(description, Does.Contain(arm!),
                    $"the coverage_repairs description does not name the '{arm}' arm. That "
                    + "string is the HELP text on /metrics, so an operator reading it sees an "
                    + "enumeration that silently omits an arm the instrument actually records");
            }

            var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["three"] = 3, ["four"] = 4, ["five"] = 5, ["six"] = 6,
                ["seven"] = 7, ["eight"] = 8, ["nine"] = 9, ["ten"] = 10,
            };

            var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["third"] = 3, ["fourth"] = 4, ["fifth"] = 5, ["sixth"] = 6,
                ["seventh"] = 7, ["eighth"] = 8, ["ninth"] = 9, ["tenth"] = 10,
            };

            // `rearmed` is the sole LIFECYCLE arm: it co-occurs with a terminal
            // arm rather than excluding one, so it is in the primed set but not
            // in the partition. Named from the production static rather than
            // subtracted as a bare 1, so that the terminal count below stays
            // derived. This set is a TRIPWIRE, not a derivation - there is no
            // way to tell a lifecycle arm from a terminal one by looking at the
            // array, so a second lifecycle arm added without being listed here
            // reddens the terminal-arity assertion with a misleading message
            // rather than passing. Reddening is the point; the message is the
            // cost.
            var lifecycleArms = new[] { LatticeMetrics.CoverageRepairRearmed.Value?.ToString() };
            var terminalArms = armed.Where(a => !lifecycleArms.Contains(a)).ToArray();

            Assert.That(terminalArms, Is.Not.Empty,
                "every armed arm was classified as lifecycle, so the terminal-arity "
                + "assertions below would pass vacuously - issue #3194");

            // Three numeric claims live in this one string, and guarding only
            // the first reproduces the very defect this test exists to catch:
            // a claim that reddens in one place while retaining its authority
            // everywhere the guard does not look. Add an eighth arm and all
            // three must redden together.
            AssertArityClaim(
                description!, @"\bAll (three|four|five|six|seven|eight|nine|ten) arms\b",
                counts, armed.Length, "zero-priming (the whole armed set)");

            AssertArityClaim(
                description!, @"\b(three|four|five|six|seven|eight|nine|ten) terminal arms\b",
                counts, terminalArms.Length, "the terminal partition (armed set minus lifecycle arms)");

            AssertArityClaim(
                description!, @"\bthe (third|fourth|fifth|sixth|seventh|eighth|ninth|tenth) tag value\b",
                ordinals, armed.Length, "the ordinal position of the lifecycle arm in the armed set");
        });
    }

    /// <summary>
    /// Asserts that a single numeric claim in an instrument description matches
    /// the count it is describing. Factored out because the coverage_repairs
    /// description makes three such claims over two different sets, and a guard
    /// that covered only one of them would be the same partial-coverage defect
    /// it is meant to prevent, one level down.
    /// </summary>
    private static void AssertArityClaim(
        string description,
        string pattern,
        Dictionary<string, int> words,
        int expected,
        string what)
    {
        var claims = Regex.Matches(description, pattern, RegexOptions.IgnoreCase);

        Assert.That(claims.Count, Is.GreaterThan(0),
            $"the coverage_repairs description no longer makes its {what} claim "
            + $"(pattern '{pattern}'). If it was deliberately reworded, update this guard "
            + "rather than deleting it - a silently dropped claim is how the arity went "
            + "stale in the first place - issue #3194");

        // EVERY occurrence, not just the first. The description states the
        // terminal arity twice, so a Regex.Match would have let somebody update
        // one and leave the other stale - the same partial-coverage defect this
        // test exists to catch, two levels down.
        foreach (Match claim in claims)
        {
            Assert.That(words[claim.Groups[1].Value], Is.EqualTo(expected),
                $"the coverage_repairs description says '{claim.Value}' but the correct value "
                + $"for {what} is {expected}. That string is the HELP text on /metrics, and this "
                + "is the exact defect MetricDocArmArityTests would have caught had "
                + "LatticeMetrics.cs been in its scan scope - issue #3194");
        }
    }

    /// <summary>
    /// Positive control for the whole file, and the arm that already existed.
    /// A repair that succeeds records <c>repaired</c> and leaves every other arm
    /// at a measured zero.
    /// <para>
    /// Its job here is to demonstrate that the harness observes the PRESENCE of
    /// the outcome it is asked to observe, so a zero anywhere else in this file
    /// is a property of the code rather than of the listener.
    /// </para>
    /// </summary>
    [Test]
    public async Task Successful_repair_records_repaired_and_leaves_every_other_arm_at_a_measured_zero()
    {
        var treeId = UniqueCoverageRepairTreeId("repaired");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, saved) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        await ((ILeafProjection)grain).SetCheckpointOffsetAsync(7, CancellationToken.None);

        Assert.That(saved, Has.Count.EqualTo(1),
            "scanned count: exactly one capture was driven by the repair");
        Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(7L),
            "precondition: the repair actually stamped coverage");

        Assert.That(recorder.Sum("repaired"), Is.EqualTo(1L),
            "the pre-existing arm must keep working: this fixture is the positive control that "
            + "the listener in this file observes presence, not only absence");

        Assert.Multiple(() =>
        {
            Assert.That(recorder.Sum("unsatisfied"), Is.Zero,
                "the capture landed, so this is not the attempted-and-failed arm");
            Assert.That(recorder.Sum("exhausted"), Is.Zero, "one attempt of eight");
            Assert.That(recorder.Sum("capture_in_flight"), Is.Zero, "no turn interleaved");
            Assert.That(recorder.Sum("no_checkpointed_uncovered_partition"), Is.EqualTo(1L),
                "the activation evaluation, before anything was checkpointed");
        });
    }

    /// <summary>
    /// The tag domain is closed. Every measurement this seam emits carries an
    /// outcome drawn from the five documented arms and a tree tag, so the
    /// dashboard panel and the metrics doc row enumerate a complete set.
    /// <para>
    /// Without this, adding a sixth arm in source would leave the enumerating
    /// prose stale and silently correct-looking - the exact shape that slips
    /// every name-keyed gate, because those gates prove a row is PRESENT and
    /// never that it is COMPLETE.
    /// </para>
    /// </summary>
    [Test]
    public async Task Every_recorded_outcome_is_drawn_from_the_documented_arm_set()
    {
        var treeId = UniqueCoverageRepairTreeId("domain");
        using var recorder = new CoverageRepairArmRecorder(treeId);

        var (grain, _, _, _) = CreateLeafForCoverageRepair(
            persistedCheckpoint: -1L,
            reClassifyEveryN: 1000,
            saveFailure: new InvalidOperationException("snapshot store is down"),
            treeId: treeId);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);
        SeedRow(grain);
        for (var i = 1; i <= 12; i++)
        {
            await ((ILeafProjection)grain).SetCheckpointOffsetAsync(i, CancellationToken.None);
        }

        Assert.That(recorder.Measurements, Is.GreaterThan(0),
            "input count: an empty domain trivially satisfies a subset assertion, so the check "
            + "below is only meaningful once measurements are known to exist");

        Assert.That(recorder.ArmsSeen, Is.SubsetOf(CoverageRepairArms),
            "no undocumented outcome value may reach the instrument: the metrics doc row and the "
            + "dashboard panel description enumerate these five and nothing else");

        Assert.That(
            LatticeMetrics.LeafSnapshotCoverageRepairs.Name,
            Is.EqualTo("orleans.lattice.leaf.snapshot.coverage_repairs"),
            "the instrument name is the join key for the doc row and the panel query; a rename "
            + "would strand both while every name-keyed gate stayed green");
    }
}
