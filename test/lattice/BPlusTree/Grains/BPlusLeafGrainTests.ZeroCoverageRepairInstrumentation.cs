using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
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
    /// The five outcome tag values <c>TryRepairZeroCoverageAsync</c> can record,
    /// spelled out here rather than read back off <c>LatticeMetrics</c> so that a
    /// rename or a silent drop of an arm reddens these fixtures instead of being
    /// carried along by them.
    /// </summary>
    private static readonly string[] CoverageRepairArms =
    {
        "repaired",
        "unsatisfied",
        "exhausted",
        "capture_in_flight",
        "no_checkpointed_uncovered_partition",
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
    /// The three asks together, on one activation, plus the ONE documented
    /// boundary on the arms: exhaustion is reported at most once per activation,
    /// so repeat exhaustions record nothing.
    /// <para>
    /// This is stated as a property rather than hidden, because it is what makes
    /// the sum across arms a LOWER BOUND on evaluations and never an over-count.
    /// Forty-one evaluations produce ten recorded increments here, and the
    /// thirty-one suppressed ones are the dedup, not a gap in the instrument.
    /// </para>
    /// </summary>
    [Test]
    public async Task Exhaustion_is_recorded_once_and_repeat_exhaustions_are_deliberately_deduplicated()
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
            Assert.That(recorder.Sum("repaired"), Is.Zero, "coverage never landed");
            Assert.That(recorder.Sum("capture_in_flight"), Is.Zero,
                "every capture is awaited to completion here, so no turn interleaves one");
        });

        // The boundary, asserted rather than described. 41 evaluations (1 at
        // activation + 40 persists) produce 10 increments; the gap IS the dedup.
        var increments = CoverageRepairArms.Sum(recorder.Sum);
        Assert.That(increments, Is.EqualTo(10L),
            "the sum across arms is a LOWER BOUND on evaluations, never an over-count: "
            + "41 evaluations, 10 increments, 31 suppressed by the once-per-activation "
            + "exhaustion dedup. Any claim that every evaluation records exactly one arm is "
            + "false and this number is why");
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
