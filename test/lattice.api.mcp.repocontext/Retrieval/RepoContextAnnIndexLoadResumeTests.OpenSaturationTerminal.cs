using Orleans.Lattice.Testing;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The terminal state an unbroken run of admission refusals reaches, and how a
/// reader tells it apart from a slow cold open (issue #3286).
/// <para>
/// <b>Why this fixture exists, stated as the reading it makes actionable.</b>
/// Issue #3284 taught the open to book a refusal as its own outcome rather than
/// as a fault, which was right and is covered by the sibling
/// <c>OpenRefusal</c> fixture. What it left behind was a loop with no terminal
/// state. Measured live on the affected host,
/// <c>repocontext_ann_index_load_total{outcome="refused"}</c> rose at 0.66 per
/// minute for as long as it was watched while <c>fresh</c> and <c>resumed</c>
/// stayed at zero, <c>ann_build_step_in_flight_seconds{phase="opening"}</c> sat
/// at 273 seconds with every later phase at zero, and <c>/health/ready</c>
/// returned 503 throughout. <b>Every one of those readings is also what a large,
/// healthy, slow cold open produces.</b> An operator cannot act on a signal that
/// means both "wait" and "this will not finish at the present capacity".
/// </para>
/// <para>
/// <b>These tests put the admission gate at its withholding floor rather than
/// simulating one.</b> A test that drives the open to success on a small fixture
/// proves nothing about this path, because the path is only reachable when the
/// permit gate is actually refusing - so every test here refuses through
/// <see cref="BlockingScanStore.RefuseAfter"/>, which raises the real
/// <see cref="LatticeSaturatedException"/> out of the watched scan, and every
/// test asserts <c>store.Refusals</c> moved before asserting anything else.
/// </para>
/// <para>
/// <b>The zero assertions here are on <c>Fresh</c> and <c>Resumed</c>, never on
/// <c>Faulted</c>.</b> Since #3284 the faulted arm reads zero on a plane that has
/// never once loaded, so asserting it is zero is satisfied by the defect as well
/// as by the fix and proves nothing. What the defect cannot produce is a plane
/// that never arms while also declaring that it never armed.
/// </para>
/// </summary>
public sealed partial class RepoContextAnnIndexLoadResumeTests
{
    /// <summary>
    /// The same budgeted options the sibling open fixtures use, with the two
    /// refusal-run bounds pulled in so a test can reach one inside a few ticks
    /// rather than inside the shipped twelve refusals or ten minutes.
    /// </summary>
    private static RepoContextAnnOptions SaturationBoundedOptions(
        TimeProvider clock,
        int maxConsecutiveRefusals,
        TimeSpan terminalPeriod) => new()
        {
            MinimumTrainingCount = 8,
            PartitionCount = 4,
            Probes = 4,
            FlushAfterUpdates = 1,
            IngestBatchSize = 16,
            MaxItemsPerChunk = 8,
            OpenSliceBudget = OpenBudget,
            MaxOpenSliceExtensions = DefaultMaxOpenSliceExtensions,
            MaxConsecutiveOpenRefusals = maxConsecutiveRefusals,
            OpenRefusalTerminalPeriod = terminalPeriod,
            TimeProvider = clock,
        };

    /// <summary>
    /// Drives one open attempt, swallowing the escalation the open raises once it
    /// has been refused past <see cref="MaxEmptyOpenDeferrals"/> consecutive times.
    /// </summary>
    /// <remarks>
    /// The escalation is the #3130 bound and is correct: it stops a single tick
    /// retrying for ever. It is also NOT the terminal state this fixture is about,
    /// because the coordinator catches it and ticks again, which is exactly how a
    /// refusal run outlives any one attempt. Swallowing it here keeps the test
    /// about the run rather than about the attempt.
    /// </remarks>
    private async Task<bool> TickThroughRefusalAsync(RepoContextAnnIndexHandle handle)
    {
        try
        {
            await handle.AdvanceAsync(Ct);
            return false;
        }
        catch (LatticeSaturatedException)
        {
            return true;
        }
    }

    [Test]
    public async Task A_refusal_run_reaching_the_count_bound_is_declared_terminal()
    {
        // THE DEFECT, STATED AS A TEST. Before this change the run had no terminal
        // state at all: every tick booked another refusal and the state after the
        // hundredth was indistinguishable from the state after the first.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var readiness = new RepoContextRetrievalReadinessState(clock);

        using var handle = NewHandle(
            SeededSource(),
            store,
            prefix,
            reporter,
            SaturationBoundedOptions(clock, maxConsecutiveRefusals: 4, terminalPeriod: TimeSpan.Zero),
            readiness);

        for (var attempt = 1; attempt < 4; attempt++)
        {
            await TickThroughRefusalAsync(handle);

            Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Refusing),
                $"refusal {attempt} of 4 is still inside the bound, and a run inside its bound must read "
                + "as transient back-pressure - declaring on the first refusal would report an outage "
                + "every time a busy silo deferred an open");
        }

        await TickThroughRefusalAsync(handle);

        Assert.That(store.Refusals, Is.GreaterThan(0),
            "instrument validation: the walk must actually have been refused by the admission gate, or "
            + "every assertion here passes by observing a walk that never ran");

        var snapshot = reporter.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Unavailable),
                "AT THE BOUND THE RUN MUST DECLARE. This is the whole of issue #3286 part 2: without it "
                + "a plane that will never arm at the present capacity emits exactly what a plane that "
                + "is about to arm emits.");

            Assert.That(handle.ConsecutiveOpenRefusals, Is.EqualTo(4),
                "the run is counted, not merely latched, so the declaration can name how many refusals "
                + "produced it");

            Assert.That(readiness.Phase,
                Is.EqualTo(RepoContextRetrievalReadinessPhase.SaturatedUnavailable),
                "THE DECLARATION HAS TO BE READABLE FROM OUTSIDE. A terminal state the open knows and "
                + "readiness cannot report leaves /health/ready returning the same 503 it returned "
                + "before, which is the reading the issue is about.");

            Assert.That(readiness.IsReady, Is.False,
                "saturated-unavailable is NOT ready. It is a new phase, so the previous negative form "
                + "of the ready test - anything that is not Building - would have silently flipped it "
                + "to ready and reported a dead plane as serving.");

            Assert.That(snapshot.Fresh, Is.EqualTo(0),
                "the plane never armed, which is the condition being declared. Asserting the FAULTED "
                + "arm is zero instead would prove nothing: since #3284 it reads zero on a plane that "
                + "has never once loaded.");

            Assert.That(snapshot.Resumed, Is.EqualTo(0),
                "and it never resumed either, so no arm of the load counter can account for this plane "
                + "having served");
        });
    }

    [Test]
    public async Task A_refusal_run_reaching_the_elapsed_bound_is_declared_terminal()
    {
        // WHY A SECOND BOUND EXISTS. A count bound alone is never reached by a host
        // whose coordinator ticks slowly - which is precisely the host whose
        // operator most needs the signal, because a slow tick means the refusal
        // count climbs at a rate that reads as healthy.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var readiness = new RepoContextRetrievalReadinessState(clock);

        using var handle = NewHandle(
            SeededSource(),
            store,
            prefix,
            reporter,
            SaturationBoundedOptions(
                clock,
                maxConsecutiveRefusals: 0,
                terminalPeriod: TimeSpan.FromMinutes(5)),
            readiness);

        await TickThroughRefusalAsync(handle);

        Assert.That(store.Refusals, Is.GreaterThan(0),
            "instrument validation: the run must have started from a real refusal");

        Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Refusing),
            "one refusal with the count bound disabled is transient - the elapsed bound has not run yet");

        // The run is measured from the FIRST refusal, not from the latest one. A
        // window measured between consecutive refusals could never be reached
        // however long the saturation lasted.
        clock.Advance(TimeSpan.FromMinutes(6));

        Assert.Multiple(() =>
        {
            Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Unavailable),
                "THE ELAPSED BOUND MUST FIRE WITHOUT A FURTHER REFUSAL. A host that is refused once and "
                + "then ticks slowly is exactly the case this bound exists for, so requiring another "
                + "refusal to notice would make the bound unreachable on that host.");

            Assert.That(handle.ConsecutiveOpenRefusals, Is.EqualTo(1),
                "one refusal, declared on elapsed time rather than on count");
        });

        // And the declaration still has to reach readiness, which only happens on a
        // refusal: the handle does not poll. This is the next tick.
        await TickThroughRefusalAsync(handle);

        Assert.That(readiness.Phase,
            Is.EqualTo(RepoContextRetrievalReadinessPhase.SaturatedUnavailable),
            "the elapsed bound declares the same readable state the count bound does - two ways of "
            + "reaching one conclusion, not two conclusions");
    }

    [Test]
    public async Task A_declared_run_clears_when_admission_recovers()
    {
        // TERMINAL IS A CLAIM, NOT A BEHAVIOUR. The open goes on retrying, so a
        // plane whose saturation clears must self-heal without an operator. A latch
        // that could not leave Unavailable would convert a capacity episode into a
        // permanent outage requiring a restart - strictly worse than the unbounded
        // retry it replaced.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var readiness = new RepoContextRetrievalReadinessState(clock);

        using var handle = NewHandle(
            SeededSource(),
            store,
            prefix,
            reporter,
            SaturationBoundedOptions(clock, maxConsecutiveRefusals: 2, terminalPeriod: TimeSpan.Zero),
            readiness);

        for (var attempt = 1; attempt <= 2; attempt++)
        {
            await TickThroughRefusalAsync(handle);
        }

        Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Unavailable),
            "precondition: the run must actually have been declared, or the recovery below is a "
            + "recovery from nothing");
        Assert.That(readiness.Phase,
            Is.EqualTo(RepoContextRetrievalReadinessPhase.SaturatedUnavailable),
            "precondition: readiness must actually be reporting the declaration");

        // Which is what saturation does.
        store.StopRefusing();
        await handle.EnsureBuiltAsync(Ct);

        Assert.Multiple(() =>
        {
            Assert.That(handle.IsServing, Is.True,
                "a declared run that could not complete once admission recovered would be an outage the "
                + "declaration CAUSED rather than reported");

            Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Clear),
                "the run ends at the open that completed");

            Assert.That(handle.ConsecutiveOpenRefusals, Is.EqualTo(0),
                "and the count ends with it, so a later episode is measured from its own first refusal "
                + "rather than from a tally carried over from a run that recovered");

            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building),
                "readiness returns to ARMING, not straight to serving. The open completing is what ends "
                + "the saturation episode; whether the plane then serves is a separate fact reported by "
                + "whoever marks it serving.");
        });
    }

    [Test]
    public async Task A_run_that_banks_progress_on_every_attempt_is_never_declared()
    {
        // THE FALSE POSITIVE THIS SHAPE EXISTS TO AVOID, and the reason the run is
        // present-tense rather than a lifetime tally. A walk that advances on every
        // attempt and is refused on every attempt is CONVERGING. A lifetime tally
        // would declare it unavailable on its second refusal here and report an
        // outage on a healthy sliced open over a merely busy silo.
        var clock = new ManualTimeProvider();
        var store = await SeededStoreAsync();
        var prefix = RepoContextAnnIndexKeys.IndexPrefix(RepoId, Space);
        var keyMap = VectorIndexStorageKeys.KeyMapPrefix(prefix);
        using var reporter = new RepoContextAnnIndexLoadReporter();
        var readiness = new RepoContextRetrievalReadinessState(clock);

        using var handle = NewHandle(
            SeededSource(),
            store,
            prefix,
            reporter,
            SaturationBoundedOptions(clock, maxConsecutiveRefusals: 2, terminalPeriod: TimeSpan.Zero),
            readiness);

        // Each attempt serves one more mapping than the last before being refused,
        // so every attempt is refused AND every attempt banks progress.
        for (var attempt = 1; attempt <= 5; attempt++)
        {
            store.RefuseAfter(keyMap, attempt);
            await TickThroughRefusalAsync(handle);
        }

        Assert.That(store.Refusals, Is.GreaterThanOrEqualTo(5),
            "instrument validation: every attempt must have been refused, or this asserts nothing about "
            + "a run of refusals");

        Assert.Multiple(() =>
        {
            Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Clear),
                "FIVE REFUSALS AGAINST A BOUND OF TWO, AND STILL NOT DECLARED, because every one of them "
                + "was followed by banked progress");

            Assert.That(readiness.Phase, Is.EqualTo(RepoContextRetrievalReadinessPhase.Building),
                "and readiness reports a plane that is still arming, which it is");
        });
    }

    [Test]
    public async Task A_refusal_that_escalates_is_classified_the_same_way_by_both_instruments()
    {
        // ISSUE #3286 PART 1, PROVED END TO END RATHER THAN IN THE CLASSIFIER ALONE.
        // The classifier table pins ClassifyBuildFault against a hand-built
        // exception. That would still pass if the open stopped throwing the
        // saturation type and threw something else on its way out, which is exactly
        // how the two instruments came to disagree in the first place. So this
        // drives the REAL open to its escalation through the REAL admission gate and
        // classifies whatever it actually threw.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var load = new RepoContextAnnIndexLoadReporter();
        using var slices = new RepoContextAnnBuildSliceReporter();

        using var handle = NewHandle(
            SeededSource(),
            store,
            prefix,
            load,
            SaturationBoundedOptions(clock, maxConsecutiveRefusals: 12, terminalPeriod: TimeSpan.Zero));

        LatticeSaturatedException? escalated = null;
        for (var attempt = 1; attempt <= MaxEmptyOpenDeferrals && escalated is null; attempt++)
        {
            try
            {
                await handle.AdvanceAsync(Ct);
            }
            catch (LatticeSaturatedException ex)
            {
                escalated = ex;
            }
        }

        Assert.That(store.Refusals, Is.GreaterThan(0),
            "instrument validation: the walk must actually have been refused by the admission gate");
        Assert.That(escalated, Is.Not.Null,
            "precondition: the open must have escalated, or there is no fault for the tick-wide "
            + "handler to classify and this test observes nothing");

        // This is the tick-wide handler's exact call, on the exact instance the open
        // rethrew.
        slices.RecordFaulted(
            RepoContextAnnIndexBuildGrain.ClassifyBuildFault(escalated!),
            RepoId,
            Space,
            RepoContextAnnBuildStepPhase.Opening);

        var sliceSnapshot = slices.Read();
        var loadSnapshot = load.Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(loadSnapshot.Refused, Is.GreaterThan(0),
                "positive control: the load instrument books this event as a refusal, which is the "
                + "classification the slice instrument has to agree with");

            Assert.That(sliceSnapshot.FaultedByCause.Saturated, Is.EqualTo(1),
                "THE TWO INSTRUMENTS MUST AGREE ABOUT ONE EVENT. They tracked 1:1 at exactly 62 each "
                + "before #3285 split the load instrument's refusal out; leaving the slice instrument "
                + "behind did not remove the disagreement, it made it measurable.");

            Assert.That(sliceSnapshot.FaultedByCause.Unexpected, Is.EqualTo(0),
                "AND A BENIGN, CORRECTLY-BOUNDED REFUSAL MUST NOT PAGE. 'unexpected' is the arm this "
                + "instrument's own HELP text calls the only value that should page, so booking a "
                + "working admission bound there pages an operator for the bound working.");

            Assert.That(loadSnapshot.Faulted, Is.EqualTo(0),
                "and the load instrument's faulted arm stays clean, so the two agree on what this was "
                + "as well as on what it was not");
        });
    }

    [Test]
    public async Task A_refused_open_without_a_readiness_state_still_bounds_its_run()
    {
        // The readiness state is an OPTIONAL collaborator, because the handle is
        // constructed directly by several fixtures and by hosts that predate it.
        // The bound must therefore hold on the handle's own state rather than only
        // through the collaborator, or a null readiness would silently restore the
        // unbounded loop.
        var clock = new ManualTimeProvider();
        var (store, prefix) = await RefusingStoreAsync(serveBeforeRefusing: 0);
        using var reporter = new RepoContextAnnIndexLoadReporter();

        using var handle = NewHandle(
            SeededSource(),
            store,
            prefix,
            reporter,
            SaturationBoundedOptions(clock, maxConsecutiveRefusals: 2, terminalPeriod: TimeSpan.Zero),
            readiness: null);

        for (var attempt = 1; attempt <= 2; attempt++)
        {
            await TickThroughRefusalAsync(handle);
        }

        Assert.That(store.Refusals, Is.GreaterThan(0),
            "instrument validation: the walk must actually have been refused");

        Assert.That(handle.OpenSaturation, Is.EqualTo(RepoContextAnnOpenSaturationState.Unavailable),
            "the run is bounded by the handle, not by whoever happens to be listening to it");
    }
}
