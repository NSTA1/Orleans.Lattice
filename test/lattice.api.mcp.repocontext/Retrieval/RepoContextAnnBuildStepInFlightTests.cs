using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// A build step that NEVER RETURNS must still be visible while it is running
/// (issue #3130).
/// <para>
/// <b>The blindness these pin.</b> Every other instrument on the approximate-index
/// build plane fires at a TERMINAL moment: <c>repocontext.ann.build.slice</c> is
/// recorded after the step returns, <c>repocontext.ann.index.load</c> after the
/// open finishes or throws, and the coverage arms only once the build reaches
/// Ready. A phase that does not terminate therefore emits NOTHING on any of them,
/// and the resulting all-zero reading is byte-identical to "this coordinator is
/// not stepping at all". Run 14 of epic #2368 hit exactly that: a build sat inside
/// a single non-reentrant coordinator turn for over four minutes, and the only
/// evidence available anywhere was Orleans' own generic "request has been active
/// for 00:04:00" warning in a container log. The metrics surface, which had six
/// dedicated arms for this plane, could not distinguish a wedge from an idle box.
/// </para>
/// <para>
/// <b>Why only an observable instrument can close it.</b> A counter or histogram
/// is written by the code path being measured, so a path that does not complete
/// cannot write one - the defect suppresses its own evidence. An observable gauge
/// is driven by the COLLECTOR instead, so it reports a phase while that phase is
/// still running. That is not a stylistic preference between instrument shapes; it
/// is the only shape whose emission does not depend on the thing that is stuck.
/// </para>
/// <para>
/// NonParallelizable because a <see cref="MeterListener"/> is process-wide, so it
/// observes instruments published by any fixture running beside it.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextAnnBuildStepInFlightTests
{
    /// <summary>The repository every measurement in this fixture is recorded under.</summary>
    private const string Repo = "acme/widgets";

    /// <summary>A second repository, used to pin that planes do not bleed into each other.</summary>
    private const string OtherRepo = "acme/gadgets";

    /// <summary>The embedding space every measurement in this fixture is recorded under.</summary>
    private static readonly EmbeddingSpaceTag TestSpace =
        new("test-model", 8, VectorNormalization.UnitL2);

    /// <summary>One measurement seen on the in-flight gauge.</summary>
    /// <param name="Repository">The <c>repository</c> tag, or <c>null</c>.</param>
    /// <param name="Space">The <c>space</c> tag, or <c>null</c>.</param>
    /// <param name="Phase">The <c>phase</c> tag, or <c>null</c>.</param>
    /// <param name="Value">The seconds observed.</param>
    private readonly record struct InFlightMeasurement(
        string? Repository,
        string? Space,
        string? Phase,
        double Value);

    /// <summary>
    /// A hand-rolled monotonic clock. The repository references no fake-time package,
    /// and the two members below are the entire surface the reporter uses, so a local
    /// fake is cheaper than a dependency - and it keeps these tests free of sleeps,
    /// which on a wall-clock gauge is the difference between a deterministic
    /// assertion and a timing flake.
    /// </summary>
    private sealed class ManualClock : TimeProvider
    {
        private long _timestamp;

        /// <summary>One tick per second, so a test can advance in whole seconds.</summary>
        public override long TimestampFrequency => 1;

        /// <inheritdoc />
        public override long GetTimestamp() => _timestamp;

        /// <summary>Advances the clock.</summary>
        /// <param name="seconds">How many seconds to advance by.</param>
        public void Advance(long seconds) => _timestamp += seconds;
    }

    /// <summary>
    /// Runs <paramref name="act"/> against a freshly constructed reporter, then
    /// collects every measurement the in-flight gauge reports on a single
    /// observation.
    /// </summary>
    /// <param name="clock">The clock the reporter measures against.</param>
    /// <param name="act">What to drive through the reporter before observing.</param>
    /// <returns>The measurements observed, in emission order.</returns>
    private static List<InFlightMeasurement> Observe(
        ManualClock clock, Action<RepoContextAnnBuildSliceReporter> act)
    {
        var observed = new List<InFlightMeasurement>();
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (string.Equals(
                        instrument.Meter.Name, RepoContextUsageRecorder.MeterName, StringComparison.Ordinal)
                    && string.Equals(
                        instrument.Name,
                        RepoContextAnnBuildSliceReporter.InFlightInstrumentName,
                        StringComparison.Ordinal))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            string? repository = null;
            string? space = null;
            string? phase = null;
            foreach (var tag in tags)
            {
                if (string.Equals(
                    tag.Key, RepoContextAnnBuildSliceReporter.RepositoryTagKey, StringComparison.Ordinal))
                {
                    repository = tag.Value?.ToString();
                }
                else if (string.Equals(
                    tag.Key, RepoContextAnnBuildSliceReporter.SpaceTagKey, StringComparison.Ordinal))
                {
                    space = tag.Value?.ToString();
                }
                else if (string.Equals(
                    tag.Key, RepoContextAnnBuildSliceReporter.PhaseTagKey, StringComparison.Ordinal))
                {
                    phase = tag.Value?.ToString();
                }
            }

            lock (observed)
            {
                observed.Add(new InFlightMeasurement(repository, space, phase, value));
            }
        });

        listener.Start();

        using (var reporter = new RepoContextAnnBuildSliceReporter(clock))
        {
            act(reporter);
            listener.RecordObservableInstruments();
        }

        listener.Dispose();
        return observed;
    }

    /// <summary>
    /// The value the gauge reports for one phase on the canonical plane.
    /// </summary>
    /// <param name="observed">The measurements collected.</param>
    /// <param name="phase">The phase to read.</param>
    /// <returns>The seconds reported, or <c>null</c> when that arm was not emitted.</returns>
    private static double? ValueFor(
        List<InFlightMeasurement> observed, RepoContextAnnBuildStepPhase phase)
    {
        var tag = RepoContextAnnBuildSliceReporter.DescribePhase(phase);
        foreach (var measurement in observed)
        {
            if (string.Equals(measurement.Phase, tag, StringComparison.Ordinal)
                && string.Equals(measurement.Repository, Repo, StringComparison.Ordinal))
            {
                return measurement.Value;
            }
        }

        return null;
    }

    /// <summary>
    /// A primed plane with no step running reports EVERY phase at zero - not an
    /// absence.
    /// </summary>
    /// <remarks>
    /// This is the half of the instrument that makes it readable. A gauge that
    /// emitted only while a step was in flight would report nothing on a healthy
    /// build, which is the same reading a gauge that was never wired up produces -
    /// so a reader could never tell "no step is stuck" from "this instrument is
    /// broken". Issue #2952 removed exactly that conflation from the slice counter;
    /// it must not be reintroduced one instrument over.
    /// </remarks>
    [Test]
    public void ObserveInFlight_WithNoStepInFlight_ReportsEveryPhaseAtZero()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter => reporter.EnsurePrimed(Repo, TestSpace));

        Assert.Multiple(() =>
        {
            foreach (var phase in Enum.GetValues<RepoContextAnnBuildStepPhase>())
            {
                Assert.That(
                    ValueFor(observed, phase),
                    Is.EqualTo(0d),
                    $"phase '{phase}' must be primed to zero so a healthy build is "
                    + "distinguishable from an unwired instrument");
            }
        });
    }

    /// <summary>
    /// A step still executing reports its elapsed seconds - which is the whole point
    /// of the instrument, because no other instrument on this plane emits at all
    /// until the step ends.
    /// </summary>
    [Test]
    public void ObserveInFlight_WhileStepIsRunning_ReportsElapsedSecondsOnTheLiveArm()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            reporter.ObserveStepPhase(token, RepoContextAnnBuildStepPhase.Ingesting);
            clock.Advance(240);
        });

        Assert.That(
            ValueFor(observed, RepoContextAnnBuildStepPhase.Ingesting),
            Is.EqualTo(240d),
            "a step that has not returned must still be reported, because every other "
            + "instrument on this plane only fires once it does");
    }

    /// <summary>
    /// The elapsed reading is attributed to the phase the step is INSIDE, and the
    /// phases it is not inside stay at zero.
    /// </summary>
    /// <remarks>
    /// This is the attribution that makes the gauge actionable rather than merely
    /// alarming. An 'ingesting' arm climbing without bound is a corpus-read defect;
    /// a 'persisting' one is an index-write defect. They have different owners and
    /// different remedies, so a single whole-step number - which both produce
    /// identically - can start an investigation and cannot direct one.
    /// </remarks>
    [Test]
    public void ObserveInFlight_WhileStepIsRunning_LeavesTheOtherPhasesAtZero()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            reporter.ObserveStepPhase(token, RepoContextAnnBuildStepPhase.Persisting);
            clock.Advance(97);
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                ValueFor(observed, RepoContextAnnBuildStepPhase.Persisting),
                Is.EqualTo(97d),
                "the phase the step is inside carries the elapsed time");

            foreach (var phase in Enum.GetValues<RepoContextAnnBuildStepPhase>())
            {
                if (phase == RepoContextAnnBuildStepPhase.Persisting)
                {
                    continue;
                }

                Assert.That(
                    ValueFor(observed, phase),
                    Is.EqualTo(0d),
                    $"phase '{phase}' is not executing and must read zero, so the wedge "
                    + "is attributed to one half of the step rather than to the step");
            }
        });
    }

    /// <summary>
    /// Entering a new phase RESTARTS the clock, so the reading answers "which half is
    /// not returning" rather than "has this step been slow".
    /// </summary>
    [Test]
    public void ObserveStepPhase_OnPhaseChange_RestartsTheElapsedClock()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            reporter.ObserveStepPhase(token, RepoContextAnnBuildStepPhase.Ingesting);
            clock.Advance(500);
            reporter.ObserveStepPhase(token, RepoContextAnnBuildStepPhase.Training);
            clock.Advance(3);
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                ValueFor(observed, RepoContextAnnBuildStepPhase.Training),
                Is.EqualTo(3d),
                "the live phase reports time spent in THAT phase, not in the whole step");
            Assert.That(
                ValueFor(observed, RepoContextAnnBuildStepPhase.Ingesting),
                Is.EqualTo(0d),
                "a phase the step has left is no longer in flight and must fall back to zero");
        });
    }

    /// <summary>
    /// A step that ends returns its plane to the primed zero, so a completed build
    /// does not leave a permanently climbing series behind it.
    /// </summary>
    [Test]
    public void EndStep_AfterAStepCompletes_ReturnsEveryArmToZero()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            reporter.ObserveStepPhase(token, RepoContextAnnBuildStepPhase.Ingesting);
            clock.Advance(60);
            reporter.EndStep(token);
            clock.Advance(60);
        });

        Assert.Multiple(() =>
        {
            foreach (var phase in Enum.GetValues<RepoContextAnnBuildStepPhase>())
            {
                Assert.That(
                    ValueFor(observed, phase),
                    Is.EqualTo(0d),
                    $"phase '{phase}' must be released when the step ends");
            }
        });
    }

    /// <summary>
    /// A phase recorded AFTER its step was retired is ignored rather than
    /// re-creating the entry.
    /// </summary>
    /// <remarks>
    /// A late write re-creating an entry would leave a step in flight for ever with
    /// no caller left to end it, and the gauge would then report its own bookkeeping
    /// leak as a build wedge. That is the one reading this instrument must never
    /// invent: a false wedge costs exactly the investigation the instrument exists to
    /// save.
    /// </remarks>
    [Test]
    public void ObserveStepPhase_ForARetiredToken_IsIgnored()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            reporter.EndStep(token);
            reporter.ObserveStepPhase(token, RepoContextAnnBuildStepPhase.Ingesting);
            clock.Advance(3600);
        });

        Assert.That(
            ValueFor(observed, RepoContextAnnBuildStepPhase.Ingesting),
            Is.EqualTo(0d),
            "a retired token must not be resurrected, or the gauge reports its own leak "
            + "as a wedge");
    }

    /// <summary>
    /// Two steps outstanding on the SAME plane are tracked independently, so ending
    /// the later one cannot erase the earlier one.
    /// </summary>
    /// <remarks>
    /// This is why steps are keyed by token and not by plane. Keyed by plane, a
    /// second step would overwrite the first's start time and a single
    /// <see cref="RepoContextAnnBuildSliceReporter.EndStep"/> would clear both - so
    /// the very condition the gauge exists to expose, a step that never returns,
    /// would be erased by the next step that did.
    /// </remarks>
    [Test]
    public void BeginStep_WithTwoStepsOutstandingOnOnePlane_TracksThemIndependently()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var wedged = reporter.BeginStep(Repo, TestSpace);
            reporter.ObserveStepPhase(wedged, RepoContextAnnBuildStepPhase.Ingesting);
            clock.Advance(900);

            var healthy = reporter.BeginStep(Repo, TestSpace);
            reporter.ObserveStepPhase(healthy, RepoContextAnnBuildStepPhase.Training);
            reporter.EndStep(healthy);
        });

        Assert.Multiple(() =>
        {
            Assert.That(
                ValueFor(observed, RepoContextAnnBuildStepPhase.Ingesting),
                Is.EqualTo(900d),
                "the outstanding step must survive a sibling step beginning and ending");
            Assert.That(
                ValueFor(observed, RepoContextAnnBuildStepPhase.Training),
                Is.EqualTo(0d),
                "the completed step must be released");
        });
    }

    /// <summary>
    /// A step in flight on one plane does not appear on another plane's arms.
    /// </summary>
    [Test]
    public void ObserveInFlight_WithAStepOnOnePlane_LeavesTheOtherPlaneAtZero()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            reporter.EnsurePrimed(OtherRepo, TestSpace);
            var token = reporter.BeginStep(Repo, TestSpace);
            reporter.ObserveStepPhase(token, RepoContextAnnBuildStepPhase.Ingesting);
            clock.Advance(120);
        });

        var ingesting = RepoContextAnnBuildSliceReporter.DescribePhase(
            RepoContextAnnBuildStepPhase.Ingesting);

        Assert.Multiple(() =>
        {
            Assert.That(
                ValueFor(observed, RepoContextAnnBuildStepPhase.Ingesting),
                Is.EqualTo(120d),
                "the plane that is stepping reports the elapsed time");

            var other = observed.Find(m =>
                string.Equals(m.Repository, OtherRepo, StringComparison.Ordinal)
                && string.Equals(m.Phase, ingesting, StringComparison.Ordinal));

            Assert.That(
                other.Value,
                Is.EqualTo(0d),
                "a wedge on one repository must not be attributed to another");
        });
    }

    /// <summary>
    /// Beginning a step primes the plane, so the arms exist even when nothing else
    /// has primed it first.
    /// </summary>
    [Test]
    public void BeginStep_OnAnUnprimedPlane_PrimesEveryPhaseArm()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            reporter.EndStep(token);
        });

        Assert.That(
            observed.FindAll(m => string.Equals(m.Repository, Repo, StringComparison.Ordinal)),
            Has.Count.EqualTo(Enum.GetValues<RepoContextAnnBuildStepPhase>().Length),
            "every phase must be emitted for a plane the reporter has seen");
    }

    /// <summary>A step cannot be begun for a null repository.</summary>
    [Test]
    public void BeginStep_WithNullRepoId_Throws()
    {
        using var reporter = new RepoContextAnnBuildSliceReporter();

        Assert.That(
            () => reporter.BeginStep(null!, TestSpace),
            Throws.TypeOf<ArgumentNullException>());
    }

    /// <summary>
    /// The probe forwards a phase entered mid-step to the reporter, which is what
    /// makes a phase entered DEEP INSIDE a long step visible before the step ends.
    /// </summary>
    /// <remarks>
    /// Without the forward the phase would still be recorded on the probe - and read
    /// back only when the step RETURNED, which is precisely what a wedged step never
    /// does. The probe's own single-writer contract is preserved because the only
    /// thread that calls <c>Enter</c> is the one taking the step.
    /// </remarks>
    [Test]
    public void Enter_OnAProbeWithAnAttachedSink_ReportsThePhaseBeforeTheStepEnds()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            var probe = new RepoContextAnnBuildPhaseProbe();
            probe.AttachSink(reporter, token);

            probe.Enter(RepoContextAnnBuildStepPhase.Opening);
            clock.Advance(315);
        });

        Assert.That(
            ValueFor(observed, RepoContextAnnBuildStepPhase.Opening),
            Is.EqualTo(315d),
            "a phase entered inside a step that has not returned must reach the gauge");
    }

    /// <summary>
    /// A detached probe stops forwarding, so a probe reused by a later tick cannot
    /// write that tick's phases against a retired token.
    /// </summary>
    [Test]
    public void Enter_OnADetachedProbe_DoesNotReachTheGauge()
    {
        var clock = new ManualClock();

        var observed = Observe(clock, reporter =>
        {
            var token = reporter.BeginStep(Repo, TestSpace);
            var probe = new RepoContextAnnBuildPhaseProbe();
            probe.AttachSink(reporter, token);
            probe.DetachSink();

            probe.Enter(RepoContextAnnBuildStepPhase.Reconciling);
            clock.Advance(42);
        });

        Assert.That(
            ValueFor(observed, RepoContextAnnBuildStepPhase.Reconciling),
            Is.EqualTo(0d),
            "a detached probe must not write against a token it no longer owns");
    }

    /// <summary>A probe cannot be attached to a null sink.</summary>
    [Test]
    public void AttachSink_WithANullSink_Throws()
    {
        var probe = new RepoContextAnnBuildPhaseProbe();

        Assert.That(
            () => probe.AttachSink(null!, 1),
            Throws.TypeOf<ArgumentNullException>());
    }

    /// <summary>
    /// Attaching a sink does not disturb the probe's own phase reading, which the
    /// coordinator still reads back to tag the slice it records.
    /// </summary>
    [Test]
    public void Enter_WithAnAttachedSink_StillRecordsThePhaseOnTheProbe()
    {
        using var reporter = new RepoContextAnnBuildSliceReporter();
        var token = reporter.BeginStep(Repo, TestSpace);
        var probe = new RepoContextAnnBuildPhaseProbe();
        probe.AttachSink(reporter, token);

        probe.Enter(RepoContextAnnBuildStepPhase.Training);

        Assert.That(
            probe.Phase,
            Is.EqualTo(RepoContextAnnBuildStepPhase.Training),
            "the forward must be additive - the coordinator still reads Phase back to "
            + "tag the slice it records");
    }
}
