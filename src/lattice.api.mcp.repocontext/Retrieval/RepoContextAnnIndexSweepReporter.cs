using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// How one iteration of the approximate-index build sweep ended. The three values
/// are exhaustive over a sweep that ran, which is what lets them be counted as a
/// partition rather than as three unrelated tallies.
/// <para>
/// <see cref="Empty"/> exists because a sweep that completes without arming
/// anything is a distinct state that reads exactly like success: it returns
/// cleanly, it settles into the long cadence, and it schedules nothing. Folding it
/// into <see cref="Armed"/> would leave "the plane never builds because no
/// repository is registered" indistinguishable from "the plane never builds for
/// some other reason".
/// </para>
/// </summary>
internal enum RepoContextAnnSweepOutcome
{
    /// <summary>
    /// The sweep threw before completing, so an unknown number of repositories
    /// were left unarmed. Until it gets through, nothing is scheduled at all.
    /// </summary>
    Faulted = 0,

    /// <summary>
    /// The sweep completed and armed at least one build coordinator. This is the
    /// only outcome under which the approximate index can make progress.
    /// </summary>
    Armed = 1,

    /// <summary>
    /// The sweep completed but armed nothing, because it observed no repository in
    /// the store listing (or every one it observed declined arming). A successful
    /// sweep with nothing to do, not a failure - and not progress either.
    /// <para>
    /// Note what this tag does and does not assert. It records what the sweep
    /// <i>observed</i>, never what the store <i>contains</i>: the listing is an
    /// observation like any other and can be wrong, which is the failure mode issue
    /// #2406 records. Anything that reports this outcome must report the observed
    /// count rather than assert an empty store.
    /// </para>
    /// </summary>
    Empty = 2,
}

/// <summary>
/// Why a sweep faulted, as a closed vocabulary resolved at the fault site rather
/// than inferred from the exception afterwards.
/// <para>
/// <b>Why this exists.</b> The <see cref="RepoContextAnnSweepOutcome.Faulted"/> arm
/// is literally correct about what it counts and says nothing about what to do
/// about it. The final scrape of the gate run 2 container read
/// <c>armed 5, faulted 4</c>: four faults, no cause, and four causes below that
/// need four different responses - one is a registration defect, one is a startup
/// race that clears itself, one is deterministic and will never clear, and one is a
/// bug. A reader who cannot tell them apart supplies a cause, and the one a reader
/// supplies is always the benign one.
/// </para>
/// <para>
/// <b>Why the default value is the loud one.</b> <see cref="Unexpected"/> is zero
/// deliberately. Every recording path names its cause explicitly and the recording
/// call has no default argument, so an unset value cannot arise through the public
/// surface at all - but were one ever to, it must read as the value that pages
/// rather than as one of the four that have a known and often benign explanation.
/// A vocabulary whose default is benign is the same defect in a new costume.
/// </para>
/// <para>
/// <b>What this vocabulary deliberately cannot express, and why.</b> Issue #2578
/// proposed <c>corpus-empty</c> and <c>insufficient-corpus</c> among its values.
/// Neither is reachable from here and neither is included. Both are conditions of
/// the <i>build</i> phase, evaluated inside the coordinator's build step long after
/// the sweep's arming call has returned; the sweep calls
/// <see cref="RepoContextAnnIndexScheduler.TryArmAsync"/>, which registers a
/// reminder and returns, and never reads a vector count at any point. A cause value
/// that no site can ever set does not read as absent - it reads as checked and
/// fine, which is precisely the misreading this whole dimension exists to prevent.
/// The corpus question is real and is answered by build-phase telemetry, not here:
/// note that the <see cref="RepoContextAnnSweepOutcome.Empty"/> arm's own
/// documentation already disclaims the corpus reading in the same terms.
/// </para>
/// <para>
/// <b>And why there is no <c>startup-ordering</c> value.</b> Whether a given
/// failure from the store means "the silo is not dispatch-ready yet" or "this is
/// genuinely broken" is not decidable from the exception, so such a tag would be a
/// guess rendered as a measurement. Transience is a property of the series over
/// time and is already readable without a tag: <c>faulted</c> flat while
/// <c>armed</c> advances is a startup transient, which is exactly the run 2
/// signature, whereas <c>faulted</c> advancing while <c>armed</c> stays flat is
/// not. What the vocabulary does say is <i>which</i> dependency was not ready -
/// <see cref="ListingUnavailable"/> or <see cref="DependencyUnavailable"/> - which
/// is the actionable half and is decidable.
/// </para>
/// </summary>
internal enum RepoContextAnnSweepFaultCause
{
    /// <summary>
    /// Something outside the four known causes. The only value that should page: it
    /// means a path faulted in a way nobody has classified, so the vocabulary itself
    /// is behind the code. Also covers a cancellation that escapes the per-repository
    /// loop while the host is <i>not</i> shutting down, which is a bug rather than an
    /// orderly stop.
    /// </summary>
    Unexpected = 0,

    /// <summary>
    /// Resolving the sweep's run credential threw, so <b>nothing was attempted</b> -
    /// no listing, no arming. Look at the run-authority registration. This arm
    /// matters out of proportion to how often it fires, because the neighbouring
    /// failure is silent: an uncredentialed sweep does <i>not</i> throw on a
    /// default-deny gate, it reads back an empty listing and reports
    /// <see cref="RepoContextAnnSweepOutcome.Empty"/> forever, which is the defect
    /// issue #2406 records.
    /// </summary>
    AuthorityUnavailable = 1,

    /// <summary>
    /// The repository listing threw, so <b>nothing was attempted</b> and the
    /// accompanying observed count of zero corroborates rather than contradicts
    /// that. A grain call from a hosted service's start can race ahead of the silo
    /// becoming dispatch-ready, and that race lands here, so a small burst of this
    /// cause at process start that stops once arming begins is the expected shape
    /// and not a defect.
    /// </summary>
    ListingUnavailable = 2,

    /// <summary>
    /// A build coordinator was reached and <b>refused the request it was given</b> -
    /// an argument-shaped failure, or an embedding-space mismatch. Deterministic: the
    /// retry backoff will re-issue the same rejected call indefinitely and never
    /// clear it, so this cause needs a change rather than patience.
    /// </summary>
    PlaneRejected = 3,

    /// <summary>
    /// A build coordinator <b>could not be reached</b>: silo churn, a rejected
    /// message, or a transport, storage or IO failure underneath. Expected to clear
    /// on its own once the cluster settles.
    /// <para>
    /// Deliberately excludes a grain call timeout. A non-reentrant coordinator inside
    /// a long build turn answers the arming call late, and the sweep already treats
    /// that as a deferral rather than a fault - it is the expected answer from a
    /// healthy coordinator doing exactly the work it was armed to do, and counting it
    /// here would re-create the false-failure signal issue #2252 records.
    /// </para>
    /// </summary>
    DependencyUnavailable = 4,
}

/// <summary>
/// The fault counts by cause, cumulative since process start.
/// </summary>
/// <param name="Unexpected">Faults with no recognised cause.</param>
/// <param name="AuthorityUnavailable">Faults resolving the run credential.</param>
/// <param name="ListingUnavailable">Faults listing the repositories.</param>
/// <param name="PlaneRejected">Arming calls a coordinator refused.</param>
/// <param name="DependencyUnavailable">Arming calls that could not reach a coordinator.</param>
internal readonly record struct RepoContextAnnSweepFaultTally(
    long Unexpected,
    long AuthorityUnavailable,
    long ListingUnavailable,
    long PlaneRejected,
    long DependencyUnavailable)
{
    /// <summary>
    /// The sum over every cause. Equal to the faulted arm of the outcome partition
    /// by construction, which is what makes an unclassified path detectable rather
    /// than merely undocumented.
    /// </summary>
    internal long Total =>
        Unexpected + AuthorityUnavailable + ListingUnavailable + PlaneRejected + DependencyUnavailable;

    /// <summary>The count for one cause.</summary>
    /// <param name="cause">The cause to read.</param>
    /// <returns>The cumulative count.</returns>
    internal long For(RepoContextAnnSweepFaultCause cause) => cause switch
    {
        RepoContextAnnSweepFaultCause.AuthorityUnavailable => AuthorityUnavailable,
        RepoContextAnnSweepFaultCause.ListingUnavailable => ListingUnavailable,
        RepoContextAnnSweepFaultCause.PlaneRejected => PlaneRejected,
        RepoContextAnnSweepFaultCause.DependencyUnavailable => DependencyUnavailable,
        _ => Unexpected,
    };
}

/// <summary>
/// The transition, if any, that one recorded sweep outcome represents and that is
/// therefore worth a log line. Steady state announces nothing: repetitions go to
/// the counter, which is what keeps a fault that persists for hours from writing a
/// line every thirty seconds.
/// </summary>
internal enum RepoContextAnnSweepAnnouncement
{
    /// <summary>Steady state. The outcome is counted and not logged.</summary>
    None = 0,

    /// <summary>
    /// The first sweep in this process that armed a coordinator. This is the line
    /// whose absence made the whole arming path unobservable: without it, a sweep
    /// that works and a sweep that never ran produce identical output.
    /// </summary>
    FirstArmed = 1,

    /// <summary>
    /// The first sweep in this process that completed without arming anything.
    /// Named for what was observed - nothing was armed - rather than for a cause,
    /// because the two causes the <see cref="RepoContextAnnSweepOutcome.Empty"/> tag
    /// covers are not distinguishable from here.
    /// </summary>
    ArmedNothing = 2,

    /// <summary>
    /// The first fault of a new run of consecutive faults. The exception belongs on
    /// this line; subsequent faults in the same run are counted, not logged.
    /// </summary>
    FaultBegan = 3,

    /// <summary>
    /// A sweep completed after one or more consecutive faults, which closes the
    /// episode the <see cref="FaultBegan"/> line opened and reports how long it
    /// lasted.
    /// </summary>
    Recovered = 4,
}

/// <summary>
/// What the caller should say about one recorded sweep outcome, and the length of
/// the fault run the outcome opened or closed.
/// </summary>
/// <param name="Announcement">The transition worth logging, if any.</param>
/// <param name="ConsecutiveFaults">
/// For <see cref="RepoContextAnnSweepAnnouncement.FaultBegan"/> the length of the
/// run so far (always one); for <see cref="RepoContextAnnSweepAnnouncement.Recovered"/>
/// the length of the run that just ended; otherwise zero. Computed inside the
/// recording call from locals, so it cannot be torn by a concurrent record.
/// </param>
internal readonly record struct RepoContextAnnSweepReport(
    RepoContextAnnSweepAnnouncement Announcement,
    long ConsecutiveFaults);

/// <summary>
/// A point-in-time reading of the sweep counters, cumulative since process start.
/// </summary>
/// <param name="Armed">Sweeps that completed and armed at least one coordinator.</param>
/// <param name="Empty">Sweeps that completed with nothing to arm.</param>
/// <param name="Faulted">Sweeps that threw.</param>
/// <param name="FaultedByCause">
/// The faulted total decomposed by cause. Sums to <paramref name="Faulted"/>.
/// </param>
/// <param name="ConsecutiveFaults">The length of the fault run in progress, or zero.</param>
internal readonly record struct RepoContextAnnSweepSnapshot(
    long Armed,
    long Empty,
    long Faulted,
    RepoContextAnnSweepFaultTally FaultedByCause,
    long ConsecutiveFaults);

/// <summary>
/// Meters the approximate-index build sweep, and decides which of its outcomes are
/// transitions worth a log line rather than a counter increment.
/// <para>
/// <b>Why this exists.</b> Before it, every state of the arming path produced the
/// same output on a deployment running at information level: nothing. A sweep
/// arming successfully logged at debug, a sweep throwing logged at debug and
/// swallowed the exception, and a sweep that never started logged nothing because
/// it never ran. Three states, one observation - the same defect class as issue
/// #2252, where a plane serving from a trained partitioning and a plane serving by
/// exhaustive scan were one count. Field evidence on the deployed container:
/// across 45,572 log lines the string <c>repo-context-vector-index</c> never
/// appears, no build coordinator was ever seen armed, and the plane never served,
/// with nothing in the log able to say which of the three states produced that.
/// </para>
/// <para>
/// <b>Why a zero here is evidence and not silence.</b> The counter records every
/// sweep that ran, partitioned by outcome, so the total advances once per sweep
/// for as long as the loop is alive. A fault that persists therefore
/// shows as <c>outcome=faulted</c> climbing while <c>outcome=armed</c> stays at
/// zero - loudest exactly when the hazard is occurring, rather than silent. A
/// counter that only counted successful arming would instead read zero at the
/// highest rate of the very fault it exists to catch, which is the shape declined
/// on issue #2314.
/// </para>
/// <para>
/// <b>Two cadences, so a rate taken against the interval is the wrong number.</b>
/// The loop waits the sweep interval after a sweep that completes but the retry
/// backoff after one that faults, and that backoff starts at 250 ms and doubles
/// to a 30-second ceiling. The faulting arm therefore advances far faster than
/// the interval, not at it. Against the 15-minute default reconcile interval a
/// fault episode records nine sweeps in its first 62 seconds - at 0.0, 0.25,
/// 0.75, 1.75, 3.75, 7.75, 15.75, 31.75 and 61.75 seconds - where a completing
/// sweep would not yet have recorded its second at all, and it settles to 30 per
/// interval once the backoff tops out. Against the one-minute floor that
/// steady-state ratio is 2. So an operator who derives an expected rate from the
/// sweep interval and compares <c>faulted</c> against it will understate the
/// fault by that factor. The arm is louder than such a denominator predicts
/// rather than quieter, so the error does not hide a fault - but it does misprice
/// one, and the correct denominator is the total across all three arms, which is
/// why the partition is total.
/// </para>
/// <para>
/// <b>What it deliberately cannot cover.</b> A sweep loop that never starts
/// records nothing, so every series reads zero - indistinguishable from a process
/// that has only just come up. No counter emitted by the sweep can close that gap,
/// because the gap is the sweep not running. It is closed instead by the
/// unconditional information line
/// <see cref="RepoContextAnnIndexSweepService"/> writes on entry, before any
/// branch: with that line present the loop started, and with it absent the service
/// never executed. The two together separate all three states; neither does alone.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexSweepReporter : IDisposable
{
    /// <summary>
    /// The counter of sweep iterations, partitioned by outcome. Named on the
    /// repository-context surface so one scraper subscription covers it.
    /// </summary>
    internal const string SweepInstrumentName = "repocontext.ann.sweep";

    /// <summary>The tag key carrying the outcome partition.</summary>
    internal const string OutcomeTagKey = "outcome";

    /// <summary>The tag value for a sweep that armed at least one coordinator.</summary>
    internal const string OutcomeArmedTag = "armed";

    /// <summary>The tag value for a sweep that completed with nothing to arm.</summary>
    internal const string OutcomeEmptyTag = "empty";

    /// <summary>The tag value for a sweep that threw.</summary>
    internal const string OutcomeFaultedTag = "faulted";

    /// <summary>
    /// The tag key carrying the fault cause. Emitted on the faulted arm only, so the
    /// completing arms keep exactly the cardinality they had before this dimension
    /// existed.
    /// </summary>
    internal const string CauseTagKey = "cause";

    /// <summary>The tag value for a fault resolving the run credential.</summary>
    internal const string CauseAuthorityUnavailableTag = "authority-unavailable";

    /// <summary>The tag value for a fault listing the repositories.</summary>
    internal const string CauseListingUnavailableTag = "listing-unavailable";

    /// <summary>The tag value for an arming call a coordinator refused.</summary>
    internal const string CausePlaneRejectedTag = "plane-rejected";

    /// <summary>The tag value for an arming call that could not reach a coordinator.</summary>
    internal const string CauseDependencyUnavailableTag = "dependency-unavailable";

    /// <summary>The tag value for a fault with no recognised cause.</summary>
    internal const string CauseUnexpectedTag = "unexpected";

    // Declared above the instrument it constructs, and the instrument is built from
    // this field, so reordering the two throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Counter<long> _annSweeps;

    private readonly Lock _gate = new();
    private long _armed;
    private long _empty;
    private long _faulted;
    private long _faultedAuthorityUnavailable;
    private long _faultedListingUnavailable;
    private long _faultedPlaneRejected;
    private long _faultedDependencyUnavailable;
    private long _faultedUnexpected;
    private long _consecutiveFaults;
    private bool _announcedArmed;
    private bool _announcedEmpty;

    /// <summary>Creates the reporter and its instrument.</summary>
    public RepoContextAnnIndexSweepReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _annSweeps = _meter.CreateCounter<long>(
            SweepInstrumentName,
            unit: "{sweep}",
            description:
                "Approximate-index build sweeps partitioned by outcome: 'armed' (the sweep completed and armed at "
                + "least one build coordinator), 'empty' (it completed without arming anything, either because it "
                + "observed no repository in the store listing or because every repository it observed declined "
                + "arming - this arm reports what the sweep observed, never that the store is empty), or 'faulted' "
                + "(it threw, so nothing is scheduled until it gets through). Every sweep that runs is counted, so "
                + "the total advances once per sweep - at the sweep interval while sweeps complete, and at the "
                + "faster retry cadence (250 ms doubling to a 30-second ceiling) while they fault, so do not "
                + "denominate 'faulted' by the sweep interval - and a zero on 'armed' beside a rising 'faulted' is "
                + "a measured absence of arming rather than an absent measurement. All three series reading zero "
                + "means the sweep loop is not running at all, which the service's startup line distinguishes. "
                + "The 'faulted' arm alone carries a second tag, 'cause', drawn from a closed set that is resolved "
                + "where the fault is raised: 'authority-unavailable' (resolving the run credential threw, so "
                + "nothing was attempted), 'listing-unavailable' (the repository listing threw, so nothing was "
                + "attempted and the observed count of zero corroborates rather than contradicts that), "
                + "'plane-rejected' (a coordinator was reached and refused the request, which is deterministic and "
                + "will not clear on retry), 'dependency-unavailable' (a coordinator could not be reached, which "
                + "is expected to clear once the cluster settles, and which deliberately excludes a grain call "
                + "timeout because that is a busy coordinator and is counted as a deferral rather than a fault), "
                + "or 'unexpected' (unclassified - the only value that should page). The cause says what to do; it "
                + "does not say whether the fault will persist, which is read from the shape of the series instead: "
                + "'faulted' flat while 'armed' advances is a startup transient, whereas 'faulted' advancing while "
                + "'armed' stays flat is not. No cause reports on the corpus, because the sweep only arms a "
                + "coordinator and never reads a vector count.");
    }

    /// <summary>
    /// Records one sweep iteration that completed, and reports which transition, if
    /// any, the caller should log. The counter is incremented for every outcome,
    /// including the faulting one, because it is the partition of a total that rises
    /// once per sweep - not once per sweep interval, since a faulting sweep is re-run
    /// on the retry backoff - that makes a zero on any single arm interpretable.
    /// </summary>
    /// <param name="outcome">How the sweep ended. Must not be the faulting outcome.</param>
    /// <returns>The transition to announce, and the length of the fault run it closed.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="outcome"/> is <see cref="RepoContextAnnSweepOutcome.Faulted"/>,
    /// which must be recorded through <see cref="RecordFaulted"/> so that it carries a
    /// cause.
    /// </exception>
    /// <remarks>
    /// The split into two entry points is what makes acceptance criterion 2 of issue
    /// #2578 - that no path may emit a default or empty cause - structural rather than
    /// a matter of discipline. A single <c>Record(outcome, cause = default)</c> would
    /// have let a new fault path compile while emitting the default value, and a
    /// default is exactly how the next reader is handed a benign-looking number again.
    /// Here a fault cannot be counted without a cause because there is no overload that
    /// accepts one without.
    /// </remarks>
    public RepoContextAnnSweepReport RecordCompleted(RepoContextAnnSweepOutcome outcome)
    {
        if (outcome == RepoContextAnnSweepOutcome.Faulted)
        {
            throw new ArgumentOutOfRangeException(
                nameof(outcome),
                outcome,
                "A faulted sweep must be recorded through RecordFaulted so that it carries a cause.");
        }

        _annSweeps.Add(
            1,
            new KeyValuePair<string, object?>(OutcomeTagKey, DescribeOutcome(outcome)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            if (outcome == RepoContextAnnSweepOutcome.Armed)
            {
                _armed++;
            }
            else
            {
                _empty++;
            }

            // A recovery closes an open fault episode and supersedes a first-of-kind
            // announcement: an operator reading the log needs the episode's end more
            // than the fact that this happened to be the first sweep of its kind.
            var run = _consecutiveFaults;
            _consecutiveFaults = 0;
            if (run > 0)
            {
                return new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.Recovered, run);
            }

            if (outcome == RepoContextAnnSweepOutcome.Armed)
            {
                if (_announcedArmed)
                {
                    return new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.None, 0);
                }

                _announcedArmed = true;
                return new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.FirstArmed, 0);
            }

            if (_announcedEmpty)
            {
                return new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.None, 0);
            }

            _announcedEmpty = true;
            return new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.ArmedNothing, 0);
        }
    }

    /// <summary>
    /// Records one sweep iteration that threw, under the cause the fault site
    /// resolved, and reports whether it opened a new fault run.
    /// </summary>
    /// <param name="cause">
    /// Why the sweep faulted. Required, and resolved where the fault was raised
    /// rather than inferred here, because the site is the only place that knows which
    /// stage was executing.
    /// </param>
    /// <returns>The transition to announce, and the length of the fault run it opened.</returns>
    public RepoContextAnnSweepReport RecordFaulted(RepoContextAnnSweepFaultCause cause)
    {
        _annSweeps.Add(
            1,
            new KeyValuePair<string, object?>(OutcomeTagKey, OutcomeFaultedTag),
            new KeyValuePair<string, object?>(CauseTagKey, DescribeCause(cause)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            _faulted++;
            switch (cause)
            {
                case RepoContextAnnSweepFaultCause.AuthorityUnavailable:
                    _faultedAuthorityUnavailable++;
                    break;
                case RepoContextAnnSweepFaultCause.ListingUnavailable:
                    _faultedListingUnavailable++;
                    break;
                case RepoContextAnnSweepFaultCause.PlaneRejected:
                    _faultedPlaneRejected++;
                    break;
                case RepoContextAnnSweepFaultCause.DependencyUnavailable:
                    _faultedDependencyUnavailable++;
                    break;
                default:
                    // Fails open onto the arm that pages, matching the tag DescribeCause
                    // resolves for the same value, so the tally can never disagree with
                    // the meter about which arm an out-of-range cast landed on.
                    _faultedUnexpected++;
                    break;
            }

            _consecutiveFaults++;
            return _consecutiveFaults == 1
                ? new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.FaultBegan, 1)
                : new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.None, 0);
        }
    }

    /// <summary>Reads the cumulative counters.</summary>
    /// <returns>The snapshot.</returns>
    public RepoContextAnnSweepSnapshot Read()
    {
        lock (_gate)
        {
            return new RepoContextAnnSweepSnapshot(
                _armed,
                _empty,
                _faulted,
                new RepoContextAnnSweepFaultTally(
                    _faultedUnexpected,
                    _faultedAuthorityUnavailable,
                    _faultedListingUnavailable,
                    _faultedPlaneRejected,
                    _faultedDependencyUnavailable),
                _consecutiveFaults);
        }
    }

    /// <summary>
    /// The bounded tag value for an outcome. Resolved against a closed set so an
    /// unrecognised value can never reach the meter as unbounded-cardinality text.
    /// </summary>
    /// <param name="outcome">The outcome to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeOutcome(RepoContextAnnSweepOutcome outcome) => outcome switch
    {
        RepoContextAnnSweepOutcome.Armed => OutcomeArmedTag,
        RepoContextAnnSweepOutcome.Empty => OutcomeEmptyTag,
        _ => OutcomeFaultedTag,
    };

    /// <summary>
    /// The bounded tag value for a fault cause. Resolved against a closed set so an
    /// unrecognised value can never reach the meter as unbounded-cardinality text,
    /// and fails open onto <see cref="RepoContextAnnSweepFaultCause.Unexpected"/> -
    /// the arm that pages - rather than onto one with a benign explanation.
    /// </summary>
    /// <param name="cause">The cause to describe.</param>
    /// <returns>The tag value.</returns>
    internal static string DescribeCause(RepoContextAnnSweepFaultCause cause) => cause switch
    {
        RepoContextAnnSweepFaultCause.AuthorityUnavailable => CauseAuthorityUnavailableTag,
        RepoContextAnnSweepFaultCause.ListingUnavailable => CauseListingUnavailableTag,
        RepoContextAnnSweepFaultCause.PlaneRejected => CausePlaneRejectedTag,
        RepoContextAnnSweepFaultCause.DependencyUnavailable => CauseDependencyUnavailableTag,
        _ => CauseUnexpectedTag,
    };

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
