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
    /// The sweep completed but armed nothing, because no repository is registered
    /// (or every one of them declined arming). A successful sweep with nothing to
    /// do, not a failure - and not progress either.
    /// </summary>
    Empty = 2,
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
    /// The first sweep in this process that completed with nothing to arm.
    /// </summary>
    NoRepositories = 2,

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
/// <param name="ConsecutiveFaults">The length of the fault run in progress, or zero.</param>
internal readonly record struct RepoContextAnnSweepSnapshot(
    long Armed,
    long Empty,
    long Faulted,
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
                + "least one build coordinator), 'empty' (it completed with no repository to arm), or 'faulted' "
                + "(it threw, so nothing is scheduled until it gets through). Every sweep that runs is counted, so "
                + "the total advances once per sweep - at the sweep interval while sweeps complete, and at the "
                + "faster retry cadence (250 ms doubling to a 30-second ceiling) while they fault, so do not "
                + "denominate 'faulted' by the sweep interval - and a zero on 'armed' beside a rising 'faulted' is "
                + "a measured absence of arming rather than an absent measurement. All three series reading zero "
                + "means the sweep loop is not running at all, which the service's startup line distinguishes.");
    }

    /// <summary>
    /// Records one sweep iteration and reports which transition, if any, the caller
    /// should log. The counter is incremented for every outcome, including the
    /// faulting one, because it is the partition of a total that rises once per
    /// sweep - not once per sweep interval, since a faulting sweep is re-run on the
    /// retry backoff - that makes a zero on any single arm interpretable.
    /// </summary>
    /// <param name="outcome">How the sweep ended.</param>
    /// <returns>The transition to announce, and the length of the fault run it opened or closed.</returns>
    public RepoContextAnnSweepReport Record(RepoContextAnnSweepOutcome outcome)
    {
        _annSweeps.Add(
            1,
            new KeyValuePair<string, object?>(OutcomeTagKey, DescribeOutcome(outcome)),
            LatticeTenantLabel.Platform);

        lock (_gate)
        {
            if (outcome == RepoContextAnnSweepOutcome.Faulted)
            {
                _faulted++;
                _consecutiveFaults++;
                return _consecutiveFaults == 1
                    ? new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.FaultBegan, 1)
                    : new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.None, 0);
            }

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
            return new RepoContextAnnSweepReport(RepoContextAnnSweepAnnouncement.NoRepositories, 0);
        }
    }

    /// <summary>Reads the cumulative counters.</summary>
    /// <returns>The snapshot.</returns>
    public RepoContextAnnSweepSnapshot Read()
    {
        lock (_gate)
        {
            return new RepoContextAnnSweepSnapshot(_armed, _empty, _faulted, _consecutiveFaults);
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

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();
}
