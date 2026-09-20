using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>What startup admission decided about the granted managed-heap ceiling.</summary>
public enum RepoContextMemoryVerdict
{
    /// <summary>Nothing on record contradicts the grant. Start normally.</summary>
    Admit = 0,

    /// <summary>
    /// Something on record is worth an operator's attention, but is not proof the
    /// grant is inadequate. Start, and say so loudly.
    /// </summary>
    Warn = 1,

    /// <summary>
    /// This deployment has already recorded running out of managed heap at a ceiling
    /// no smaller than the one granted now. Refuse to start.
    /// </summary>
    Refuse = 2,
}

/// <summary>
/// One admission decision: the verdict, the message an operator reads, and the
/// override value honoured to reach it, if any.
/// </summary>
/// <param name="Verdict">What to do.</param>
/// <param name="Message">
/// The operator-facing explanation. It always names <b>both</b> numbers - what was
/// granted and what is on record - because the failure this replaces named neither.
/// </param>
/// <param name="HonouredOverrideBytes">
/// The recorded exhaustion ceiling an operator override was accepted for, or
/// <see langword="null"/>. Non-null only when an override actually suppressed a
/// refusal, so it is never set merely because the variable was present.
/// </param>
public readonly record struct RepoContextMemoryAdmissionDecision(
    RepoContextMemoryVerdict Verdict,
    string Message,
    long? HonouredOverrideBytes);

/// <summary>
/// Startup admission for the container's managed-heap grant: the single seam that
/// decides whether this process should accept work at the ceiling it has been given,
/// before it accepts any.
/// </summary>
/// <remarks>
/// <para>
/// <b>The problem (issue #3255).</b> When the grant is too small, this container
/// accepts the work and then crash-loops. Measured on one corpus at a 12 GiB grant:
/// two restarts in 16 minutes, 129 then 304 <see cref="OutOfMemoryException"/>,
/// <c>ExitCode=0</c>, <c>OOMKilled=false</c>, and <c>/health/ready</c> answering
/// 503. It reaches the operator as a STORAGE error reading grain state - that is, as
/// <i>flakiness</i> rather than as <i>insufficiency</i>. The value this adds is not
/// "always fits", which no admission check can deliver; it is that insufficiency is
/// never again presented as flakiness.
/// </para>
/// <para>
/// <b>A refusal rests only on evidence this deployment recorded about itself.</b>
/// There is a tempting alternative - re-implement the deploy-time corpus model from
/// <c>New-TuningEnv.ps1</c> and refuse below its prediction - and it is a trap. That
/// script's own comments document the model as a <i>one-point fit</i>: two
/// parameters identified by a single observation, so its "3 GiB fixed + 1 MB per
/// file" split is assumed rather than measured, and at a materially different corpus
/// size "the split IS the answer and has no evidence under it". A refusal is an
/// outage, and an outage may not rest on an unidentified parameter. <b>Nothing in
/// this type contains a byte constant, a corpus model, or a fraction.</b>
/// </para>
/// <para>
/// <b>Nor does it read the cgroup grant.</b> Everything here is denominated in the
/// managed-heap ceiling the runtime reports, which is what the process can actually
/// observe. Converting between that and the container's memory limit would route
/// through <c>GCHeapHardLimitPercent</c>, which issue #3133 owns; a check that
/// depended on it would silently change meaning the moment #3133 lands.
/// </para>
/// <para>
/// <b>The rule, in full.</b> Let <c>granted</c> be the ceiling the runtime reports
/// now, and <c>exhaustedAt</c> the largest ceiling at which this data root has ever
/// recorded managed heap exhaustion:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Refuse</b> when <c>exhaustedAt</c> exists and <c>granted &lt;= exhaustedAt</c>.
/// Read it as: <i>this corpus, on this host, has already proved it cannot run in
/// this much managed heap, and it has not been given any more.</i> That is a
/// measurement compared with a measurement.
/// </description></item>
/// <item><description>
/// <b>Warn</b> when the previous run's peak commitment does not fit inside the
/// ceiling granted now, or when the previous run never stopped cleanly.
/// </description></item>
/// <item><description><b>Admit</b> otherwise, including on every failure to read.</description></item>
/// </list>
/// <para>
/// <b>It fails open, everywhere, deliberately.</b> No record, an unreadable record,
/// a corrupt record, a record from an unrecognised format version, or a runtime that
/// reports no usable ceiling all admit. See the inversion remarks on
/// <see cref="RepoContextMemoryHistory"/>: a false admit costs exactly the status quo
/// this change improves on, whereas a false refusal is an outage in a distroless
/// container with no shell to debug it in.
/// </para>
/// <para>
/// <b>The corpus-shrink confound is accepted, not overlooked.</b> "Exhausted at a
/// ceiling no smaller than this one" is decisive only if the corpus is also no
/// smaller. Remove a repository or delete a large tree and the recorded exhaustion
/// may no longer describe the work in front of this process, so the refusal can be
/// wrong. It is accepted anyway, for a reason that has to be stated rather than
/// assumed: the refusal is <b>recoverable in one environment variable</b>
/// (<see cref="OverrideKey"/>), while the failure it prevents - admit, then exhaust
/// mid-ingest behind a STORAGE error - is the status quo and is recoverable only by
/// diagnosing a crash-loop. The asymmetry, not the confidence, is what justifies
/// refusing. The confound is removable later by recording the observed corpus size
/// beside the ceiling and refusing only when the ceiling is no larger <i>and</i> the
/// corpus no smaller; that would still be two measurements compared, with no model,
/// so it stays inside this type's principle. It is deliberately not done here.
/// </para>
/// <para>
/// <b>What this check cannot see, stated as a limitation rather than left to be
/// discovered.</b> Its refusal input is written in response to a <i>managed</i>
/// <see cref="OutOfMemoryException"/>. A cgroup OOM-kill delivers <c>SIGKILL</c>:
/// no exception, no unwinding, no opportunity to record anything, so on that path
/// the input is silently never written. <b>The absence of a recorded exhaustion is
/// therefore not evidence that the grant is sufficient.</b> There is a decent
/// argument that managed exhaustion normally precedes the kernel killer, because the
/// managed ceiling defaults to 75% of the cgroup limit and so binds first - but that
/// is an argument, it routes through the percentage #3133 owns, and at a hard-limit
/// percentage of 100 it fails outright. It is not coverage and must not be recorded
/// as coverage.
/// </para>
/// </remarks>
public static class RepoContextMemoryAdmission
{
    /// <summary>
    /// The environment variable that suppresses a refusal, whose value must be the
    /// exact recorded exhaustion ceiling in bytes that the operator is choosing to
    /// disbelieve.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A refusal with no escape hatch is a brick.</b> This is the first condition
    /// in this container that can refuse to start, the image is distroless so there
    /// is no shell to intervene with, and the data volume may be protected. An
    /// operator facing a wrong refusal at 03:00 needs a documented way through that
    /// does not involve deleting state.
    /// </para>
    /// <para>
    /// <b>Echoing the number is what makes it safe, and it is not ceremony.</b> A
    /// boolean flag would be set once, forgotten, and would then suppress every
    /// future refusal including correct ones - which is how a safety check quietly
    /// becomes decoration. Requiring the exact byte count has three properties a
    /// boolean has not:
    /// </para>
    /// <list type="bullet">
    /// <item><description>It cannot be set by accident, because the value is not guessable.</description></item>
    /// <item><description>
    /// It is <b>self-invalidating</b>: if the deployment later exhausts at a
    /// <i>larger</i> ceiling, the recorded figure changes, the stale override stops
    /// matching, and the container refuses again. The override covers the evidence
    /// the operator actually read, not all future evidence.
    /// </description></item>
    /// <item><description>
    /// It forces the operator to have looked at the number they are overriding, which
    /// is the number they need in order to fix the grant properly.
    /// </description></item>
    /// </list>
    /// <para>
    /// An honoured override is loud on <b>every</b> start it suppresses, never once,
    /// and is recorded into the history file
    /// (<see cref="RepoContextMemoryObservation.OverriddenAtLimitBytes"/>) so a later
    /// reader can see the record was deliberately disbelieved rather than wonder why
    /// a container carrying refusal-worthy evidence is running.
    /// </para>
    /// </remarks>
    public const string OverrideKey = "LATTICE_REPOCONTEXT_HEAP_ADMISSION_OVERRIDE";

    /// <summary>
    /// Decides whether to admit the process at the ceiling it has been granted.
    /// </summary>
    /// <param name="grantedLimitBytes">
    /// The managed-heap ceiling the runtime reports now. A non-positive value means
    /// no usable reading, which admits.
    /// </param>
    /// <param name="previous">The last recorded run, or <see langword="null"/>.</param>
    /// <param name="overrideValue">
    /// The raw value of <see cref="OverrideKey"/>, or <see langword="null"/>.
    /// </param>
    /// <param name="historyPath">
    /// The history file's path, named in the refusal message so an operator can find
    /// the evidence without a shell.
    /// </param>
    /// <returns>The decision.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="historyPath"/> is null.</exception>
    public static RepoContextMemoryAdmissionDecision Evaluate(
        long grantedLimitBytes,
        RepoContextMemoryObservation? previous,
        string? overrideValue,
        string historyPath)
    {
        ArgumentNullException.ThrowIfNull(historyPath);

        if (grantedLimitBytes <= 0)
        {
            return new RepoContextMemoryAdmissionDecision(
                RepoContextMemoryVerdict.Admit,
                "Heap admission skipped: the runtime reported no usable managed-heap ceiling, so there "
                + "is nothing to compare a recorded requirement against. Admitting, because a check "
                + "that cannot read its own input must never be the reason a container fails to start.",
                null);
        }

        if (previous is not { } record)
        {
            return new RepoContextMemoryAdmissionDecision(
                RepoContextMemoryVerdict.Admit,
                $"Heap admission: granted a managed-heap ceiling of {Bytes(grantedLimitBytes)} and no "
                    + $"prior run is on record at '{historyPath}', so there is no measurement to admit "
                    + "against. This run will record one.",
                null);
        }

        if (record.ExhaustedAtLimitBytes is { } exhaustedAt && grantedLimitBytes <= exhaustedAt)
        {
            if (TryParseOverride(overrideValue, exhaustedAt))
            {
                return new RepoContextMemoryAdmissionDecision(
                    RepoContextMemoryVerdict.Warn,
                    $"HEAP ADMISSION OVERRIDDEN. This deployment recorded running out of managed heap "
                        + $"at a ceiling of {Bytes(exhaustedAt)}, and the ceiling granted now is "
                        + $"{Bytes(grantedLimitBytes)}, which is no larger. Startup would normally be "
                        + $"refused. {OverrideKey} matches the recorded ceiling exactly, so the refusal is "
                        + "suppressed and the recorded evidence is being disbelieved on purpose. If this "
                        + "container crash-loops behind STORAGE errors reading grain state, that is this "
                        + "evidence coming true. Remove the override once the grant is raised.",
                    exhaustedAt);
            }

            return new RepoContextMemoryAdmissionDecision(
                RepoContextMemoryVerdict.Refuse,
                $"Refusing to start: the managed-heap ceiling granted to this process is "
                    + $"{Bytes(grantedLimitBytes)}, and this deployment has already recorded running out "
                    + $"of managed heap at a ceiling of {Bytes(exhaustedAt)}, which is no smaller. "
                    + "Accepting work at this ceiling would repeat a failure that is already measured: it "
                    + "does not present as an out-of-memory kill but as a wave of OutOfMemoryException "
                    + "behind a STORAGE error reading grain state, with exit code 0, and a crash-loop "
                    + $"that reads as flakiness. The evidence is at '{historyPath}'. Raise the "
                    + "container's memory grant. If this refusal is wrong - most likely because the "
                    + "indexed corpus has shrunk since the exhaustion was recorded, which this check "
                    + $"cannot see - set {OverrideKey}={exhaustedAt.ToString(CultureInfo.InvariantCulture)} "
                    + "to start anyway.",
                null);
        }

        // Everything below is a warning at most. None of it is proof, and the
        // messages say which is which rather than leaving an operator to calibrate
        // an alarm they cannot distinguish from the refusal above.
        if (record.PeakCommittedBytes > grantedLimitBytes)
        {
            return new RepoContextMemoryAdmissionDecision(
                RepoContextMemoryVerdict.Warn,
                $"Heap admission warning: the last run committed a peak of "
                    + $"{Bytes(record.PeakCommittedBytes)} against a ceiling of "
                    + $"{Bytes(record.GrantedLimitBytes)} ({Occupancy(record)}), and the ceiling granted "
                    + $"now is {Bytes(grantedLimitBytes)} - smaller than that peak, so the same behaviour "
                    + "would not fit. Starting anyway: this is a measured commitment, not a measured "
                    + "requirement, and a heap collects more eagerly the closer it sits to its ceiling, "
                    + "so a process may well commit less when given less. Watch "
                    + "lattice_repocontext_heap_peak_occupancy_ratio on this run.",
                null);
        }

        if (record.Outcome == RepoContextMemoryOutcome.Admitted)
        {
            return new RepoContextMemoryAdmissionDecision(
                RepoContextMemoryVerdict.Warn,
                $"Heap admission warning: the last run was admitted at a ceiling of "
                    + $"{Bytes(record.GrantedLimitBytes)} and never recorded stopping cleanly, peaking at "
                    + $"{Bytes(record.PeakCommittedBytes)} ({Occupancy(record)}) before it stopped being "
                    + "observed. That is ambiguous on its own - a SIGKILL, a host reboot, an orchestrator "
                    + "rescheduling it and an unrelated crash all leave this - so it cannot refuse. It is "
                    + "reported because it is also the only residue a cgroup out-of-memory kill leaves "
                    + "inside this container: that path gives no exception and no chance to record one, "
                    + $"so it is invisible to the refusal above. Granted {Bytes(grantedLimitBytes)} now.",
                null);
        }

        return new RepoContextMemoryAdmissionDecision(
            RepoContextMemoryVerdict.Admit,
            $"Heap admission: granted a managed-heap ceiling of {Bytes(grantedLimitBytes)}; the last "
                + $"run peaked at {Bytes(record.PeakCommittedBytes)} of {Bytes(record.GrantedLimitBytes)} "
                + $"({Occupancy(record)}) and ended {record.Outcome.ToString()}. Nothing on record "
                + "contradicts this grant.",
            null);
    }

    private static bool TryParseOverride(string? overrideValue, long exhaustedAt)
    {
        if (string.IsNullOrWhiteSpace(overrideValue))
        {
            return false;
        }

        return long.TryParse(
                overrideValue.Trim(),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var declared)
            && declared == exhaustedAt;
    }

    private static string Occupancy(RepoContextMemoryObservation record) =>
        record.PeakOccupancyRatio is { } ratio
            ? ratio.ToString("P1", CultureInfo.InvariantCulture)
            : "occupancy unknown";

    /// <summary>
    /// Renders a byte count as both the exact figure and a human-readable one.
    /// </summary>
    /// <remarks>
    /// Both, because each alone fails an operator at the moment they need it: the
    /// exact figure is what <see cref="OverrideKey"/> has to be set to and what a
    /// later reader can check, while the GiB figure is what makes "is this grant
    /// bigger than that one" answerable at a glance under pressure.
    /// </remarks>
    private static string Bytes(long value) => string.Create(
        CultureInfo.InvariantCulture,
        $"{value} bytes ({value / 1024d / 1024d / 1024d:F2} GiB)");
}
