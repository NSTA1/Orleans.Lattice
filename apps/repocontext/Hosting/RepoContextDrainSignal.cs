using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The container's observable drain signal: it times the window between the
/// start of a <c>SIGTERM</c>-driven shutdown and the point at which the host has
/// finished stopping, and emits one log line carrying the measured duration.
/// </summary>
/// <remarks>
/// <para>
/// It exists so the container's <c>stop_grace_period</c> can be <b>derived from a
/// measurement</b> rather than bisected by trying progressively larger
/// <c>docker stop -t</c> values and watching for the exit code to change from
/// <c>137</c> to <c>0</c>. Bisection is expensive (each probe costs a full
/// teardown and a full cold boot) and it only ever brackets the answer from
/// below, because a probe that ends in <c>SIGKILL</c> reports how long the drain
/// was *allowed*, never how long it *needed*. This signal reports the second
/// number directly.
/// </para>
/// <para>
/// The completion line is emitted on <see cref="IHostApplicationLifetime.ApplicationStopped"/>.
/// That event is raised after every hosted service - the silo, and with it the WAL
/// commit-log drainer - has stopped, <b>but it is also raised when the host gave up
/// waiting</b>. <c>HostShutdownTimeoutBehaviourTests</c> pins that behaviour by
/// observation: when <c>HostOptions.ShutdownTimeout</c> expires the host cancels the
/// stop token, abandons the remaining deactivation, and still raises
/// <c>ApplicationStopped</c>. So the completion line on its own cannot distinguish a
/// drain that finished from one that was abandoned, and reporting it unconditionally
/// would announce "drain complete" for an incomplete drain.
/// </para>
/// <para>
/// That is what the <b>budget alarm</b> exists for. <see cref="BeginDrain"/> arms a
/// timer for the host's own shutdown budget; if it fires before
/// <see cref="CompleteDrain"/>, the overrun is reported <b>at the moment it happens</b>,
/// while the process is still alive, and the later completion line is downgraded to
/// say the drain ran past the budget. The alarm is deliberately not a comparison made
/// after the fact: it is correct even in the case where <c>ApplicationStopped</c> never
/// arrives at all, where a post-hoc comparison would never run and the overrun would
/// stay silent.
/// </para>
/// <para>
/// The two remaining failure shapes stay distinguishable in a container log:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// no completion line and no overrun line - the container was <c>SIGKILL</c>ed
/// mid-drain, so <c>stop_grace_period</c> is too small (the defect of issue #2389).
/// </description>
/// </item>
/// <item>
/// <description>
/// an overrun line - the <b>host</b> abandoned the drain, so the budget itself is
/// too small (the defect of issue #2397). Since issue #2402 the budget is derived
/// from the declared container grant, so both cases are answered by the same pair
/// of values - the service's <c>stop_grace_period</c> and the
/// <see cref="RepoContextShutdownBudget.StopGracePeriodKey"/> that declares it -
/// raised together and kept equal.
/// </description>
/// </item>
/// </list>
/// <para>
/// An overrun also <b>reports itself to the process</b>, not only to the log: when
/// the overrun latches, the signal assigns
/// <see cref="RepoContextExitCode.DrainAbandoned"/> through the reporter its host
/// supplied, so the container exits non-zero and an orchestrator records the
/// abandonment as an error rather than as a clean stop (issue #2401). Without that,
/// the log line above is the only evidence, and it can only be found by a human who
/// already suspects something went wrong - which is the layer least able to act.
/// </para>
/// <para>
/// Both transitions are idempotent and latch on first call, so a duplicate
/// registration or a second lifetime callback cannot restart the clock or emit a
/// second, contradictory duration.
/// </para>
/// </remarks>
public sealed class RepoContextDrainSignal : IDisposable
{
    /// <summary>
    /// The fraction of the shutdown budget a completed drain may consume before the
    /// completion line is raised to a warning.
    /// </summary>
    /// <remarks>
    /// Advisory only: it changes no behaviour and gates nothing, it only decides the
    /// severity of a line that is emitted either way. It exists because the drain that
    /// motivated issue #2397 consumed 74.7% of the budget and reported at
    /// <see cref="LogLevel.Information"/>, which is indistinguishable from a drain that
    /// finished in a second. Crossing it means the next growth in resident state may
    /// carry the drain past the budget, so it is a lead indicator for the overrun
    /// below rather than a fault in itself.
    /// <para>
    /// The value is chosen so that the 67.2s-against-90s drain recorded on issue #2397
    /// trips it, since a threshold that misses the one case known to be concerning
    /// would be decorative. That makes it calibrated for SENSITIVITY, which is a
    /// different thing from a ceiling derived from a sample: it bounds no behaviour and
    /// nothing is refused when it is crossed.
    /// </para>
    /// </remarks>
    public const double HeadroomWarningFraction = 0.70;

    private readonly ILogger<RepoContextDrainSignal> _logger;
    private readonly Func<long> _timestamp;
    private readonly TimeSpan _shutdownBudget;
    private readonly Func<TimeSpan, CancellationToken, Task> _alarm;
    private readonly Action<int>? _reportExitCode;
    private readonly Func<int?>? _residentActivations;
    private readonly Action<RepoContextDrainObservation>? _recordObservation;
    private readonly Lock _gate = new();
    private CancellationTokenSource? _alarmCancellation;
    private long _startedAt;
    private bool _draining;
    private bool _completed;
    private bool _overran;
    private bool _recordedTerminal;
    private int? _residentAtStart;
    private TimeSpan? _elapsed;

    /// <summary>Initializes the drain signal.</summary>
    /// <param name="logger">The logger the drain lines are written to.</param>
    /// <param name="shutdownBudget">
    /// The host's own shutdown budget (<c>HostOptions.ShutdownTimeout</c>), reported
    /// alongside the measured duration so a reader can tell a drain that finished
    /// inside the budget from one the host itself abandoned. It is also the interval
    /// the overrun alarm is armed for, so it must be the same value the host is
    /// configured with or the alarm reports against the wrong ceiling.
    /// </param>
    /// <param name="timestamp">
    /// The monotonic timestamp source, defaulting to <see cref="Stopwatch.GetTimestamp"/>.
    /// Injectable so a test can measure a deterministic duration instead of a real one.
    /// </param>
    /// <param name="alarm">
    /// The delay used to arm the overrun alarm, defaulting to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.
    /// Injectable so a test can fire the alarm immediately instead of waiting out a
    /// real budget.
    /// </param>
    /// <param name="reportExitCode">
    /// Invoked once, with <see cref="RepoContextExitCode.DrainAbandoned"/>, at the
    /// moment an overrun latches, so the process reports the abandonment to its
    /// orchestrator and not only to its log (issue #2401).
    /// <para>
    /// <b>It defaults to null, meaning no process exit code is reported</b>, and
    /// <see cref="RepoContextHostBuilder"/> supplies
    /// <see cref="RepoContextExitCode.SetProcessExitCode"/> explicitly. That
    /// direction is deliberate and is not a stylistic preference: the default runs
    /// in the NUnit host, where several fixtures drive a deliberate overrun, and a
    /// default that assigned the real <see cref="Environment.ExitCode"/> would make
    /// the <b>test process itself</b> exit non-zero. The whole suite would pass and
    /// the run would still be reported as failed - a green that reads as red, which
    /// is no easier to diagnose than the reverse. Failing safe here and wiring
    /// explicitly there keeps the hazard out of every fixture that does not opt in.
    /// </para>
    /// </param>
    /// <param name="residentActivations">
    /// Samples the resident activation count, returning <see langword="null"/> when
    /// no reading is available.
    /// <para>
    /// It is sampled twice per drain, and the two readings answer different
    /// questions. The reading at <see cref="BeginDrain"/> is the size of the set the
    /// drain has to get through, and is what makes the recorded duration divisible
    /// into a per-activation cost that a later process can project from. The reading
    /// taken when an overrun latches is the set still resident at that instant, which
    /// is <b>the activations actually being torn down without banking their
    /// checkpoints</b> - the loss the abandonment message could previously only
    /// assert the existence of.
    /// </para>
    /// </param>
    /// <param name="recordObservation">
    /// Records the drain for the <b>next</b> process to read, once when the drain
    /// starts and again at its terminal outcome.
    /// <para>
    /// Writing the start marker separately is not bookkeeping. A record that says a
    /// drain began and never says how it ended can only have been left by a process
    /// that was killed while draining, which is direct evidence that the container's
    /// real grace period is smaller than the drain needed - the one fact this
    /// component otherwise argues is unobservable from inside the container, and
    /// which is unobservable only <i>within</i> a process rather than across a
    /// restart.
    /// </para>
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="logger"/> is null.</exception>
    public RepoContextDrainSignal(
        ILogger<RepoContextDrainSignal> logger,
        TimeSpan shutdownBudget,
        Func<long>? timestamp = null,
        Func<TimeSpan, CancellationToken, Task>? alarm = null,
        Action<int>? reportExitCode = null,
        Func<int?>? residentActivations = null,
        Action<RepoContextDrainObservation>? recordObservation = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _shutdownBudget = shutdownBudget;
        _timestamp = timestamp ?? Stopwatch.GetTimestamp;
        _alarm = alarm ?? Task.Delay;
        _reportExitCode = reportExitCode;
        _residentActivations = residentActivations;
        _recordObservation = recordObservation;
    }

    /// <summary>
    /// Whether this signal was given a process-exit-code reporter, and will
    /// therefore report an abandoned drain to the orchestrator rather than only to
    /// the log.
    /// </summary>
    /// <remarks>
    /// Exposed so the host's wiring can be asserted without a test having to trigger
    /// a real overrun and mutate the test process's own exit code. It reports that a
    /// reporter was supplied; it does not and cannot report which one, so it is the
    /// wiring that is under test here, and the behaviour of the production reporter
    /// itself is pinned separately against <see cref="RepoContextExitCode.SetProcessExitCode"/>.
    /// </remarks>
    public bool ReportsProcessExitCode => _reportExitCode is not null;

    /// <summary>
    /// Whether the drain has started (the host has begun stopping).
    /// </summary>
    public bool IsDraining
    {
        get { lock (_gate) { return _draining; } }
    }

    /// <summary>
    /// Whether the drain ran to completion. False while a drain is in flight, and
    /// permanently false in a process that was killed mid-drain - which is exactly
    /// the state the missing log line reports.
    /// </summary>
    /// <remarks>
    /// True does <b>not</b> mean the drain succeeded. The host raises
    /// <see cref="IHostApplicationLifetime.ApplicationStopped"/> even when it
    /// abandoned the drain at the budget, so read this together with
    /// <see cref="HasOverrunBudget"/>.
    /// </remarks>
    public bool HasCompleted
    {
        get { lock (_gate) { return _completed; } }
    }

    /// <summary>
    /// Whether the drain outlived the host's shutdown budget, meaning the host
    /// stopped waiting and deactivation was abandoned part-way.
    /// </summary>
    public bool HasOverrunBudget
    {
        get { lock (_gate) { return _overran; } }
    }

    /// <summary>
    /// The measured drain duration, or <see langword="null"/> until the drain
    /// completes.
    /// </summary>
    public TimeSpan? Elapsed
    {
        get { lock (_gate) { return _elapsed; } }
    }

    /// <summary>
    /// Starts the drain clock. Idempotent: only the first call latches, so the
    /// measured window always begins at the first shutdown signal.
    /// </summary>
    public void BeginDrain()
    {
        CancellationTokenSource? alarmCancellation = null;
        var resident = SampleResident();

        lock (_gate)
        {
            if (_draining)
            {
                return;
            }

            _draining = true;
            _startedAt = _timestamp();
            _residentAtStart = resident;

            if (_shutdownBudget > TimeSpan.Zero)
            {
                alarmCancellation = new CancellationTokenSource();
                _alarmCancellation = alarmCancellation;
            }
        }

        // Written BEFORE the drain runs, so a process killed part-way leaves a
        // record that says a drain began and never says how it ended. That record is
        // the evidence a later start reads as "the real grace period was smaller
        // than this drain needed".
        Record(new RepoContextDrainObservation(
            DateTimeOffset.UtcNow,
            RepoContextDrainOutcome.Started,
            _shutdownBudget,
            Duration: null,
            resident));

        _logger.LogInformation(
            "RepoContext drain started with {Resident} resident activations: the silo will deactivate and the "
            + "WAL commit-log will flush. The host shutdown budget is {ShutdownBudgetSeconds:F0}s; the "
            + "container's stop_grace_period must exceed it or this drain is killed mid-flight.",
            resident is { } count
                ? count.ToString(System.Globalization.CultureInfo.InvariantCulture)
                : "an unreadable number of",
            _shutdownBudget.TotalSeconds);

        if (alarmCancellation is not null)
        {
            // Deliberately not awaited: the alarm has to run alongside the stop
            // sequence it is watching, and it reports by logging rather than by
            // returning anything.
            _ = WatchForOverrunAsync(alarmCancellation.Token);
        }
    }

    /// <summary>
    /// Waits out the shutdown budget and, if the drain has not completed by then,
    /// reports the overrun at the moment it happens rather than after the fact.
    /// </summary>
    /// <remarks>
    /// Reporting at the moment of overrun is the point. A comparison made once the
    /// drain finally completes cannot fire in the case where it never does, and the
    /// process is still alive and still logging at the instant the budget expires,
    /// so this line reaches the container log even when the completion line does not.
    /// </remarks>
    private async Task WatchForOverrunAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _alarm(_shutdownBudget, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The drain completed inside the budget and disarmed the alarm.
            return;
        }
        catch (Exception ex)
        {
            // The alarm is diagnostics: a fault in it must never disturb a shutdown
            // that is otherwise proceeding normally.
            _logger.LogDebug(ex, "The RepoContext drain overrun alarm failed and no overrun will be reported.");
            return;
        }

        ReportOverrun();
    }

    /// <summary>
    /// Emits the overrun line once. Latches, so the alarm and a late
    /// <see cref="CompleteDrain"/> cannot both report the same overrun.
    /// </summary>
    private void ReportOverrun()
    {
        var strandedNow = SampleResident();
        TimeSpan measured;
        int? residentAtStart;

        lock (_gate)
        {
            if (_overran || !_draining)
            {
                return;
            }

            _overran = true;
            measured = Stopwatch.GetElapsedTime(_startedAt, _timestamp());
            residentAtStart = _residentAtStart;
        }

        ReportAbandonedExitCode();

        // Recorded at the alarm rather than only at completion, because a process
        // killed moments after this line would otherwise leave only a start marker
        // and the next start would report the weaker killed-mid-drain finding when
        // the stronger measured one was already available.
        RecordTerminal(RepoContextDrainOutcome.Abandoned, measured, residentAtStart, final: false);

        _logger.LogError(
            "RepoContext drain ABANDONED after {ShutdownBudgetSeconds:F0}s: the host shutdown budget expired "
            + "before the silo finished deactivating, so the host has stopped waiting. {Loss} The process will "
            + "exit {ExitCode} rather than 0, so this is visible to an orchestrator and not only in this log. "
            + "The budget is derived from the container grace period the deployment declares, so the remedy is "
            + "to raise the service's stop_grace_period AND the " + RepoContextShutdownBudget.StopGracePeriodKey
            + " that declares it, together and to the same value - to at least {RequiredSeconds:F0}s, which is "
            + "derived from this drain and grants no headroom. Raising only the declaration buys no drain time "
            + "and silences this line, because the container still kills the process at the real grace period. "
            + "Drain duration tracks the resident activation set, which nothing here bounds; this drain is "
            + "recorded so the next start reports the mismatch BEFORE the next stop rather than during it.",
            _shutdownBudget.TotalSeconds,
            DescribeLoss(strandedNow, residentAtStart),
            RepoContextExitCode.DrainAbandoned,
            RepoContextShutdownBudget.RequiredGrantFor(measured).TotalSeconds);
    }

    /// <summary>
    /// Renders what the abandonment actually cost: how many activations were still
    /// resident at the instant the host stopped waiting, which is the count torn down
    /// without banking their projection checkpoints.
    /// </summary>
    /// <remarks>
    /// Rendered as a clause rather than a bare number so that an unreadable count
    /// reads as unreadable. Substituting a zero would turn a lost measurement into a
    /// confident claim that nothing was lost, which is the more expensive of the two
    /// mistakes by a wide margin.
    /// </remarks>
    private static string DescribeLoss(int? strandedNow, int? residentAtStart)
    {
        if (strandedNow is not { } stranded)
        {
            return "The activations still resident at that instant are being torn down without banking their "
                + "projection checkpoints; the resident count could not be read, so this line cannot say how "
                + "many.";
        }

        var count = stranded.ToString(System.Globalization.CultureInfo.InvariantCulture);
        if (residentAtStart is not { } atStart)
        {
            return $"{count} activations were still resident and are being torn down without banking their "
                + "projection checkpoints.";
        }

        return $"{count} activations were still resident and are being torn down without banking their "
            + $"projection checkpoints, out of {atStart.ToString(System.Globalization.CultureInfo.InvariantCulture)} "
            + "resident when the drain began.";
    }

    /// <summary>
    /// Samples the resident activation count, swallowing any fault. A probe that
    /// throws costs a diagnostic; a probe that throws <b>on the drain path</b> would
    /// cost the stop sequence.
    /// </summary>
    private int? SampleResident()
    {
        if (_residentActivations is null)
        {
            return null;
        }

        try
        {
            return _residentActivations();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Sampling the RepoContext resident activation count failed.");
            return null;
        }
    }

    /// <summary>
    /// Records the drain's terminal outcome for the next process.
    /// </summary>
    /// <remarks>
    /// The alarm records a non-final outcome and the completion path records a final
    /// one, in that order, so the more accurate measurement supersedes the less
    /// accurate: the alarm can only ever report the budget it fired at, whereas the
    /// completion path reports what the drain actually took. Only a final record
    /// latches, so an alarm that fires after completion cannot overwrite a completed
    /// drain with an abandoned one.
    /// </remarks>
    private void RecordTerminal(
        RepoContextDrainOutcome outcome,
        TimeSpan measured,
        int? residentAtStart,
        bool final)
    {
        lock (_gate)
        {
            if (_recordedTerminal)
            {
                return;
            }

            _recordedTerminal = final;
        }

        Record(new RepoContextDrainObservation(
            DateTimeOffset.UtcNow,
            outcome,
            _shutdownBudget,
            measured,
            residentAtStart));
    }

    /// <summary>
    /// Hands an observation to the recorder, swallowing any fault for the same reason
    /// the alarm swallows its own.
    /// </summary>
    private void Record(RepoContextDrainObservation observation)
    {
        if (_recordObservation is null)
        {
            return;
        }

        try
        {
            _recordObservation(observation);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Recording the RepoContext drain observation failed.");
        }
    }

    /// <summary>
    /// Reports the abandoned-drain exit code to the process, once per latched
    /// overrun (issue #2401).
    /// </summary>
    /// <remarks>
    /// A fault in the reporter is swallowed for the same reason the alarm swallows
    /// its own: this is diagnostics, and it must never turn a shutdown that is
    /// otherwise proceeding into a crash. Swallowing here costs the exit-code
    /// signal, whereas throwing would cost the remainder of the stop sequence.
    /// </remarks>
    private void ReportAbandonedExitCode()
    {
        if (_reportExitCode is null)
        {
            return;
        }

        try
        {
            _reportExitCode(RepoContextExitCode.DrainAbandoned);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Reporting the RepoContext abandoned-drain exit code failed.");
        }
    }

    /// <summary>
    /// Stops the drain clock and emits the measured duration. Idempotent: only the
    /// first call latches, so a duplicate lifetime callback cannot report a second,
    /// contradictory duration. A call that arrives without a preceding
    /// <see cref="BeginDrain"/> is ignored rather than reporting a duration measured
    /// from an unset start.
    /// </summary>
    /// <remarks>
    /// The line it emits depends on how much of the budget the drain consumed,
    /// because the host raises <c>ApplicationStopped</c> whether the drain finished
    /// or was abandoned. A drain that reached the budget is reported as abandoned, at
    /// <see cref="LogLevel.Error"/>; one that consumed more than
    /// <see cref="HeadroomWarningFraction"/> of it is reported at
    /// <see cref="LogLevel.Warning"/>; only a drain with real headroom left is
    /// reported as an unqualified success.
    /// </remarks>
    public void CompleteDrain()
    {
        TimeSpan measured;
        bool overran;
        bool latchedHere = false;
        int? residentAtStart;
        CancellationTokenSource? alarmCancellation;

        lock (_gate)
        {
            if (!_draining || _completed)
            {
                return;
            }

            _completed = true;
            measured = Stopwatch.GetElapsedTime(_startedAt, _timestamp());
            _elapsed = measured;

            // Belt and braces alongside the alarm. A drain can reach the budget
            // without the alarm having been observed yet - timer resolution, or a
            // saturated thread pool during teardown - and an overrun that the clock
            // can see must never be reported as a clean drain because of that race.
            if (_shutdownBudget > TimeSpan.Zero && measured >= _shutdownBudget && !_overran)
            {
                _overran = true;
                latchedHere = true;
            }

            overran = _overran;
            residentAtStart = _residentAtStart;
            alarmCancellation = _alarmCancellation;
            _alarmCancellation = null;
        }

        alarmCancellation?.Cancel();
        alarmCancellation?.Dispose();

        // Only when the clock latched the overrun that the alarm had not already
        // reported, so the exit code is assigned exactly once per drain however the
        // two paths race.
        if (latchedHere)
        {
            ReportAbandonedExitCode();
        }

        // The completion path carries the fuller measurement - the alarm can only
        // ever record the budget it fired at - so it records too, and RecordTerminal
        // latches so the two paths cannot write contradictory records.
        RecordTerminal(
            overran ? RepoContextDrainOutcome.Abandoned : RepoContextDrainOutcome.Completed,
            measured,
            residentAtStart,
            final: true);

        if (overran)
        {
            _logger.LogError(
                "RepoContext drain ran to {DrainSeconds:F1}s against a {ShutdownBudgetSeconds:F0}s host shutdown "
                + "budget, so it did NOT complete: the host abandoned deactivation at the budget and this line "
                + "reports when the stop sequence unwound, not a successful drain. The process exits "
                + "{ExitCode} rather than 0. Raise the service's stop_grace_period and the "
                + RepoContextShutdownBudget.StopGracePeriodKey + " that declares it together, to the same "
                + "value; the budget is derived from the second and bounded by the first.",
                measured.TotalSeconds,
                _shutdownBudget.TotalSeconds,
                RepoContextExitCode.DrainAbandoned);
            return;
        }

        var consumed = _shutdownBudget > TimeSpan.Zero
            ? measured.TotalSeconds / _shutdownBudget.TotalSeconds
            : 0d;

        if (consumed >= HeadroomWarningFraction)
        {
            _logger.LogWarning(
                "RepoContext drain complete in {DrainSeconds:F1}s, consuming {ConsumedPercent:F1}% of the "
                + "{ShutdownBudgetSeconds:F0}s host shutdown budget. The drain completed, but the headroom is "
                + "thin: drain time grows with resident grain state, so the next growth in the index may carry "
                + "it past the budget, at which point deactivation is abandoned rather than merely slow.",
                measured.TotalSeconds,
                consumed * 100d,
                _shutdownBudget.TotalSeconds);
            return;
        }

        _logger.LogInformation(
            "RepoContext drain complete in {DrainSeconds:F1}s, consuming {ConsumedPercent:F1}% of the "
            + "{ShutdownBudgetSeconds:F0}s host shutdown budget. This is the measured teardown requirement: "
            + "the container's stop_grace_period must exceed it, and the absence of this line in a container "
            + "log means the drain was killed before it finished.",
            measured.TotalSeconds,
            consumed * 100d,
            _shutdownBudget.TotalSeconds);
    }

    /// <summary>
    /// Binds the signal to the host lifetime so the drain window is measured across
    /// the whole stop sequence: the clock starts on
    /// <see cref="IHostApplicationLifetime.ApplicationStopping"/> and stops on
    /// <see cref="IHostApplicationLifetime.ApplicationStopped"/>, which the host
    /// raises only once every hosted service has stopped.
    /// </summary>
    /// <param name="lifetime">The host application lifetime.</param>
    /// <exception cref="ArgumentNullException"><paramref name="lifetime"/> is null.</exception>
    public void Bind(IHostApplicationLifetime lifetime)
    {
        ArgumentNullException.ThrowIfNull(lifetime);

        lifetime.ApplicationStopping.Register(BeginDrain);
        lifetime.ApplicationStopped.Register(CompleteDrain);
    }

    /// <summary>
    /// Disarms the overrun alarm. Safe to call whether or not a drain ever started,
    /// and safe to call more than once.
    /// </summary>
    public void Dispose()
    {
        CancellationTokenSource? alarmCancellation;
        lock (_gate)
        {
            alarmCancellation = _alarmCancellation;
            _alarmCancellation = null;
        }

        alarmCancellation?.Cancel();
        alarmCancellation?.Dispose();
    }
}
