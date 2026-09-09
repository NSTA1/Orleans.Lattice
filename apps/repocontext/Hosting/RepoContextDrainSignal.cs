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
/// an overrun line - the <b>host</b> abandoned the drain, so
/// <see cref="RepoContextHostBuilder.ShutdownBudget"/> itself is too small and no
/// grace period can rescue it (the defect of issue #2397).
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
    private readonly Lock _gate = new();
    private CancellationTokenSource? _alarmCancellation;
    private long _startedAt;
    private bool _draining;
    private bool _completed;
    private bool _overran;
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
    /// <exception cref="ArgumentNullException"><paramref name="logger"/> is null.</exception>
    public RepoContextDrainSignal(
        ILogger<RepoContextDrainSignal> logger,
        TimeSpan shutdownBudget,
        Func<long>? timestamp = null,
        Func<TimeSpan, CancellationToken, Task>? alarm = null,
        Action<int>? reportExitCode = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _shutdownBudget = shutdownBudget;
        _timestamp = timestamp ?? Stopwatch.GetTimestamp;
        _alarm = alarm ?? Task.Delay;
        _reportExitCode = reportExitCode;
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

        lock (_gate)
        {
            if (_draining)
            {
                return;
            }

            _draining = true;
            _startedAt = _timestamp();

            if (_shutdownBudget > TimeSpan.Zero)
            {
                alarmCancellation = new CancellationTokenSource();
                _alarmCancellation = alarmCancellation;
            }
        }

        _logger.LogInformation(
            "RepoContext drain started: the silo will deactivate and the WAL commit-log will flush. "
            + "The host shutdown budget is {ShutdownBudgetSeconds:F0}s; the container's stop_grace_period "
            + "must exceed it or this drain is killed mid-flight.",
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
        lock (_gate)
        {
            if (_overran || !_draining)
            {
                return;
            }

            _overran = true;
        }

        ReportAbandonedExitCode();

        _logger.LogError(
            "RepoContext drain ABANDONED after {ShutdownBudgetSeconds:F0}s: the host shutdown budget expired "
            + "before the silo finished deactivating, so the host has stopped waiting and the remaining leaf "
            + "activations are being torn down without banking their projection checkpoints. The process will "
            + "exit {ExitCode} rather than 0, so this is visible to an orchestrator and not only in this log. "
            + "Raising the container's stop_grace_period does NOT fix this - "
            + "RepoContextHostBuilder.ShutdownBudget is the binding "
            + "ceiling and must rise, and stop_grace_period must then be raised to stay strictly greater.",
            _shutdownBudget.TotalSeconds,
            RepoContextExitCode.DrainAbandoned);
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

        if (overran)
        {
            _logger.LogError(
                "RepoContext drain ran to {DrainSeconds:F1}s against a {ShutdownBudgetSeconds:F0}s host shutdown "
                + "budget, so it did NOT complete: the host abandoned deactivation at the budget and this line "
                + "reports when the stop sequence unwound, not a successful drain. The process exits "
                + "{ExitCode} rather than 0. "
                + "RepoContextHostBuilder.ShutdownBudget must rise, and the container's stop_grace_period with it.",
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
