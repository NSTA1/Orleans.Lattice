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
/// The completion line is emitted on <see cref="IHostApplicationLifetime.ApplicationStopped"/>,
/// which the generic host raises only after every hosted service - the silo, and
/// with it the WAL commit-log drainer - has stopped. So the line's presence is
/// itself the discriminator an operator needs: if it appears in
/// <c>docker logs</c>, the drain completed; if the log ends without it, the
/// container was killed mid-drain and the grace period is too small. That makes
/// a crash teardown visible in the log stream rather than only in an exit code
/// that a later restart overwrites.
/// </para>
/// <para>
/// Both transitions are idempotent and latch on first call, so a duplicate
/// registration or a second lifetime callback cannot restart the clock or emit a
/// second, contradictory duration.
/// </para>
/// </remarks>
public sealed class RepoContextDrainSignal
{
    private readonly ILogger<RepoContextDrainSignal> _logger;
    private readonly Func<long> _timestamp;
    private readonly TimeSpan _shutdownBudget;
    private readonly Lock _gate = new();
    private long _startedAt;
    private bool _draining;
    private bool _completed;
    private TimeSpan? _elapsed;

    /// <summary>Initializes the drain signal.</summary>
    /// <param name="logger">The logger the drain lines are written to.</param>
    /// <param name="shutdownBudget">
    /// The host's own shutdown budget (<c>HostOptions.ShutdownTimeout</c>), reported
    /// alongside the measured duration so a reader can tell a drain that finished
    /// inside the budget from one the host itself abandoned.
    /// </param>
    /// <param name="timestamp">
    /// The monotonic timestamp source, defaulting to <see cref="Stopwatch.GetTimestamp"/>.
    /// Injectable so a test can measure a deterministic duration instead of a real one.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="logger"/> is null.</exception>
    public RepoContextDrainSignal(
        ILogger<RepoContextDrainSignal> logger,
        TimeSpan shutdownBudget,
        Func<long>? timestamp = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _shutdownBudget = shutdownBudget;
        _timestamp = timestamp ?? Stopwatch.GetTimestamp;
    }

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
    public bool HasCompleted
    {
        get { lock (_gate) { return _completed; } }
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
        lock (_gate)
        {
            if (_draining)
            {
                return;
            }

            _draining = true;
            _startedAt = _timestamp();
        }

        _logger.LogInformation(
            "RepoContext drain started: the silo will deactivate and the WAL commit-log will flush. "
            + "The host shutdown budget is {ShutdownBudgetSeconds:F0}s; the container's stop_grace_period "
            + "must exceed it or this drain is killed mid-flight.",
            _shutdownBudget.TotalSeconds);
    }

    /// <summary>
    /// Stops the drain clock and emits the measured duration. Idempotent: only the
    /// first call latches, so a duplicate lifetime callback cannot report a second,
    /// contradictory duration. A call that arrives without a preceding
    /// <see cref="BeginDrain"/> is ignored rather than reporting a duration measured
    /// from an unset start.
    /// </summary>
    public void CompleteDrain()
    {
        TimeSpan measured;
        lock (_gate)
        {
            if (!_draining || _completed)
            {
                return;
            }

            _completed = true;
            measured = Stopwatch.GetElapsedTime(_startedAt, _timestamp());
            _elapsed = measured;
        }

        _logger.LogInformation(
            "RepoContext drain complete in {DrainSeconds:F1}s (host shutdown budget {ShutdownBudgetSeconds:F0}s). "
            + "This is the measured teardown requirement: the container's stop_grace_period must exceed it, "
            + "and the absence of this line in a container log means the drain was killed before it finished.",
            measured.TotalSeconds,
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
}
