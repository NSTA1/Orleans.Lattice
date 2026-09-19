namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// The container's lifecycle phase, driving the readiness probe. The host starts
/// in <see cref="Starting"/> (silo joining, activation-time WAL replay / cold
/// rebuild in progress, providers not yet proven reachable), transitions to
/// <see cref="Ready"/> once a warmup write has proven the durable stores are
/// reachable and the MCP surface is serving, and moves to <see cref="Draining"/>
/// at the very start of graceful shutdown so the readiness probe reports
/// not-ready before the silo begins to stop.
/// </summary>
public enum RepoContextLifecyclePhase
{
    /// <summary>Silo joining and replaying; not yet serving. Readiness is not-ready.</summary>
    Starting = 0,

    /// <summary>Silo joined, stores reachable, MCP serving. Readiness is ready.</summary>
    Ready = 1,

    /// <summary>Graceful shutdown in progress. Readiness is not-ready.</summary>
    Draining = 2,
}

/// <summary>
/// Thread-safe holder for the container's <see cref="RepoContextLifecyclePhase"/>,
/// shared between the readiness health check, the warmup seeder that flips the
/// host to <see cref="RepoContextLifecyclePhase.Ready"/>, and the shutdown hook
/// that flips it to <see cref="RepoContextLifecyclePhase.Draining"/>.
/// </summary>
public sealed class RepoContextReadinessState
{
    private readonly TimeProvider _timeProvider;
    private int _phase = (int)RepoContextLifecyclePhase.Starting;

    // The UTC instant the FIRST BeginDrain recorded, in UtcTicks; 0 means "not yet
    // draining". Written once (CompareExchange from 0) so a second BeginDrain - the
    // ApplicationStopping hook and StopAsync both call it - cannot move the origin
    // the drain-duration bound is measured from.
    private long _drainStartedAtTicks;

    /// <summary>Initializes the readiness state.</summary>
    /// <param name="timeProvider">
    /// The clock used to stamp the drain-start instant; defaults to
    /// <see cref="TimeProvider.System"/>. Injected so a test can drive the
    /// drain-duration bound deterministically.
    /// </param>
    public RepoContextReadinessState(TimeProvider? timeProvider = null)
        => _timeProvider = timeProvider ?? TimeProvider.System;

    /// <summary>The current lifecycle phase.</summary>
    public RepoContextLifecyclePhase Phase => (RepoContextLifecyclePhase)Volatile.Read(ref _phase);

    /// <summary><see langword="true"/> only when the host is fully ready to serve.</summary>
    public bool IsReady => Phase == RepoContextLifecyclePhase.Ready;

    /// <summary>
    /// The instant graceful shutdown began, or <see langword="null"/> while the host
    /// is not draining. Recorded on the first <see cref="BeginDrain"/> and never
    /// moved thereafter, so the silo health check can bound how long a drain has run
    /// and flag one that has hung rather than reporting it healthy indefinitely.
    /// </summary>
    public DateTimeOffset? DrainStartedAtUtc
    {
        get
        {
            var ticks = Volatile.Read(ref _drainStartedAtTicks);
            return ticks == 0L ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    /// <summary>
    /// Marks the host ready once startup replay is done and the durable stores are
    /// proven reachable. Ignored once draining has begun so a late warmup can never
    /// re-open readiness during shutdown.
    /// </summary>
    public void MarkReady()
        => Interlocked.CompareExchange(
            ref _phase,
            (int)RepoContextLifecyclePhase.Ready,
            (int)RepoContextLifecyclePhase.Starting);

    /// <summary>
    /// Flips the host into the draining phase at the start of graceful shutdown so
    /// readiness reports not-ready before the silo begins to stop. Terminal.
    /// </summary>
    /// <remarks>
    /// The drain-start instant is stamped <b>before</b> the phase is published, so any
    /// thread that observes <see cref="RepoContextLifecyclePhase.Draining"/> also
    /// observes a non-null <see cref="DrainStartedAtUtc"/>. Idempotent on the origin:
    /// the stamp is written only on the first call (both the
    /// <c>ApplicationStopping</c> hook and <c>StopAsync</c> call this), so a repeated
    /// drain cannot reset the clock the drain-duration bound measures from.
    /// </remarks>
    public void BeginDrain()
    {
        Interlocked.CompareExchange(ref _drainStartedAtTicks, _timeProvider.GetUtcNow().UtcTicks, 0L);
        Volatile.Write(ref _phase, (int)RepoContextLifecyclePhase.Draining);
    }
}
