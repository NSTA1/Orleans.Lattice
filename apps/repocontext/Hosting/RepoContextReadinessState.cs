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
    private int _phase = (int)RepoContextLifecyclePhase.Starting;

    /// <summary>The current lifecycle phase.</summary>
    public RepoContextLifecyclePhase Phase => (RepoContextLifecyclePhase)Volatile.Read(ref _phase);

    /// <summary><see langword="true"/> only when the host is fully ready to serve.</summary>
    public bool IsReady => Phase == RepoContextLifecyclePhase.Ready;

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
    public void BeginDrain()
        => Volatile.Write(ref _phase, (int)RepoContextLifecyclePhase.Draining);
}
