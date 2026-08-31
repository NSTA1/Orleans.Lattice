using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Liveness probe: reports healthy while the process and silo host are alive. It
/// deliberately does not consult the readiness phase, so a draining or
/// still-replaying container is reported live (the process is up) even though it
/// is not yet, or no longer, ready to serve. An orchestrator uses this to decide
/// whether to restart the container, not whether to route traffic to it.
/// </summary>
public sealed class RepoContextLivenessHealthCheck : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "self";

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult(HealthCheckResult.Healthy("Process and silo host are alive."));
}

/// <summary>
/// Readiness probe: reports healthy only once the host has reached
/// <see cref="RepoContextLifecyclePhase.Ready"/> - the silo has joined, the
/// activation-time WAL replay / cold rebuild warmup has completed, the durable
/// stores were proven reachable, and the MCP surface is serving. It reports
/// not-ready during startup replay and again during drain, so an orchestrator
/// stops routing MCP traffic before the silo begins to stop.
/// </summary>
/// <param name="state">The shared lifecycle-phase holder.</param>
public sealed class RepoContextReadinessHealthCheck(RepoContextReadinessState state) : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "ready";

    private readonly RepoContextReadinessState _state = state
        ?? throw new ArgumentNullException(nameof(state));

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var phase = _state.Phase;
        var result = phase == RepoContextLifecyclePhase.Ready
            ? HealthCheckResult.Healthy("Silo joined, stores reachable, MCP serving.")
            : HealthCheckResult.Unhealthy($"Not ready: lifecycle phase is {phase}.");

        return Task.FromResult(result);
    }
}

/// <summary>
/// Readiness probe component for the <b>vector plane</b>: reports healthy only once
/// the host can serve the retrieval it is configured for. It exists because the
/// lifecycle-phase component alone reports fully ready as soon as the silo has joined
/// and its durable stores are writable - which a box happily does while its vector
/// plane is still replaying and cannot answer a single semantic query. Registered
/// under the readiness tag alongside
/// <see cref="RepoContextReadinessHealthCheck"/>, so <c>/health/ready</c> is the
/// conjunction of both and an orchestrator holds traffic back until semantic retrieval
/// actually works.
/// <para>
/// <b>Liveness is deliberately untouched.</b> A still-replaying box is alive and must
/// not be restarted; only readiness reflects the vector plane.
/// </para>
/// <para>
/// <b>It never deadlocks.</b> A host with no embedding provider bound reports
/// <see cref="RepoContextRetrievalReadinessPhase.KeywordOnly"/>, which is healthy:
/// keyword recall is that deployment's intended steady state, not a degradation.
/// </para>
/// <para>
/// <b>It never flaps.</b> The check is a pure reader of
/// <see cref="RepoContextRetrievalReadinessState"/>, whose fault hold-down keeps a
/// proven-serving plane ready across a transient fault.
/// </para>
/// </summary>
/// <param name="state">The shared vector-plane readiness state.</param>
public sealed class RepoContextRetrievalReadinessHealthCheck(RepoContextRetrievalReadinessState state) : IHealthCheck
{
    /// <summary>The health-check registration name.</summary>
    public const string Name = "retrieval";

    // Cached results: the probe runs on every orchestrator poll, so the steady-state
    // path allocates neither a result nor a Task.
    private static readonly Task<HealthCheckResult> Serving = Task.FromResult(
        HealthCheckResult.Healthy("Vector plane is serving semantic retrieval."));

    private static readonly Task<HealthCheckResult> KeywordOnly = Task.FromResult(
        HealthCheckResult.Healthy(
            "Keyword-only: no embedding provider is bound, so there is no vector plane to wait for."));

    private static readonly Task<HealthCheckResult> Building = Task.FromResult(
        HealthCheckResult.Unhealthy(
            "Not ready: the vector plane cannot serve semantic retrieval yet (still building, or unavailable)."));

    private readonly RepoContextRetrievalReadinessState _state = state
        ?? throw new ArgumentNullException(nameof(state));

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => _state.Phase switch
        {
            RepoContextRetrievalReadinessPhase.Serving => Serving,
            RepoContextRetrievalReadinessPhase.KeywordOnly => KeywordOnly,
            _ => Building,
        };
}
