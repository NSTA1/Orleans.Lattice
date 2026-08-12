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
