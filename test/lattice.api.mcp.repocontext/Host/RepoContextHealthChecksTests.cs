using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the liveness and readiness health checks: liveness is always
/// healthy while the process is up, and readiness reflects the lifecycle phase
/// (not-ready during startup replay and during drain, healthy only when ready).
/// </summary>
[TestFixture]
public sealed class RepoContextHealthChecksTests
{
    private static readonly HealthCheckContext Context = new()
    {
        Registration = new HealthCheckRegistration(
            "test",
            new RepoContextLivenessHealthCheck(),
            HealthStatus.Unhealthy,
            tags: null),
    };

    [Test]
    public async Task Liveness_is_healthy_regardless_of_phase()
    {
        var check = new RepoContextLivenessHealthCheck();

        var result = await check.CheckHealthAsync(Context);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task Readiness_is_unhealthy_during_startup()
    {
        var state = new RepoContextReadinessState();
        var check = new RepoContextReadinessHealthCheck(state);

        var result = await check.CheckHealthAsync(Context);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
    }

    [Test]
    public async Task Readiness_is_healthy_once_ready()
    {
        var state = new RepoContextReadinessState();
        state.MarkReady();
        var check = new RepoContextReadinessHealthCheck(state);

        var result = await check.CheckHealthAsync(Context);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task Readiness_is_unhealthy_during_drain()
    {
        var state = new RepoContextReadinessState();
        state.MarkReady();
        state.BeginDrain();
        var check = new RepoContextReadinessHealthCheck(state);

        var result = await check.CheckHealthAsync(Context);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
    }

    [Test]
    public void Readiness_check_rejects_a_null_state()
        => Assert.That(() => new RepoContextReadinessHealthCheck(null!), Throws.ArgumentNullException);
}
