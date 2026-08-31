using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the liveness and readiness health checks: liveness is always
/// healthy while the process is up, readiness reflects the lifecycle phase
/// (not-ready during startup replay and during drain, healthy only when ready),
/// and the vector-plane component reports on demonstrated semantic-retrieval
/// capability without flapping or wedging a keyword-only box.
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

    [Test]
    public void Retrieval_readiness_check_rejects_a_null_state()
        => Assert.That(
            () => new RepoContextRetrievalReadinessHealthCheck(null!), Throws.ArgumentNullException);

    [Test]
    public void Health_check_registration_names_are_distinct()
        => Assert.That(
            new[]
            {
                RepoContextLivenessHealthCheck.Name,
                RepoContextReadinessHealthCheck.Name,
                RepoContextRetrievalReadinessHealthCheck.Name,
            },
            Is.Unique,
            "Two health checks sharing a registration name would collide in the probe endpoint.");

    [Test]
    public async Task Retrieval_readiness_is_unhealthy_while_the_vector_plane_is_unavailable()
    {
        using var state = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        var check = new RepoContextRetrievalReadinessHealthCheck(state);

        var result = await check.CheckHealthAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy),
                "A box configured for semantic retrieval must not report ready before it can serve one.");
            Assert.That(result.Description, Does.Contain("vector plane"));
        });
    }

    [Test]
    public async Task Retrieval_readiness_is_healthy_once_the_plane_serves()
    {
        using var state = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        state.MarkServing();
        var check = new RepoContextRetrievalReadinessHealthCheck(state);

        var result = await check.CheckHealthAsync(Context);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task Retrieval_readiness_is_healthy_in_a_keyword_only_configuration()
    {
        using var state = new RepoContextRetrievalReadinessState(new SettableTimeProvider());
        state.MarkKeywordOnly();
        var check = new RepoContextRetrievalReadinessHealthCheck(state);

        var result = await check.CheckHealthAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy),
                "A box with no embedder bound is legitimately ready in keyword-only mode.");
            Assert.That(result.Description, Does.Contain("Keyword-only"));
        });
    }

    [Test]
    public async Task Retrieval_readiness_does_not_oscillate_across_a_transient_fault()
    {
        var clock = new SettableTimeProvider();
        using var state = new RepoContextRetrievalReadinessState(clock, TimeSpan.FromSeconds(30));
        state.MarkServing();
        var check = new RepoContextRetrievalReadinessHealthCheck(state);
        var liveness = new RepoContextLivenessHealthCheck();

        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);

        // Poll the probes the way an orchestrator would while the fault is in flight.
        for (var i = 0; i < 5; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            var readiness = await check.CheckHealthAsync(Context);
            var live = await liveness.CheckHealthAsync(Context);

            Assert.Multiple(() =>
            {
                Assert.That(readiness.Status, Is.EqualTo(HealthStatus.Healthy),
                    $"Readiness flapped at poll {i} inside the fault hold-down window.");
                Assert.That(live.Status, Is.EqualTo(HealthStatus.Healthy),
                    "Liveness must stay healthy throughout: a replaying box is alive and must not be restarted.");
            });
        }

        state.MarkServing();
        clock.Advance(TimeSpan.FromMinutes(5));

        Assert.That(
            (await check.CheckHealthAsync(Context)).Status,
            Is.EqualTo(HealthStatus.Healthy),
            "A fault that cleared inside the window must leave readiness untouched.");
    }

    [Test]
    public async Task Liveness_stays_healthy_while_retrieval_readiness_is_unhealthy()
    {
        var clock = new SettableTimeProvider();
        using var state = new RepoContextRetrievalReadinessState(clock, TimeSpan.FromSeconds(30));
        state.MarkServing();
        state.MarkUnavailable(RepoContextRetrievalPath.KeywordVectorPlaneUnavailable);
        clock.Advance(TimeSpan.FromSeconds(30));

        var readiness = await new RepoContextRetrievalReadinessHealthCheck(state).CheckHealthAsync(Context);
        var live = await new RepoContextLivenessHealthCheck().CheckHealthAsync(Context);

        Assert.Multiple(() =>
        {
            Assert.That(readiness.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(live.Status, Is.EqualTo(HealthStatus.Healthy));
        });
    }
}
