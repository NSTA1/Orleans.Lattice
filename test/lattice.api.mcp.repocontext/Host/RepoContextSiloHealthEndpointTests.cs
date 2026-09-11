using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// End-to-end HTTP tests of the real <c>/health/silo</c> endpoint over a TestServer,
/// mapped through the same <see cref="RepoContextSiloHealthEndpoint.CreateOptions"/>
/// factory the host uses. Unlike the isolated logic tests, these exercise the full
/// ASP.NET pipeline - the tag predicate, the three-way status-code mapping, and the
/// plain-text response writer - so a drift in the status codes or the wire body a
/// generic checker or the self-probe reads is caught. A fake probe stands in for the
/// grain call, driving each of the four states issue #2666 requires be demonstrated.
/// </summary>
[TestFixture]
public sealed class RepoContextSiloHealthEndpointTests
{
    private sealed class FixedProbe(Func<CancellationToken, Task> behaviour) : IRepoContextSiloProbe
    {
        public Task ProbeAsync(CancellationToken cancellationToken) => behaviour(cancellationToken);
    }

    private static async Task<(HttpStatusCode Status, string Body)> ProbeEndpointAsync(
        IRepoContextSiloProbe probe, RepoContextReadinessState readiness)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(probe);
        builder.Services.AddSingleton(readiness);
        builder.Services.AddHealthChecks()
            .AddCheck<RepoContextSiloHealthCheck>(
                RepoContextSiloHealthCheck.Name,
                tags: new[] { RepoContextHostBuilder.SiloTag });

        await using var app = builder.Build();
        app.MapHealthChecks(
            RepoContextHostBuilder.SiloPath,
            RepoContextSiloHealthEndpoint.CreateOptions(RepoContextHostBuilder.SiloTag));

        await app.StartAsync();
        try
        {
            using var client = app.GetTestClient();
            using var response = await client.GetAsync(RepoContextHostBuilder.SiloPath);
            var body = await response.Content.ReadAsStringAsync();
            return (response.StatusCode, body);
        }
        finally
        {
            await app.StopAsync();
        }
    }

    private static RepoContextReadinessState Ready()
    {
        var state = new RepoContextReadinessState();
        state.MarkReady();
        return state;
    }

    // State 4: healthy silo -> 200 and a "Healthy:" body.
    [Test]
    public async Task Healthy_silo_returns_200_and_a_healthy_body()
    {
        var (status, body) = await ProbeEndpointAsync(
            new FixedProbe(_ => Task.CompletedTask), Ready());

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(body, Does.StartWith("Healthy:"));
        });
    }

    // State 1: stopped silo (grain call cannot connect) -> 503 and an "Unhealthy:" body.
    [Test]
    public async Task Stopped_silo_returns_503_and_an_unhealthy_body()
    {
        var (status, body) = await ProbeEndpointAsync(
            new FixedProbe(_ => throw new InvalidOperationException("no active silo membership")),
            Ready());

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.ServiceUnavailable));
            Assert.That(body, Does.StartWith("Unhealthy:"));
            Assert.That(body, Does.Contain("no active silo membership"));
        });
    }

    // State 2: silo up, grain layer failing -> 503 and an "Unhealthy:" body.
    [Test]
    public async Task Failing_grain_layer_returns_503_and_an_unhealthy_body()
    {
        var (status, body) = await ProbeEndpointAsync(
            new FixedProbe(_ => throw new TimeoutException("grain activation could not be created")),
            Ready());

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.ServiceUnavailable));
            Assert.That(body, Does.StartWith("Unhealthy:"));
            Assert.That(body, Does.Contain("grain activation could not be created"));
        });
    }

    // State 3: silo still starting -> 503 and a "Degraded:" body (the self-probe reads
    // this as STARTING, distinct from Unhealthy).
    [Test]
    public async Task Starting_silo_returns_503_and_a_degraded_body()
    {
        var (status, body) = await ProbeEndpointAsync(
            new FixedProbe(_ => throw new InvalidOperationException("still joining the cluster")),
            new RepoContextReadinessState()); // never marked ready

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.ServiceUnavailable));
            Assert.That(body, Does.StartWith("Degraded:"),
                "A starting silo must be distinguishable in the body from a faulted one, or the self-probe "
                + "cannot tell STARTING from UNHEALTHY - which is what would crash-loop a normal boot.");
        });
    }
}
