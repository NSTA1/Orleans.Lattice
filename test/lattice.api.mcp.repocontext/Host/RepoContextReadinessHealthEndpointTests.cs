using Orleans.Lattice.Testing.Hygiene;
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
/// End-to-end HTTP tests of the real <c>/health/ready</c> endpoint over a TestServer,
/// mapped through the same <see cref="RepoContextReadinessHealthEndpoint.CreateOptions"/>
/// factory the host uses, so a drift between the mapping under test and the one the
/// host serves is caught.
/// </summary>
/// <remarks>
/// The property under test is <b>attributability</b>, not merely that a body exists.
/// Issue #2962 records an acceptance run that reported a container healthy through a
/// total retrieval outage; the readiness endpoint knew better and said so, but the
/// endpoint carried no response writer, so every component description was dropped and
/// the wire body was the aggregate status word alone. A reading of <c>Unhealthy</c>
/// cannot distinguish a replaying lifecycle from a degraded vector plane, and those
/// have different remedies.
/// <para>
/// <see cref="Two_different_readiness_failures_are_distinguishable_on_the_wire"/> is
/// the assertion that fails if the response writer is removed: without it both
/// failures serialise to the identical string. A test that only asserted "the body is
/// non-empty" would pass on the defect, which is precisely how the defect survived.
/// </para>
/// </remarks>
[TestFixture]
[FastInProcessHostFixture("Builds a WebApplication in-process with no silo or storage; measured at 167 ms for 7 tests, below the 5-second threshold.")]
public sealed class RepoContextReadinessHealthEndpointTests
{
    private static async Task<(HttpStatusCode Status, string Body)> ProbeAsync(
        RepoContextReadinessState lifecycle,
        RepoContextRetrievalReadinessState retrieval)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(lifecycle);
        builder.Services.AddSingleton(retrieval);
        builder.Services.AddHealthChecks()
            .AddCheck<RepoContextReadinessHealthCheck>(
                RepoContextReadinessHealthCheck.Name,
                tags: new[] { RepoContextHostBuilder.ReadinessTag })
            .AddCheck<RepoContextRetrievalReadinessHealthCheck>(
                RepoContextRetrievalReadinessHealthCheck.Name,
                tags: new[] { RepoContextHostBuilder.ReadinessTag });

        await using var app = builder.Build();
        app.MapHealthChecks(
            RepoContextHostBuilder.ReadinessPath,
            RepoContextReadinessHealthEndpoint.CreateOptions(RepoContextHostBuilder.ReadinessTag));

        await app.StartAsync();
        try
        {
            using var client = app.GetTestClient();
            using var response = await client.GetAsync(RepoContextHostBuilder.ReadinessPath);
            return (response.StatusCode, await response.Content.ReadAsStringAsync());
        }
        finally
        {
            await app.StopAsync();
        }
    }

    private static RepoContextReadinessState ReadyLifecycle()
    {
        var state = new RepoContextReadinessState();
        state.MarkReady();
        return state;
    }

    private static RepoContextRetrievalReadinessState ServingRetrieval()
    {
        var state = new RepoContextRetrievalReadinessState(TimeProvider.System);
        state.MarkServing();
        return state;
    }

    // A fresh state sits in Building, which is the not-ready phase.
    private static RepoContextRetrievalReadinessState BuildingRetrieval()
        => new(TimeProvider.System);

    [Test]
    public async Task A_fully_ready_host_returns_200_and_names_every_component()
    {
        using var retrieval = ServingRetrieval();

        var (status, body) = await ProbeAsync(ReadyLifecycle(), retrieval);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.OK));
            Assert.That(body, Does.StartWith("Healthy"));
            Assert.That(body, Does.Contain(RepoContextReadinessHealthCheck.Name));
            Assert.That(body, Does.Contain(RepoContextRetrievalReadinessHealthCheck.Name));
        });
    }

    [Test]
    public async Task A_degraded_vector_plane_returns_503_and_says_so_on_the_wire()
    {
        using var retrieval = BuildingRetrieval();

        var (status, body) = await ProbeAsync(ReadyLifecycle(), retrieval);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.ServiceUnavailable));

            // The component, its verdict, and the discriminating detail the check
            // authored. The retrievalPath token is the specific string an operator
            // needs to tell a withheld exact fallback from an unavailable plane, and
            // it was being computed and discarded on every probe.
            Assert.That(body, Does.Contain(RepoContextRetrievalReadinessHealthCheck.Name));
            Assert.That(body, Does.Contain("vector plane"));
            Assert.That(body, Does.Contain("exact_fallback_suppressed"));

            // The healthy component is still reported, so a reader can see that the
            // lifecycle is NOT the cause rather than having to infer it.
            Assert.That(body, Does.Contain("Silo joined"));
        });
    }

    [Test]
    public async Task A_not_ready_lifecycle_returns_503_and_names_the_phase()
    {
        using var retrieval = ServingRetrieval();

        var (status, body) = await ProbeAsync(new RepoContextReadinessState(), retrieval);

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(HttpStatusCode.ServiceUnavailable));
            Assert.That(body, Does.Contain("lifecycle phase"));
            Assert.That(body, Does.Contain("Vector plane is serving"));
        });
    }

    /// <summary>
    /// The anti-vacuity assertion. Remove the response writer from
    /// <see cref="RepoContextReadinessHealthEndpoint.CreateOptions"/> and both bodies
    /// collapse to the single word "Unhealthy", so this fails while every
    /// status-code assertion above still passes. That asymmetry is the defect issue
    /// #2962 records: the verdict was right and the attribution was absent.
    /// </summary>
    [Test]
    public async Task Two_different_readiness_failures_are_distinguishable_on_the_wire()
    {
        using var serving = ServingRetrieval();
        using var building = BuildingRetrieval();

        var (lifecycleStatus, lifecycleBody) = await ProbeAsync(new RepoContextReadinessState(), serving);
        var (retrievalStatus, retrievalBody) = await ProbeAsync(ReadyLifecycle(), building);

        Assert.Multiple(() =>
        {
            Assert.That(lifecycleStatus, Is.EqualTo(HttpStatusCode.ServiceUnavailable));
            Assert.That(retrievalStatus, Is.EqualTo(HttpStatusCode.ServiceUnavailable));

            Assert.That(
                lifecycleBody,
                Is.Not.EqualTo(retrievalBody),
                "Two readiness failures with entirely different causes and remedies must not serialise to "
                + "the same bytes. The status code cannot separate them - both are 503 - so if the body "
                + "cannot either, a captured evidence set records only THAT the box was unready, never "
                + "WHICH component was. That is issue #2962.");
        });
    }

    /// <summary>
    /// Pins the wire contract the acceptance predicate and any orchestrator read. The
    /// point of this change was to add detail to the body without moving the verdict,
    /// so the status-code mapping must stay at the framework default and the first
    /// line must stay exactly the aggregate status word.
    /// </summary>
    [Test]
    public async Task The_status_code_and_first_line_are_unchanged_by_the_added_detail()
    {
        using var retrieval = BuildingRetrieval();

        var (status, body) = await ProbeAsync(ReadyLifecycle(), retrieval);
        var firstLine = body.Split('\n')[0].Trim();

        Assert.Multiple(() =>
        {
            Assert.That((int)status, Is.EqualTo(503));
            Assert.That(
                firstLine,
                Is.EqualTo("Unhealthy"),
                "A consumer that compared the whole body against the aggregate status word must keep "
                + "working; the detail is appended below it, never in place of it.");

            Assert.That(
                RepoContextReadinessHealthEndpoint.CreateOptions(RepoContextHostBuilder.ReadinessTag)
                    .ResultStatusCodes[HealthStatus.Unhealthy],
                Is.EqualTo(503));
            Assert.That(
                RepoContextReadinessHealthEndpoint.CreateOptions(RepoContextHostBuilder.ReadinessTag)
                    .ResultStatusCodes[HealthStatus.Healthy],
                Is.EqualTo(200));
        });
    }

    /// <summary>
    /// One component must occupy exactly one line. A description carrying a newline
    /// would otherwise split into what reads as an extra component and corrupt a
    /// line-oriented parse of the body - a false reading produced by the very change
    /// meant to make the body readable.
    /// </summary>
    [Test]
    public async Task Each_component_occupies_exactly_one_line()
    {
        using var retrieval = BuildingRetrieval();

        var (_, body) = await ProbeAsync(ReadyLifecycle(), retrieval);
        var lines = body.Split('\n');

        Assert.Multiple(() =>
        {
            // One aggregate line plus one line per registered readiness component.
            Assert.That(lines, Has.Length.EqualTo(3));
            Assert.That(lines[1], Does.Contain(": "));
            Assert.That(lines[2], Does.Contain(": "));
        });
    }

    [Test]
    public void The_writer_rejects_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(
                () => RepoContextComponentHealthResponse.Write(
                    null!,
                    new HealthReport(
                        new Dictionary<string, HealthReportEntry>(StringComparer.Ordinal),
                        TimeSpan.Zero)));

            Assert.ThrowsAsync<ArgumentNullException>(
                () => RepoContextComponentHealthResponse.Write(
                    new Microsoft.AspNetCore.Http.DefaultHttpContext(), null!));

            Assert.Throws<ArgumentException>(
                () => RepoContextReadinessHealthEndpoint.CreateOptions(string.Empty));
        });
}
