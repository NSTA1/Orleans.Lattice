using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for the container's <c>--healthcheck</c> self-probe classification and
/// the <c>/health/silo</c> response writer. The runtime image is shell-less, so the
/// probe is the host binary re-invoked against itself; it must map the endpoint's
/// three-valued body to a stable token and a Docker-actionable exit code, and treat
/// every ambiguous or unreachable answer as unhealthy rather than as healthy.
/// </summary>
[TestFixture]
public sealed class RepoContextHealthProbeTests
{
    [Test]
    public void Classify_reads_a_healthy_body_as_healthy()
        => Assert.That(
            RepoContextHealthProbe.Classify("Healthy: silo membership active and the grain layer answered."),
            Is.EqualTo(RepoContextHealthProbe.Verdict.Healthy));

    [Test]
    public void Classify_reads_a_degraded_body_as_starting()
        => Assert.That(
            RepoContextHealthProbe.Classify("Degraded: Starting: the silo has not yet answered a grain call."),
            Is.EqualTo(RepoContextHealthProbe.Verdict.Starting));

    [Test]
    public void Classify_reads_an_unhealthy_body_as_unhealthy()
        => Assert.That(
            RepoContextHealthProbe.Classify("Unhealthy: the silo is not answering grain calls."),
            Is.EqualTo(RepoContextHealthProbe.Verdict.Unhealthy));

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("Service Unavailable")]
    [TestCase("<html>502 Bad Gateway</html>")]
    public void Classify_treats_an_absent_or_unrecognised_body_as_unhealthy(string? body)
        => Assert.That(
            RepoContextHealthProbe.Classify(body),
            Is.EqualTo(RepoContextHealthProbe.Verdict.Unhealthy),
            "An ambiguous answer must never read as healthy; that is how a dead silo stays green.");

    [Test]
    public void Classify_tolerates_leading_whitespace()
        => Assert.That(
            RepoContextHealthProbe.Classify("  Healthy: ok"),
            Is.EqualTo(RepoContextHealthProbe.Verdict.Healthy));

    [TestCase("8080", ExpectedResult = 8080)]
    [TestCase("9000", ExpectedResult = 9000)]
    [TestCase(null, ExpectedResult = 8080)]
    [TestCase("", ExpectedResult = 8080)]
    [TestCase("not-a-port", ExpectedResult = 8080)]
    [TestCase("0", ExpectedResult = 8080)]
    [TestCase("-1", ExpectedResult = 8080)]
    public int ResolvePort_parses_or_falls_back_to_the_default(string? raw)
        => RepoContextHealthProbe.ResolvePort(raw);

    [Test]
    public void ResolvePort_default_matches_the_host_default_port()
        => Assert.That(
            RepoContextHealthProbe.ResolvePort(null),
            Is.EqualTo(RepoContextHostConfiguration.DefaultMcpPort),
            "A drifted probe default would target the wrong port and report a healthy silo as unreachable.");

    [Test]
    public async Task Response_writer_emits_the_status_and_description_on_one_line()
    {
        var context = new DefaultHttpContext();
        using var body = new MemoryStream();
        context.Response.Body = body;

        var report = new HealthReport(
            new Dictionary<string, HealthReportEntry>
            {
                [RepoContextSiloHealthCheck.Name] = new HealthReportEntry(
                    HealthStatus.Unhealthy,
                    "The silo is not answering grain calls (connection refused).",
                    TimeSpan.Zero,
                    exception: null,
                    data: null),
            },
            TimeSpan.Zero);

        await RepoContextSiloHealthResponse.Write(context, report);

        var text = Encoding.UTF8.GetString(body.ToArray());
        Assert.Multiple(() =>
        {
            Assert.That(text, Does.StartWith("Unhealthy: "),
                "The leading status word is what the shell-less self-probe classifies on.");
            Assert.That(text, Does.Contain("connection refused"),
                "The reason must reach the Docker health log an operator reads with docker inspect.");
            Assert.That(context.Response.ContentType, Does.Contain("text/plain"));
        });
    }

    [Test]
    public void Response_writer_rejects_a_null_context()
        => Assert.That(
            () => RepoContextSiloHealthResponse.Write(
                null!,
                new HealthReport(new Dictionary<string, HealthReportEntry>(), TimeSpan.Zero)),
            Throws.ArgumentNullException);

    [Test]
    public void Response_writer_rejects_a_null_report()
        => Assert.That(
            () => RepoContextSiloHealthResponse.Write(new DefaultHttpContext(), null!),
            Throws.ArgumentNullException);
}
