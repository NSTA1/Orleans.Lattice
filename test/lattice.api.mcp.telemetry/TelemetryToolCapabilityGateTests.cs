using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Regression tests for the call-time cluster-telemetry capability check every
/// <see cref="TelemetryToolHandlers"/> method performs.
/// </summary>
/// <remarks>
/// <para>
/// Before this check existed the MCP telemetry tools were gated at <b>discovery</b>
/// alone: the permission resolver decided whether to advertise the
/// <c>lattice_telemetry_*</c> tools, and the handlers themselves consulted only the
/// metric-name access policy, whose default <c>ReadAll</c> posture admits every
/// name. MCP tool names are not secrets - they are stable, published, and
/// guessable - so a caller holding any other facade group could invoke a telemetry
/// tool directly by name, supply arbitrary PromQL, and read cluster-wide metrics it
/// was never granted.
/// </para>
/// <para>
/// These tests pin the three properties that close it: a denied caller gets a
/// structured refusal on every one of the four tools; a filtered allow is a
/// refusal too (cluster telemetry is attached to no key, so a per-key filter has
/// not authorized the scope); and the refusal happens <b>before</b> the backend is
/// touched, so an unauthorized call is not merely unanswered but never issued.
/// </para>
/// </remarks>
[TestFixture]
public sealed class TelemetryToolCapabilityGateTests
{
    private const string VectorJson =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}";

    private static PrometheusQueryClient Client(out CapturingHttpMessageHandler handler)
    {
        handler = new CapturingHttpMessageHandler(VectorJson, System.Net.HttpStatusCode.OK);
        var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://prometheus.internal:9090/"),
        };
        return new PrometheusQueryClient(http, Options.Create(new LatticeApiMcpTelemetryOptions()));
    }

    private static TelemetryMetricAccessPolicy ReadAll()
        => new(new LatticeApiMcpTelemetryOptions());

    private static IOptions<LatticeApiMcpTelemetryOptions> Guardrails()
        => Options.Create(new LatticeApiMcpTelemetryOptions
        {
            MaxRange = TimeSpan.FromHours(24),
            MaxStep = TimeSpan.FromHours(1),
        });

    [Test]
    public async Task Query_without_the_telemetry_capability_is_refused()
    {
        var client = Client(out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, ReadAll(), TelemetryAuthorizers.Denied(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("Telemetry capability"));
            Assert.That(handler.RequestCount, Is.Zero, "The backend must not be reached at all.");
        });
    }

    [Test]
    public async Task Query_range_without_the_telemetry_capability_is_refused()
    {
        var client = Client(out var handler);
        var start = DateTimeOffset.UnixEpoch;

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client,
            ReadAll(),
            TelemetryAuthorizers.Denied(),
            Guardrails(),
            CancellationToken.None,
            "up",
            start,
            start.AddMinutes(5),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("Telemetry capability"));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task Query_range_refuses_before_the_range_guardrails_run()
    {
        // An over-budget range would otherwise be rejected with a message naming
        // the configured maximum, which would let an unauthorized caller read the
        // cluster's range budget back out of the refusal.
        var client = Client(out _);
        var start = DateTimeOffset.UnixEpoch;

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client,
            ReadAll(),
            TelemetryAuthorizers.Denied(),
            Guardrails(),
            CancellationToken.None,
            "up",
            start,
            start.AddDays(365),
            TimeSpan.FromSeconds(1));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("Telemetry capability"));
        });
    }

    [Test]
    public async Task List_metrics_without_the_telemetry_capability_is_refused()
    {
        var client = Client(out var handler);

        var result = await TelemetryToolHandlers.ListMetricsAsync(
            client, ReadAll(), TelemetryAuthorizers.Denied(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("Telemetry capability"));
            Assert.That(result.Metrics, Is.Empty);
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task Metric_metadata_without_the_telemetry_capability_is_refused()
    {
        var client = Client(out var handler);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), TelemetryAuthorizers.Denied(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("Telemetry capability"));
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_key_filtered_allow_does_not_authorize_cluster_telemetry()
    {
        var client = Client(out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, ReadAll(), TelemetryAuthorizers.Filtered(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(handler.RequestCount, Is.Zero);
        });
    }

    [Test]
    public async Task An_authorization_off_cluster_is_unaffected()
    {
        // No gate registered at all is the authorization-off posture, and it must
        // stay byte-for-byte on its pre-check behaviour.
        var client = Client(out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, ReadAll(), TelemetryAuthorizers.Allowed(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(handler.RequestCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_denial_never_echoes_the_gate_reason_or_the_subject()
    {
        // The exception carries the resolved subject id and the gate's own reason
        // text; neither is a caller-owned value, so neither may cross back.
        var client = Client(out _);

        var result = TelemetryToolHandlers
            .QueryAsync(client, ReadAll(), TelemetryAuthorizers.Denied(), CancellationToken.None, "up")
            .GetAwaiter()
            .GetResult();

        Assert.Multiple(() =>
        {
            Assert.That(result.Error, Does.Not.Contain("does not hold"));
            Assert.That(result.Error, Does.Not.Contain("anonymous").IgnoreCase);
        });
    }
}
