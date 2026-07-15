using System.Reflection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="PrometheusQueryClient"/>: it targets the configured
/// backend base address, stamps the configured <b>backend</b> credential on every
/// request, never carries a Lattice credential (there is no seam that could
/// forward one), and parses the backend envelopes. All requests are captured with
/// a fake <see cref="HttpMessageHandler"/> - no real network.
/// </summary>
[TestFixture]
public sealed class PrometheusQueryClientTests
{
    private const string BackendBase = "https://prometheus.internal:9090/";

    private static PrometheusQueryClient CreateClient(
        LatticeApiMcpTelemetryOptions options,
        out CapturingHttpMessageHandler handler,
        string responseJson = "{\"status\":\"success\",\"data\":{}}")
    {
        handler = new CapturingHttpMessageHandler(responseJson);
        var http = new HttpClient(handler) { BaseAddress = new Uri(BackendBase) };
        return new PrometheusQueryClient(http, Options.Create(options));
    }

    [Test]
    public async Task InstantQuery_targets_the_backend_query_endpoint()
    {
        var client = CreateClient(new LatticeApiMcpTelemetryOptions(), out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(handler.LastRequest!.RequestUri!.ToString(),
            Is.EqualTo($"{BackendBase}api/v1/query?query=up"));
    }

    [Test]
    public async Task RangeQuery_targets_the_backend_range_endpoint_with_range_parameters()
    {
        var client = CreateClient(new LatticeApiMcpTelemetryOptions(), out var handler);
        var start = DateTimeOffset.FromUnixTimeSeconds(1000);
        var end = DateTimeOffset.FromUnixTimeSeconds(2000);

        await client.RangeQueryAsync("up", start, end, TimeSpan.FromSeconds(30), CancellationToken.None);

        var uri = handler.LastRequest!.RequestUri!.ToString();
        Assert.Multiple(() =>
        {
            Assert.That(uri, Does.StartWith($"{BackendBase}api/v1/query_range?query=up"));
            Assert.That(uri, Does.Contain("start=1000"));
            Assert.That(uri, Does.Contain("end=2000"));
            Assert.That(uri, Does.Contain("step=30"));
        });
    }

    [Test]
    public async Task Bearer_mode_stamps_the_configured_backend_token()
    {
        var options = new LatticeApiMcpTelemetryOptions
        {
            AuthMode = LatticeTelemetryBackendAuthMode.Bearer,
            Credential = new LatticeTelemetryBackendCredential { BearerToken = "backend-secret" },
        };
        var client = CreateClient(options, out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        var auth = handler.LastRequest!.Headers.Authorization;
        Assert.Multiple(() =>
        {
            Assert.That(auth!.Scheme, Is.EqualTo("Bearer"));
            Assert.That(auth.Parameter, Is.EqualTo("backend-secret"));
        });
    }

    [Test]
    public async Task Basic_mode_stamps_the_configured_backend_basic_credential()
    {
        var options = new LatticeApiMcpTelemetryOptions
        {
            AuthMode = LatticeTelemetryBackendAuthMode.Basic,
            Credential = new LatticeTelemetryBackendCredential
            {
                BasicUsername = "svc",
                BasicPassword = "pw",
            },
        };
        var client = CreateClient(options, out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        var auth = handler.LastRequest!.Headers.Authorization;
        var expected = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("svc:pw"));
        Assert.Multiple(() =>
        {
            Assert.That(auth!.Scheme, Is.EqualTo("Basic"));
            Assert.That(auth.Parameter, Is.EqualTo(expected));
        });
    }

    [Test]
    public async Task None_mode_sends_no_authorization_header()
    {
        var client = CreateClient(new LatticeApiMcpTelemetryOptions(), out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(handler.LastRequest!.Headers.Authorization, Is.Null);
    }

    [Test]
    public async Task Mutual_tls_mode_sends_no_authorization_header()
    {
        var options = new LatticeApiMcpTelemetryOptions
        {
            AuthMode = LatticeTelemetryBackendAuthMode.MutualTls,
        };
        var client = CreateClient(options, out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(handler.LastRequest!.Headers.Authorization, Is.Null);
    }

    [Test]
    public async Task ListMetricNames_parses_the_backend_value_array()
    {
        var client = CreateClient(
            new LatticeApiMcpTelemetryOptions(),
            out var handler,
            "{\"status\":\"success\",\"data\":[\"up\",\"lattice_wal_append_total\"]}");

        var names = await client.ListMetricNamesAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(handler.LastRequest!.RequestUri!.ToString(),
                Is.EqualTo($"{BackendBase}api/v1/label/__name__/values"));
            Assert.That(names, Is.EqualTo(new[] { "up", "lattice_wal_append_total" }));
        });
    }

    [Test]
    public async Task MetricMetadata_for_a_named_metric_targets_the_metadata_endpoint()
    {
        var client = CreateClient(new LatticeApiMcpTelemetryOptions(), out var handler);

        var response = await client.MetricMetadataAsync("up", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(handler.LastRequest!.RequestUri!.ToString(),
                Is.EqualTo($"{BackendBase}api/v1/metadata?metric=up"));
            Assert.That(response.Status, Is.EqualTo("success"));
        });
    }

    [Test]
    public void Null_query_is_rejected()
    {
        var client = CreateClient(new LatticeApiMcpTelemetryOptions(), out _);
        Assert.ThrowsAsync<ArgumentNullException>(
            () => client.InstantQueryAsync(query: null!, time: null, CancellationToken.None));
    }

    [Test]
    public void The_client_depends_only_on_an_http_client_and_its_own_options()
    {
        var parameters = typeof(PrometheusQueryClient)
            .GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .Single()
            .GetParameters()
            .Select(p => p.ParameterType)
            .ToArray();

        Assert.That(
            parameters,
            Is.EqualTo(new[] { typeof(HttpClient), typeof(IOptions<LatticeApiMcpTelemetryOptions>) }),
            "The backend client must have no seam through which a caller's Lattice credential could reach the backend.");
    }
}
