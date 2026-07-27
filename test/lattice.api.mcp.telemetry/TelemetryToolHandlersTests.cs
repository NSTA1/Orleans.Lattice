using System.Net;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="TelemetryToolHandlers"/>: each handler projects its
/// Prometheus response shape (vector, matrix, scalar, label-values, metadata) into
/// a structured result; the metric-access policy filters or rejects a
/// non-admitted metric in the deny-all posture while the read-all posture passes
/// it; the range guardrails reject an over-budget request; and a backend fault
/// surfaces as a clean structured error rather than an unhandled exception. All
/// backend traffic is served by a fake <see cref="HttpMessageHandler"/>.
/// </summary>
[TestFixture]
public sealed class TelemetryToolHandlersTests
{
    private const string BackendBase = "https://prometheus.internal:9090/";

    private static PrometheusQueryClient Client(
        string responseJson,
        out CapturingHttpMessageHandler handler,
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        handler = new CapturingHttpMessageHandler(responseJson, statusCode);
        var http = new HttpClient(handler) { BaseAddress = new Uri(BackendBase) };
        return new PrometheusQueryClient(http, Options.Create(new LatticeApiMcpTelemetryOptions()));
    }

    private static TelemetryMetricAccessPolicy ReadAll()
        => new(new LatticeApiMcpTelemetryOptions());

    private static TelemetryMetricAccessPolicy DenyAll(params string[] allowed)
    {
        var options = new LatticeApiMcpTelemetryOptions
        {
            MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
        };
        foreach (var entry in allowed)
        {
            options.AllowedMetrics.Add(entry);
        }

        return new TelemetryMetricAccessPolicy(options);
    }

    private static IOptions<LatticeApiMcpTelemetryOptions> Guardrails(
        TimeSpan? maxRange = null,
        TimeSpan? maxStep = null)
        => Options.Create(new LatticeApiMcpTelemetryOptions
        {
            MaxRange = maxRange ?? TimeSpan.FromHours(24),
            MaxStep = maxStep ?? TimeSpan.FromHours(1),
        });

    // ---- Instant query (vector / scalar) ----

    [Test]
    public async Task Query_projects_a_vector_result()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\",\"job\":\"api\"},\"value\":[1435781451.781,\"1\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Error, Is.Null);
            Assert.That(result.ResultType, Is.EqualTo("vector"));
            Assert.That(result.Series, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Labels["__name__"], Is.EqualTo("up"));
            Assert.That(result.Series[0].Labels["job"], Is.EqualTo("api"));
            Assert.That(result.Series[0].Samples, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Samples[0].Timestamp, Is.EqualTo(1435781451.781).Within(1e-6));
            Assert.That(result.Series[0].Samples[0].Value, Is.EqualTo("1"));
        });
    }

    [Test]
    public async Task Query_projects_a_scalar_result_as_a_single_labelless_series()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"scalar\",\"result\":[1435781451.781,\"42\"]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "42");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResultType, Is.EqualTo("scalar"));
            Assert.That(result.Series, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Labels, Is.Empty);
            Assert.That(result.Series[0].Samples[0].Value, Is.EqualTo("42"));
        });
    }

    [Test]
    public async Task Query_in_deny_all_rejects_a_non_whitelisted_metric_without_calling_the_backend()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, DenyAll("lattice_wal_append_total"), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("up"));
            Assert.That(handler.LastRequest, Is.Null, "A denied query must not reach the backend.");
        });
    }

    [Test]
    public async Task Query_in_deny_all_passes_a_whitelisted_metric()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"lattice_wal_append_total\"},\"value\":[1.0,\"5\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, DenyAll("lattice_wal_append_total"), CancellationToken.None, "lattice_wal_append_total");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Series[0].Samples[0].Value, Is.EqualTo("5"));
        });
    }

    [Test]
    public async Task Query_in_deny_all_rejects_a_name_matcher_naming_a_denied_metric()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, DenyAll("lattice_wal_append_total"), CancellationToken.None, "{__name__=\"secret_metric\"}");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("secret_metric"));
            Assert.That(handler.LastRequest, Is.Null, "A denied query must not reach the backend.");
        });
    }

    [Test]
    public async Task Query_in_deny_all_admits_a_name_matcher_naming_an_allowed_metric()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"lattice_wal_append_total\"},\"value\":[1.0,\"5\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(
            client,
            DenyAll("lattice_wal_append_total"),
            CancellationToken.None,
            "{__name__=\"lattice_wal_append_total\"}");

        Assert.That(result.Success, Is.True);
    }

    [Test]
    public async Task Query_in_deny_all_rejects_a_regex_name_matcher()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, DenyAll("secret_metric"), CancellationToken.None, "{__name__=~\"secret_.*\"}");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.Not.Null.And.Not.Empty);
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task Query_in_deny_all_rejects_a_label_only_selector_with_no_metric_name()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, DenyAll("up"), CancellationToken.None, "{job=\"api\"}");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.Not.Null.And.Not.Empty);
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task Query_in_read_all_admits_a_name_matcher_deny_all_would_reject()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"secret_metric\"},\"value\":[1.0,\"1\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, ReadAll(), CancellationToken.None, "{__name__=~\"secret_.*\"}");

        Assert.That(result.Success, Is.True);
    }

    [Test]
    public async Task QueryRange_in_deny_all_rejects_a_name_matcher_naming_a_denied_metric()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, DenyAll("lattice_wal_append_total"), Guardrails(), CancellationToken.None,
            "{__name__=\"secret_metric\"}",
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeSeconds(600),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("secret_metric"));
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task QueryRange_in_deny_all_rejects_a_label_only_selector_with_no_metric_name()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, DenyAll("up"), Guardrails(), CancellationToken.None,
            "{job=\"api\"}",
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeSeconds(600),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task Query_in_read_all_passes_a_metric_that_deny_all_would_reject()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"value\":[1.0,\"1\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.That(result.Success, Is.True);
    }

    [Test]
    public async Task Query_surfaces_a_backend_http_failure_as_a_clean_error()
    {
        var client = Client("{}", out _, HttpStatusCode.InternalServerError);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.Not.Null.And.Not.Empty);
            Assert.That(result.Series, Is.Empty);
        });
    }

    [Test]
    public async Task Query_surfaces_a_non_success_backend_status_as_a_clean_error()
    {
        const string json = "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"parse error\"}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("error"));
        });
    }

    [Test]
    public async Task Query_surfaces_a_malformed_payload_as_a_clean_error()
    {
        var client = Client("{ this is not json", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.That(result.Success, Is.False);
    }

    [Test]
    public void Query_rejects_a_null_query()
    {
        var client = Client("{}", out _);
        Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, query: null!));
    }

    [Test]
    public void Query_rejects_a_null_client()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.QueryAsync(client: null!, ReadAll(), CancellationToken.None, "up"));

    [Test]
    public void Query_propagates_a_genuine_caller_cancellation()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            () => TelemetryToolHandlers.QueryAsync(client, ReadAll(), cts.Token, "up"));
    }

    // ---- Range query (matrix) ----

    [Test]
    public async Task QueryRange_projects_a_matrix_result()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"values\":[[1.0,\"1\"],[2.0,\"1\"],[3.0,\"0\"]]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, ReadAll(), Guardrails(), CancellationToken.None,
            "up",
            DateTimeOffset.FromUnixTimeSeconds(1000),
            DateTimeOffset.FromUnixTimeSeconds(2000),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResultType, Is.EqualTo("matrix"));
            Assert.That(result.Series, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Samples, Has.Count.EqualTo(3));
            Assert.That(result.Series[0].Samples[2].Value, Is.EqualTo("0"));
        });
    }

    [Test]
    public async Task QueryRange_rejects_a_range_exceeding_the_maximum_without_calling_the_backend()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, ReadAll(), Guardrails(maxRange: TimeSpan.FromHours(1)), CancellationToken.None,
            "up",
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeSeconds(0) + TimeSpan.FromHours(2),
            TimeSpan.FromMinutes(1));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("range"));
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task QueryRange_rejects_a_step_exceeding_the_maximum()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, ReadAll(), Guardrails(maxStep: TimeSpan.FromMinutes(5)), CancellationToken.None,
            "up",
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeSeconds(600),
            TimeSpan.FromMinutes(10));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("step"));
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task QueryRange_rejects_an_end_before_start()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, ReadAll(), Guardrails(), CancellationToken.None,
            "up",
            DateTimeOffset.FromUnixTimeSeconds(2000),
            DateTimeOffset.FromUnixTimeSeconds(1000),
            TimeSpan.FromSeconds(30));

        Assert.That(result.Success, Is.False);
    }

    [Test]
    public async Task QueryRange_rejects_a_non_positive_step()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, ReadAll(), Guardrails(), CancellationToken.None,
            "up",
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeSeconds(600),
            TimeSpan.Zero);

        Assert.That(result.Success, Is.False);
    }

    [Test]
    public async Task QueryRange_in_deny_all_rejects_a_non_whitelisted_metric()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, DenyAll("lattice_wal_append_total"), Guardrails(), CancellationToken.None,
            "up",
            DateTimeOffset.FromUnixTimeSeconds(0),
            DateTimeOffset.FromUnixTimeSeconds(600),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public void QueryRange_rejects_a_null_options()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.QueryRangeAsync(
                Client("{}", out _), ReadAll(), options: null!, CancellationToken.None,
                "up", DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch, TimeSpan.FromSeconds(1)));

    // ---- List metrics ----

    [Test]
    public async Task ListMetrics_in_read_all_returns_every_name()
    {
        const string json = "{\"status\":\"success\",\"data\":[\"up\",\"lattice_wal_append_total\",\"lattice_shard_count\"]}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.ListMetricsAsync(client, ReadAll(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Is.EqualTo(new[] { "up", "lattice_wal_append_total", "lattice_shard_count" }));
        });
    }

    [Test]
    public async Task ListMetrics_in_deny_all_filters_to_admitted_names()
    {
        const string json = "{\"status\":\"success\",\"data\":[\"up\",\"lattice_wal_append_total\",\"lattice_shard_count\"]}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.ListMetricsAsync(
            client, DenyAll("lattice_wal_*"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Is.EqualTo(new[] { "lattice_wal_append_total" }));
        });
    }

    [Test]
    public async Task ListMetrics_surfaces_a_backend_failure_as_a_clean_error()
    {
        var client = Client("{}", out _, HttpStatusCode.ServiceUnavailable);

        var result = await TelemetryToolHandlers.ListMetricsAsync(client, ReadAll(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Metrics, Is.Empty);
        });
    }

    [Test]
    public void ListMetrics_rejects_a_null_client()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.ListMetricsAsync(client: null!, ReadAll(), CancellationToken.None));

    // ---- Metric metadata ----

    [Test]
    public async Task MetricMetadata_projects_the_metadata_entries()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"up\":[{\"type\":\"gauge\",\"help\":\"up help\",\"unit\":\"\"}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Has.Count.EqualTo(1));
            Assert.That(result.Metrics[0].Metric, Is.EqualTo("up"));
            Assert.That(result.Metrics[0].Type, Is.EqualTo("gauge"));
            Assert.That(result.Metrics[0].Help, Is.EqualTo("up help"));
        });
    }

    [Test]
    public async Task MetricMetadata_in_deny_all_rejects_a_non_whitelisted_named_metric_without_calling_the_backend()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, DenyAll("up"), CancellationToken.None, "lattice_wal_append_total");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("lattice_wal_append_total"));
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task MetricMetadata_in_deny_all_filters_the_listing_to_admitted_names()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{"
            + "\"up\":[{\"type\":\"gauge\",\"help\":\"up\",\"unit\":\"\"}],"
            + "\"lattice_wal_append_total\":[{\"type\":\"counter\",\"help\":\"appends\",\"unit\":\"\"}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, DenyAll("up"), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Has.Count.EqualTo(1));
            Assert.That(result.Metrics[0].Metric, Is.EqualTo("up"));
        });
    }

    [Test]
    public async Task MetricMetadata_surfaces_a_backend_failure_as_a_clean_error()
    {
        var client = Client("{}", out _, HttpStatusCode.BadGateway);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Metrics, Is.Empty);
        });
    }

    [Test]
    public async Task MetricMetadata_degrades_a_404_metadata_endpoint_to_a_graceful_empty_result()
    {
        // A backend whose metadata endpoint 404s (unpopulated or partly unwired)
        // must not surface a raw 404 passthrough: it degrades to an empty success,
        // consistent with how list_metrics and the query tools return empty on an
        // empty backend (issue #1339). Distinct from a genuine backend fault (see
        // the BadGateway test above, which stays a failure).
        var client = Client("{}", out _, HttpStatusCode.NotFound);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True, "a 404 metadata endpoint degrades to success, not a raw passthrough");
            Assert.That(result.Error, Is.Null);
            Assert.That(result.Metrics, Is.Empty);
        });
    }

    [Test]
    public async Task MetricMetadata_named_lookup_that_resolves_nothing_carries_a_distinct_notice()
    {
        // Issue #1402 item 12: list_metrics returns Prometheus exposition names
        // (…_total) that do not resolve verbatim here, which keys on the OTEL base
        // instrument name. A named lookup resolving nothing must be a distinct
        // signal - a success carrying a 'notice' advisory - not indistinguishable
        // from an admitted-but-empty listing.
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, "orleans_lattice_backup_captures_total");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Is.Empty);
            Assert.That(result.Notice, Is.Not.Null);
            Assert.That(result.Notice, Does.Contain("orleans_lattice_backup_captures_total"));
            Assert.That(result.Notice, Does.Contain("base instrument name"));
        });
    }

    [Test]
    public async Task MetricMetadata_unnamed_listing_carries_no_notice()
    {
        // The advisory is only for a specific named lookup that resolved nothing;
        // a full (unnamed) listing that is simply empty is not flagged.
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Notice, Is.Null);
        });
    }

    [Test]
    public void MetricMetadata_rejects_a_null_policy()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.MetricMetadataAsync(
                Client("{}", out _), policy: null!, CancellationToken.None, "up"));
}
