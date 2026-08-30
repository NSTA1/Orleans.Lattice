using System.Text.Json;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Payload-shape and cancellation tests for <see cref="TelemetryToolHandlers"/>.
/// The handlers parse an untrusted backend envelope and draw a deliberate line
/// between two categories. An <b>unrecognisable</b> envelope - one whose
/// <c>data</c> is not an object, or that carries no string <c>resultType</c> - is
/// a structured failure. Everything else a backend can legitimately return but
/// this proxy cannot project (a missing <c>result</c>, a non-array result, a
/// sample that is not a two-element pair, a series with no <c>metric</c> object, a
/// non-string label value) <b>degrades to an empty projection</b>, the same
/// posture the query and metadata tools already take on an empty backend. These
/// tests pin both sides of that line so neither drifts into the other. A genuine
/// caller cancellation, by contrast, must propagate on every handler.
/// </summary>
public sealed partial class TelemetryToolHandlersTests
{
    // ---- Cancellation propagates on every handler ----

    [Test]
    public void QueryRange_propagates_a_genuine_caller_cancellation()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            () => TelemetryToolHandlers.QueryRangeAsync(
                client,
                ReadAll(),
                Guardrails(),
                cts.Token,
                "up",
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(5),
                TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void ListMetrics_propagates_a_genuine_caller_cancellation()
    {
        var client = Client("{\"status\":\"success\",\"data\":[]}", out _);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            () => TelemetryToolHandlers.ListMetricsAsync(client, ReadAll(), cts.Token));
    }

    [Test]
    public void MetricMetadata_propagates_a_genuine_caller_cancellation()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            () => TelemetryToolHandlers.MetricMetadataAsync(client, ReadAll(), cts.Token, "up"));
    }

    // ---- Non-success and unrecognisable envelopes ----

    [Test]
    public async Task MetricMetadata_surfaces_a_non_success_backend_status_as_a_clean_error()
    {
        var client = Client("{\"status\":\"error\",\"data\":{}}", out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("error"));
        });
    }

    [Test]
    public async Task An_absent_backend_status_is_reported_as_no_status_rather_than_an_empty_name()
    {
        var client = Client("{\"data\":{\"resultType\":\"vector\",\"result\":[]}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Is.EqualTo("The telemetry backend returned no status."));
        });
    }

    [Test]
    public async Task A_query_envelope_whose_data_is_not_an_object_is_an_unrecognisable_payload()
    {
        var client = Client("{\"status\":\"success\",\"data\":[1,2]}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("recognisable result payload"));
        });
    }

    [Test]
    public async Task A_query_envelope_missing_its_result_type_is_an_unrecognisable_payload()
    {
        var client = Client("{\"status\":\"success\",\"data\":{\"result\":[]}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("recognisable result payload"));
        });
    }

    [Test]
    public async Task A_query_envelope_whose_result_type_is_not_a_string_is_an_unrecognisable_payload()
    {
        var client = Client("{\"status\":\"success\",\"data\":{\"resultType\":7,\"result\":[]}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.That(result.Success, Is.False);
    }

    [Test]
    public async Task A_query_envelope_with_no_result_member_yields_an_empty_series_list()
    {
        var client = Client("{\"status\":\"success\",\"data\":{\"resultType\":\"vector\"}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResultType, Is.EqualTo("vector"));
            Assert.That(result.Series, Is.Empty);
        });
    }

    [Test]
    public async Task An_unknown_result_type_yields_an_empty_series_list_rather_than_a_fault()
    {
        var client = Client(
            "{\"status\":\"success\",\"data\":{\"resultType\":\"histogram\",\"result\":[]}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.ResultType, Is.EqualTo("histogram"));
            Assert.That(result.Series, Is.Empty);
        });
    }

    // ---- Malformed vector / matrix / scalar payloads ----

    [Test]
    public async Task A_vector_result_that_is_not_an_array_yields_no_series()
    {
        var client = Client(
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":{}}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Series, Is.Empty);
        });
    }

    [Test]
    public async Task A_matrix_result_that_is_not_an_array_yields_no_series()
    {
        var client = Client(
            "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":\"nope\"}}", out _);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client,
            ReadAll(),
            Guardrails(),
            CancellationToken.None,
            "up",
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch.AddMinutes(5),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Series, Is.Empty);
        });
    }

    [Test]
    public async Task A_matrix_series_whose_values_member_is_not_an_array_yields_an_empty_sample_list()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"values\":{}}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client,
            ReadAll(),
            Guardrails(),
            CancellationToken.None,
            "up",
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch.AddMinutes(5),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Series, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Samples, Is.Empty);
        });
    }

    [Test]
    public async Task A_vector_series_with_no_metric_object_carries_no_labels()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"value\":[1.0,\"1\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Series, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Labels, Is.Empty);
            Assert.That(result.Series[0].Samples, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_vector_series_whose_metric_member_is_not_an_object_carries_no_labels()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":\"up\",\"value\":[1.0,\"1\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.That(result.Series[0].Labels, Is.Empty);
    }

    [Test]
    public async Task A_non_string_label_value_is_preserved_as_its_raw_json_text()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\",\"replica\":3},\"value\":[1.0,\"1\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Series[0].Labels["__name__"], Is.EqualTo("up"));
            Assert.That(result.Series[0].Labels["replica"], Is.EqualTo("3"));
        });
    }

    [Test]
    public async Task A_vector_series_with_no_value_member_carries_no_samples()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"}}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Series, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Samples, Is.Empty);
        });
    }

    [Test]
    public async Task A_sample_that_is_not_an_array_is_dropped()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"value\":\"1\"}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.That(result.Series[0].Samples, Is.Empty);
    }

    [Test]
    public async Task A_sample_pair_shorter_than_two_elements_is_dropped()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"value\":[1.0]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.That(result.Series[0].Samples, Is.Empty);
    }

    [Test]
    public async Task A_sample_with_a_non_numeric_timestamp_reads_as_zero()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"value\":[\"not-a-number\",\"1\"]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Series[0].Samples, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Samples[0].Timestamp, Is.Zero);
            Assert.That(result.Series[0].Samples[0].Value, Is.EqualTo("1"));
        });
    }

    [Test]
    public async Task A_sample_with_a_non_string_value_is_preserved_as_its_raw_json_text()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"value\":[1.5,42]}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Series[0].Samples[0].Timestamp, Is.EqualTo(1.5).Within(1e-9));
            Assert.That(result.Series[0].Samples[0].Value, Is.EqualTo("42"));
        });
    }

    [Test]
    public async Task A_scalar_result_that_is_not_a_pair_yields_no_series_at_all()
    {
        // ParseScalarOrString adds a series only when the single sample parsed, so a
        // malformed scalar produces an empty listing rather than a labelless,
        // sampleless placeholder series.
        var client = Client(
            "{\"status\":\"success\",\"data\":{\"resultType\":\"scalar\",\"result\":\"42\"}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "42");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Series, Is.Empty);
        });
    }

    [Test]
    public async Task A_string_result_type_is_projected_like_a_scalar()
    {
        var client = Client(
            "{\"status\":\"success\",\"data\":{\"resultType\":\"string\",\"result\":[1.0,\"hello\"]}}", out _);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "\"hello\"");

        Assert.Multiple(() =>
        {
            Assert.That(result.ResultType, Is.EqualTo("string"));
            Assert.That(result.Series, Has.Count.EqualTo(1));
            Assert.That(result.Series[0].Samples[0].Value, Is.EqualTo("hello"));
        });
    }

    // ---- Metadata payloads ----

    [Test]
    public async Task A_metadata_payload_that_is_not_an_object_is_an_unrecognisable_payload()
    {
        var client = Client("{\"status\":\"success\",\"data\":[]}", out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("recognisable metadata payload"));
        });
    }

    [Test]
    public async Task A_metadata_entry_whose_value_is_not_an_array_is_skipped()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"up\":{\"type\":\"gauge\"}}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Is.Empty);
        });
    }

    [Test]
    public async Task A_metadata_entry_that_is_not_an_object_reads_as_empty_strings()
    {
        var client = Client("{\"status\":\"success\",\"data\":{\"up\":[\"gauge\"]}}", out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Metrics, Has.Count.EqualTo(1));
            Assert.That(result.Metrics[0].Metric, Is.EqualTo("up"));
            Assert.That(result.Metrics[0].Type, Is.Empty);
            Assert.That(result.Metrics[0].Help, Is.Empty);
            Assert.That(result.Metrics[0].Unit, Is.Empty);
        });
    }

    [Test]
    public async Task A_metadata_entry_with_a_non_string_field_reads_that_field_as_empty()
    {
        const string json =
            "{\"status\":\"success\",\"data\":{\"up\":[{\"type\":\"gauge\",\"help\":7}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Metrics[0].Type, Is.EqualTo("gauge"));
            Assert.That(result.Metrics[0].Help, Is.Empty);
        });
    }

    [Test]
    public async Task A_metadata_entry_missing_a_field_reads_that_field_as_empty()
    {
        // The entry is an object but carries no 'unit', which is the common shape a
        // real Prometheus exposes for an unpaced instrument.
        const string json =
            "{\"status\":\"success\",\"data\":{\"up\":[{\"type\":\"gauge\",\"help\":\"Up.\"}]}}";
        var client = Client(json, out _);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, metric: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Metrics[0].Type, Is.EqualTo("gauge"));
            Assert.That(result.Metrics[0].Help, Is.EqualTo("Up."));
            Assert.That(result.Metrics[0].Unit, Is.Empty);
        });
    }

    // ---- Deny-all gate: a query that names no metric fails closed ----

    [Test]
    public async Task Query_in_deny_all_rejects_an_expression_that_names_no_metric()
    {
        // A constant expression resolves to no metric name at all. The gate cannot
        // prove it admissible, so it must fail closed rather than pass it through.
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client, DenyAll("lattice_wal_append_total"), CancellationToken.None, "1 + 1");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("does not name a metric"));
            Assert.That(handler.LastRequest, Is.Null, "A query the gate cannot admit must not reach the backend.");
        });
    }

    [Test]
    public async Task QueryRange_in_deny_all_rejects_an_expression_that_names_no_metric()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out var handler);

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client,
            DenyAll("lattice_wal_append_total"),
            Guardrails(),
            CancellationToken.None,
            "1 + 1",
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch.AddMinutes(5),
            TimeSpan.FromSeconds(30));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Contain("does not name a metric"));
            Assert.That(handler.LastRequest, Is.Null);
        });
    }

    [Test]
    public async Task ListMetrics_in_deny_all_over_an_empty_backend_returns_an_empty_admitted_list()
    {
        var client = Client("{\"status\":\"success\",\"data\":[]}", out _);

        var result = await TelemetryToolHandlers.ListMetricsAsync(
            client, DenyAll("lattice_wal_*"), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Is.Empty);
        });
    }

    [Test]
    public async Task ListMetrics_tolerates_a_backend_label_payload_that_is_not_an_array()
    {
        var client = Client("{\"status\":\"success\",\"data\":{}}", out _);

        var result = await TelemetryToolHandlers.ListMetricsAsync(client, ReadAll(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Is.Empty);
        });
    }

    [Test]
    public async Task ListMetrics_tolerates_a_backend_response_with_no_data_member()
    {
        var client = Client("{\"status\":\"success\"}", out _);

        var result = await TelemetryToolHandlers.ListMetricsAsync(client, ReadAll(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(result.Metrics, Is.Empty);
        });
    }

    [Test]
    public async Task ListMetrics_drops_a_non_string_entry_from_the_backend_label_values()
    {
        var client = Client("{\"status\":\"success\",\"data\":[\"up\",null,\"down\"]}", out _);

        var result = await TelemetryToolHandlers.ListMetricsAsync(client, ReadAll(), CancellationToken.None);

        Assert.That(result.Metrics, Is.EqualTo(new[] { "up", "down" }));
    }

    // ---- Null-argument guards on every handler ----

    [Test]
    public void QueryRange_rejects_a_null_client()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.QueryRangeAsync(
                client: null!,
                ReadAll(),
                Guardrails(),
                CancellationToken.None,
                "up",
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(5),
                TimeSpan.FromSeconds(30)));

    [Test]
    public void QueryRange_rejects_a_null_policy()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.QueryRangeAsync(
                Client("{}", out _),
                policy: null!,
                Guardrails(),
                CancellationToken.None,
                "up",
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(5),
                TimeSpan.FromSeconds(30)));

    [Test]
    public void QueryRange_rejects_a_null_query()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.QueryRangeAsync(
                Client("{}", out _),
                ReadAll(),
                Guardrails(),
                CancellationToken.None,
                query: null!,
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(5),
                TimeSpan.FromSeconds(30)));

    [Test]
    public void Query_rejects_a_null_policy()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.QueryAsync(
                Client("{}", out _), policy: null!, CancellationToken.None, "up"));

    [Test]
    public void ListMetrics_rejects_a_null_policy()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.ListMetricsAsync(
                Client("{}", out _), policy: null!, CancellationToken.None));

    [Test]
    public void MetricMetadata_rejects_a_null_client()
        => Assert.ThrowsAsync<ArgumentNullException>(
            () => TelemetryToolHandlers.MetricMetadataAsync(
                client: null!, ReadAll(), CancellationToken.None, "up"));

    // ---- The instant query carries its optional evaluation timestamp ----

    [Test]
    public async Task Query_with_an_evaluation_timestamp_puts_it_on_the_backend_request()
    {
        var client = Client("{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}", out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(
            client,
            ReadAll(),
            CancellationToken.None,
            "up",
            DateTimeOffset.FromUnixTimeSeconds(1435781451));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(handler.LastRequest, Is.Not.Null);
            Assert.That(handler.LastRequest!.RequestUri!.Query, Does.Contain("time=1435781451"));
        });
    }

    [Test]
    public async Task Query_without_an_evaluation_timestamp_sends_no_time_parameter()
    {
        var client = Client("{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}", out var handler);

        await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.That(handler.LastRequest!.RequestUri!.Query, Does.Not.Contain("time="));
    }

    [Test]
    public async Task An_at_budget_range_and_step_are_admitted_at_the_boundary()
    {
        // The guardrails reject strictly-greater-than, so a request exactly at the
        // configured budget must pass rather than be rejected off by one.
        var client = Client("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}", out var handler);
        var start = DateTimeOffset.UnixEpoch;

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client,
            ReadAll(),
            Guardrails(maxRange: TimeSpan.FromHours(1), maxStep: TimeSpan.FromMinutes(5)),
            CancellationToken.None,
            "up",
            start,
            start.AddHours(1),
            TimeSpan.FromMinutes(5));

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.True);
            Assert.That(handler.LastRequest, Is.Not.Null);
        });
    }

    [Test]
    public async Task A_zero_width_range_is_admitted()
    {
        var client = Client("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}", out _);
        var instant = DateTimeOffset.UnixEpoch;

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client,
            ReadAll(),
            Guardrails(),
            CancellationToken.None,
            "up",
            instant,
            instant,
            TimeSpan.FromSeconds(30));

        Assert.That(result.Success, Is.True);
    }

    [Test]
    public async Task A_query_result_data_member_is_detached_from_the_disposed_backend_document()
    {
        // The client clones the data element out of the JsonDocument it disposes, so
        // reading the projected series after the call must not fault.
        const string json =
            "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":"
            + "[{\"metric\":{\"__name__\":\"up\"},\"value\":[1.0,\"1\"]}]}}";
        var client = Client(json, out _);

        var response = await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo("success"));
            Assert.That(response.Data.ValueKind, Is.EqualTo(JsonValueKind.Object));
            Assert.That(response.Data.GetProperty("resultType").GetString(), Is.EqualTo("vector"));
        });
    }
}
