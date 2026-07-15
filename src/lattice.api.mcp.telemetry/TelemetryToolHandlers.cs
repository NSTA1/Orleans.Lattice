using System.ComponentModel;
using System.Text.Json;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// The read-only adapter methods behind the telemetry tool module. Each method is
/// a thin binding over a single <see cref="IPrometheusQueryClient"/> call that
/// enforces the metric-access policy, maps the backend envelope into a structured
/// result, and surfaces every backend fault, guardrail rejection, or metric-access
/// denial as a clean structured result rather than a thrown exception.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="IPrometheusQueryClient"/>, <see cref="TelemetryMetricAccessPolicy"/>,
/// and (for the range tool) <see cref="IOptions{TOptions}"/> parameters are
/// resolved from the tool invocation's request service provider by the MCP SDK
/// (they are excluded from each tool's input schema); the
/// <see cref="CancellationToken"/> is bound to the invocation's token. The
/// remaining, schema-visible arguments carry the query text and range budget.
/// </para>
/// <para>
/// A genuine caller cancellation propagates; a backend timeout, HTTP failure,
/// non-success status, or malformed payload is caught and returned on the result's
/// <c>Error</c> field so the agent observes a structured result instead of a
/// transport fault.
/// </para>
/// </remarks>
internal static class TelemetryToolHandlers
{
    private const string SuccessStatus = "success";

    /// <summary>Evaluates a PromQL expression at a single instant.</summary>
    public static async Task<TelemetryQueryResult> QueryAsync(
        IPrometheusQueryClient client,
        TelemetryMetricAccessPolicy policy,
        CancellationToken cancellationToken,
        [Description("The PromQL expression to evaluate at a single instant, for example 'up' or 'rate(lattice_wal_append_total[5m])'.")]
        string query,
        [Description("Optional evaluation timestamp; null evaluates at the backend's current time.")]
        DateTimeOffset? time = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(query);

        if (!TryAuthorizeQuery(policy, query, out var deniedMetric))
        {
            return TelemetryQueryResult.Failure(DeniedMessage(deniedMetric!));
        }

        try
        {
            var response = await client.InstantQueryAsync(query, time, cancellationToken).ConfigureAwait(false);
            return MapQueryEnvelope(response);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return TelemetryQueryResult.Failure(BackendErrorMessage(ex));
        }
    }

    /// <summary>Evaluates a PromQL expression over a time range at a fixed resolution.</summary>
    public static async Task<TelemetryQueryResult> QueryRangeAsync(
        IPrometheusQueryClient client,
        TelemetryMetricAccessPolicy policy,
        IOptions<LatticeApiMcpTelemetryOptions> options,
        CancellationToken cancellationToken,
        [Description("The PromQL expression to evaluate across the range.")]
        string query,
        [Description("The inclusive start of the range.")]
        DateTimeOffset start,
        [Description("The inclusive end of the range; must be at or after start.")]
        DateTimeOffset end,
        [Description("The resolution step between evaluation points, as a duration (for example 00:00:30 for 30 seconds). Must be strictly positive.")]
        TimeSpan step)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(query);

        var settings = options.Value;
        if (end < start)
        {
            return TelemetryQueryResult.Failure("The range end must be at or after the range start.");
        }

        if (step <= TimeSpan.Zero)
        {
            return TelemetryQueryResult.Failure("The range step must be strictly positive.");
        }

        var range = end - start;
        if (range > settings.MaxRange)
        {
            return TelemetryQueryResult.Failure(
                $"The requested range of {range} exceeds the configured maximum of {settings.MaxRange}.");
        }

        if (step > settings.MaxStep)
        {
            return TelemetryQueryResult.Failure(
                $"The requested step of {step} exceeds the configured maximum of {settings.MaxStep}.");
        }

        if (!TryAuthorizeQuery(policy, query, out var deniedMetric))
        {
            return TelemetryQueryResult.Failure(DeniedMessage(deniedMetric!));
        }

        try
        {
            var response = await client.RangeQueryAsync(query, start, end, step, cancellationToken).ConfigureAwait(false);
            return MapQueryEnvelope(response);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return TelemetryQueryResult.Failure(BackendErrorMessage(ex));
        }
    }

    /// <summary>Lists the backend metric names the caller may see.</summary>
    public static async Task<TelemetryMetricListResult> ListMetricsAsync(
        IPrometheusQueryClient client,
        TelemetryMetricAccessPolicy policy,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(policy);

        try
        {
            var names = await client.ListMetricNamesAsync(cancellationToken).ConfigureAwait(false);
            if (policy.IsReadAll)
            {
                return new TelemetryMetricListResult { Success = true, Metrics = names };
            }

            var admitted = new List<string>(names.Count);
            foreach (var name in names)
            {
                if (policy.IsAdmitted(name))
                {
                    admitted.Add(name);
                }
            }

            return new TelemetryMetricListResult { Success = true, Metrics = admitted };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return TelemetryMetricListResult.Failure(BackendErrorMessage(ex));
        }
    }

    /// <summary>Reads backend metadata for a named metric, or for every admitted metric.</summary>
    public static async Task<TelemetryMetricMetadataResult> MetricMetadataAsync(
        IPrometheusQueryClient client,
        TelemetryMetricAccessPolicy policy,
        CancellationToken cancellationToken,
        [Description("Optional metric name to look up; null returns metadata for every metric the caller may see.")]
        string? metric = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(policy);

        if (metric is not null && !policy.IsAdmitted(metric))
        {
            return TelemetryMetricMetadataResult.Failure(DeniedMessage(metric));
        }

        try
        {
            var response = await client.MetricMetadataAsync(metric, cancellationToken).ConfigureAwait(false);
            if (!IsSuccess(response.Status))
            {
                return TelemetryMetricMetadataResult.Failure(StatusMessage(response.Status));
            }

            return MapMetadata(response.Data, policy);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return TelemetryMetricMetadataResult.Failure(BackendErrorMessage(ex));
        }
    }

    private static bool TryAuthorizeQuery(
        TelemetryMetricAccessPolicy policy,
        string query,
        out string? deniedMetric)
    {
        deniedMetric = null;
        if (policy.IsReadAll)
        {
            return true;
        }

        foreach (var name in PromQlMetricExtractor.Extract(query))
        {
            if (!policy.IsAdmitted(name))
            {
                deniedMetric = name;
                return false;
            }
        }

        return true;
    }

    private static TelemetryQueryResult MapQueryEnvelope(PrometheusQueryResponse response)
    {
        if (!IsSuccess(response.Status))
        {
            return TelemetryQueryResult.Failure(StatusMessage(response.Status));
        }

        var data = response.Data;
        if (data.ValueKind != JsonValueKind.Object
            || !data.TryGetProperty("resultType", out var resultTypeElement)
            || resultTypeElement.ValueKind != JsonValueKind.String)
        {
            return TelemetryQueryResult.Failure(
                "The telemetry backend response did not contain a recognisable result payload.");
        }

        var resultType = resultTypeElement.GetString() ?? string.Empty;
        var series = new List<TelemetrySeries>();
        if (data.TryGetProperty("result", out var result))
        {
            switch (resultType)
            {
                case "vector":
                    ParseVector(result, series);
                    break;
                case "matrix":
                    ParseMatrix(result, series);
                    break;
                case "scalar":
                case "string":
                    ParseScalarOrString(result, series);
                    break;
            }
        }

        return new TelemetryQueryResult { Success = true, ResultType = resultType, Series = series };
    }

    private static void ParseVector(JsonElement result, List<TelemetrySeries> series)
    {
        if (result.ValueKind != JsonValueKind.Array)
        {
            return;
        }

        foreach (var item in result.EnumerateArray())
        {
            var samples = new List<TelemetrySample>(1);
            if (item.TryGetProperty("value", out var value))
            {
                AddSample(value, samples);
            }

            series.Add(new TelemetrySeries { Labels = ReadLabels(item), Samples = samples });
        }
    }

    private static void ParseMatrix(JsonElement result, List<TelemetrySeries> series)
    {
        if (result.ValueKind != JsonValueKind.Array)
        {
            return;
        }

        foreach (var item in result.EnumerateArray())
        {
            var samples = new List<TelemetrySample>();
            if (item.TryGetProperty("values", out var values) && values.ValueKind == JsonValueKind.Array)
            {
                foreach (var pair in values.EnumerateArray())
                {
                    AddSample(pair, samples);
                }
            }

            series.Add(new TelemetrySeries { Labels = ReadLabels(item), Samples = samples });
        }
    }

    private static void ParseScalarOrString(JsonElement result, List<TelemetrySeries> series)
    {
        var samples = new List<TelemetrySample>(1);
        AddSample(result, samples);
        if (samples.Count > 0)
        {
            series.Add(new TelemetrySeries { Samples = samples });
        }
    }

    private static IReadOnlyDictionary<string, string> ReadLabels(JsonElement item)
    {
        if (!item.TryGetProperty("metric", out var metric) || metric.ValueKind != JsonValueKind.Object)
        {
            return EmptyLabels;
        }

        var labels = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var property in metric.EnumerateObject())
        {
            labels[property.Name] = property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString() ?? string.Empty
                : property.Value.GetRawText();
        }

        return labels;
    }

    private static void AddSample(JsonElement pair, List<TelemetrySample> samples)
    {
        if (pair.ValueKind != JsonValueKind.Array || pair.GetArrayLength() < 2)
        {
            return;
        }

        var timestampElement = pair[0];
        var valueElement = pair[1];
        var timestamp = timestampElement.ValueKind == JsonValueKind.Number
            ? timestampElement.GetDouble()
            : 0d;
        var value = valueElement.ValueKind == JsonValueKind.String
            ? valueElement.GetString() ?? string.Empty
            : valueElement.GetRawText();

        samples.Add(new TelemetrySample { Timestamp = timestamp, Value = value });
    }

    private static TelemetryMetricMetadataResult MapMetadata(JsonElement data, TelemetryMetricAccessPolicy policy)
    {
        if (data.ValueKind != JsonValueKind.Object)
        {
            return TelemetryMetricMetadataResult.Failure(
                "The telemetry backend response did not contain a recognisable metadata payload.");
        }

        var metrics = new List<TelemetryMetricMetadata>();
        foreach (var property in data.EnumerateObject())
        {
            if (!policy.IsAdmitted(property.Name) || property.Value.ValueKind != JsonValueKind.Array)
            {
                continue;
            }

            foreach (var entry in property.Value.EnumerateArray())
            {
                metrics.Add(new TelemetryMetricMetadata
                {
                    Metric = property.Name,
                    Type = ReadString(entry, "type"),
                    Help = ReadString(entry, "help"),
                    Unit = ReadString(entry, "unit"),
                });
            }
        }

        return new TelemetryMetricMetadataResult { Success = true, Metrics = metrics };
    }

    private static string ReadString(JsonElement element, string propertyName)
        => element.ValueKind == JsonValueKind.Object
            && element.TryGetProperty(propertyName, out var value)
            && value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? string.Empty
            : string.Empty;

    private static bool IsSuccess(string status)
        => string.Equals(status, SuccessStatus, StringComparison.Ordinal);

    private static string StatusMessage(string status)
        => string.IsNullOrEmpty(status)
            ? "The telemetry backend returned no status."
            : $"The telemetry backend reported status '{status}'.";

    private static string BackendErrorMessage(Exception ex)
        => $"The telemetry backend request failed: {ex.Message}";

    private static string DeniedMessage(string metric)
        => $"Metric '{metric}' is not permitted by the telemetry metric-access allow-list.";

    private static readonly IReadOnlyDictionary<string, string> EmptyLabels
        = new Dictionary<string, string>(StringComparer.Ordinal);
}
