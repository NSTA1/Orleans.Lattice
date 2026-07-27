using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// The telemetry tool module: the <see cref="ILatticeApiMcpToolGroup"/> for
/// <see cref="LatticeApiMcpGroup.Telemetry"/>. It plugs the companion telemetry
/// package into the MCP binding's permission-aware discovery core so a caller
/// holding a <c>LatticeOperation.Telemetry</c> grant is offered the four
/// read-only <c>lattice_telemetry_*</c> tools that proxy the configured
/// Prometheus / PromQL-compatible backend.
/// </summary>
/// <remarks>
/// <para>
/// The tools are built <b>once</b> in the constructor from the static
/// <see cref="TelemetryToolHandlers"/> method groups. Each tool resolves its
/// <see cref="IPrometheusQueryClient"/>, <see cref="TelemetryMetricAccessPolicy"/>,
/// and options collaborators from the request service provider at call time, so
/// the per-session discovery filter selects from this prebuilt list and never
/// re-materialises a tool per <c>tools/list</c> or <c>tools/call</c>.
/// </para>
/// <para>
/// Every tool is read-only and non-destructive. Metric-access enforcement,
/// range guardrails, and backend-fault mapping live in the handlers, so the module
/// itself adds no query logic.
/// </para>
/// </remarks>
internal sealed class TelemetryToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the telemetry tool list once from the supplied service provider,
    /// which the SDK consults to mark the <see cref="IPrometheusQueryClient"/>,
    /// <see cref="TelemetryMetricAccessPolicy"/>, and options handler parameters as
    /// DI-injected (schema-excluded); the instances themselves are resolved per
    /// invocation from the request's service scope.
    /// </summary>
    /// <param name="services">
    /// The service provider whose <c>IServiceProviderIsService</c> recognises the
    /// registered backend client, metric-access policy, and telemetry options.
    /// </param>
    public TelemetryToolGroup(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);

        Tools = new McpServerTool[]
        {
            Create(
                services,
                TelemetryToolHandlers.QueryAsync,
                "lattice_telemetry_query",
                "Run an instant telemetry query",
                "Evaluates a PromQL expression at a single instant against the cluster's read-only metrics "
                + "backend, returning the projected vector or scalar series. Absent and denied are surfaced "
                + "differently: under the deny-all posture a query naming a non-admitted metric is rejected "
                + "with a denial message, whereas a query for a metric that exists in the allow-list but has "
                + "produced no samples (or an unknown metric under a read-all posture) returns success with an "
                + "empty series - an empty series therefore means 'no data', not 'denied'. Read-only."),
            Create(
                services,
                TelemetryToolHandlers.QueryRangeAsync,
                "lattice_telemetry_query_range",
                "Run a range telemetry query",
                "Evaluates a PromQL expression over a time range at a fixed resolution, returning the projected "
                + "matrix series. The range and step are bounded by the configured guardrails; an over-budget "
                + "request returns a clean error. Read-only."),
            Create(
                services,
                TelemetryToolHandlers.ListMetricsAsync,
                "lattice_telemetry_list_metrics",
                "List telemetry metric names",
                "Lists the metric names the backend exposes, filtered to those the metric-access policy admits "
                + "in the deny-all posture. These are Prometheus exposition names, which carry a suffix "
                + "(_total for counters; _bucket/_count/_sum for histograms) that the underlying OTEL base "
                + "instrument name does not - so a name listed here often will not resolve verbatim in "
                + "metric_metadata, which keys on the base instrument name (strip the exposition suffix). "
                + "Read-only."),
            Create(
                services,
                TelemetryToolHandlers.MetricMetadataAsync,
                "lattice_telemetry_metric_metadata",
                "Read telemetry metric metadata",
                "Reads backend metadata (type, help text, and unit) for a named metric, or for every admitted "
                + "metric when none is named. This keys on the OTEL base instrument name, not the Prometheus "
                + "exposition name returned by list_metrics: pass the base name (drop the "
                + "_total/_bucket/_count/_sum suffix). A named lookup that resolves nothing returns an empty "
                + "result carrying a 'notice' advisory, distinguishing an unrecognised name from an "
                + "admitted-but-empty listing. A non-admitted named metric is rejected in the deny-all posture. "
                + "Read-only."),
        };
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.Telemetry;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static McpServerTool Create(
        IServiceProvider services,
        Delegate handler,
        string name,
        string title,
        string description)
        => McpServerTool.Create(
            handler,
            new McpServerToolCreateOptions
            {
                Services = services,
                Name = name,
                Title = title,
                Description = description,
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });
}
