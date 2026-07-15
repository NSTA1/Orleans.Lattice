namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// The read-only backend seam the telemetry tool layer (added in Phase D) drives
/// to answer metric queries. It covers the four operations the tools need:
/// instant query, range query, metric-name listing, and metric metadata. Every
/// implementation talks to the configured Prometheus / PromQL-compatible backend
/// stamping the configured <b>backend</b> credential and never the caller's
/// Lattice credential.
/// </summary>
internal interface IPrometheusQueryClient
{
    /// <summary>
    /// Evaluates a PromQL expression at a single instant.
    /// </summary>
    /// <param name="query">The PromQL expression to evaluate.</param>
    /// <param name="time">
    /// The evaluation timestamp, or <see langword="null"/> to evaluate at the
    /// backend's current time.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the request.</param>
    /// <returns>The backend query envelope.</returns>
    Task<PrometheusQueryResponse> InstantQueryAsync(
        string query,
        DateTimeOffset? time,
        CancellationToken cancellationToken);

    /// <summary>
    /// Evaluates a PromQL expression over a time range at a fixed resolution.
    /// </summary>
    /// <param name="query">The PromQL expression to evaluate.</param>
    /// <param name="start">The inclusive start of the range.</param>
    /// <param name="end">The inclusive end of the range.</param>
    /// <param name="step">The resolution step between evaluation points.</param>
    /// <param name="cancellationToken">A token to cancel the request.</param>
    /// <returns>The backend query envelope.</returns>
    Task<PrometheusQueryResponse> RangeQueryAsync(
        string query,
        DateTimeOffset start,
        DateTimeOffset end,
        TimeSpan step,
        CancellationToken cancellationToken);

    /// <summary>
    /// Lists the metric names the backend currently exposes (the label values of
    /// the <c>__name__</c> label).
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the request.</param>
    /// <returns>The metric names, in the order the backend returned them.</returns>
    Task<IReadOnlyList<string>> ListMetricNamesAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Reads backend metadata (type, help text, and unit) for a metric, or for
    /// every metric when <paramref name="metric"/> is <see langword="null"/>.
    /// </summary>
    /// <param name="metric">
    /// The metric name to look up, or <see langword="null"/> for all metrics.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the request.</param>
    /// <returns>The backend metadata envelope.</returns>
    Task<PrometheusMetadataResponse> MetricMetadataAsync(
        string? metric,
        CancellationToken cancellationToken);
}
