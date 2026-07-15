namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Options for the optional <c>Orleans.Lattice.Api.Mcp.Telemetry</c> add-on: the
/// read-only Prometheus / PromQL-compatible backend the telemetry tools proxy,
/// the backend credential presented on the far side of the dual-credential trust
/// boundary, the range-query guardrails, and the metric-access allow-list. Bound
/// by
/// <see cref="LatticeMcpTelemetryServiceCollectionExtensions.AddTelemetryTools"/>
/// and resolvable via <c>IOptions&lt;LatticeApiMcpTelemetryOptions&gt;</c>.
/// </summary>
/// <remarks>
/// The proxy stamps the configured <see cref="Credential"/> selected by
/// <see cref="AuthMode"/> on every backend request and <b>never</b> forwards the
/// caller's Lattice credential to the backend: the MCP-side authorization (a
/// <c>LatticeOperation.Telemetry</c> grant) and the backend-side credential are
/// two independent halves of the trust boundary.
/// </remarks>
public sealed class LatticeApiMcpTelemetryOptions
{
    /// <summary>
    /// The base address of the read-only Prometheus / PromQL-compatible backend
    /// the telemetry tools query (for example
    /// <c>https://prometheus.internal:9090/</c>). Must be an absolute URI. There
    /// is no default; a host that opts telemetry in must supply one.
    /// </summary>
    public Uri? BackendAddress { get; set; }

    /// <summary>
    /// How the proxy authenticates to the backend. Defaults to
    /// <see cref="LatticeTelemetryBackendAuthMode.None"/>. When set to any mode
    /// other than <see cref="LatticeTelemetryBackendAuthMode.None"/>, the matching
    /// member of <see cref="Credential"/> must be supplied.
    /// </summary>
    public LatticeTelemetryBackendAuthMode AuthMode { get; set; }
        = LatticeTelemetryBackendAuthMode.None;

    /// <summary>
    /// The backend credential secret material, consulted according to
    /// <see cref="AuthMode"/>. Left <see langword="null"/> when
    /// <see cref="AuthMode"/> is <see cref="LatticeTelemetryBackendAuthMode.None"/>.
    /// This holder carries the backend credential only; the caller's Lattice
    /// credential is never stored here.
    /// </summary>
    public LatticeTelemetryBackendCredential? Credential { get; set; }

    /// <summary>
    /// The per-request timeout for a backend call. Defaults to 30 seconds. Must
    /// be strictly positive.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The largest time window (<c>end - start</c>) a single range query may
    /// span, bounding the work and payload a caller can request. Defaults to 24
    /// hours. Must be strictly positive.
    /// </summary>
    public TimeSpan MaxRange { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// The largest resolution step a single range query may request, bounding a
    /// coarse-but-unbounded scan and keeping the returned series count sane.
    /// Defaults to 1 hour. Must be strictly positive.
    /// </summary>
    public TimeSpan MaxStep { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Which backend metrics the proxy may read. Defaults to
    /// <see cref="LatticeTelemetryMetricAccessMode.ReadAll"/> (no allow-list
    /// restriction). Set to
    /// <see cref="LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed"/> to deny
    /// every metric except those listed in <see cref="AllowedMetrics"/>.
    /// </summary>
    public LatticeTelemetryMetricAccessMode MetricAccess { get; set; }
        = LatticeTelemetryMetricAccessMode.ReadAll;

    /// <summary>
    /// The exact metric names and/or patterns permitted when
    /// <see cref="MetricAccess"/> is
    /// <see cref="LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed"/>. Each
    /// entry is either an exact metric name (for example
    /// <c>lattice_wal_append_total</c>) or a pattern containing <c>*</c>
    /// wildcards (for example <c>lattice_wal_*</c>). Ignored when
    /// <see cref="MetricAccess"/> is
    /// <see cref="LatticeTelemetryMetricAccessMode.ReadAll"/>.
    /// </summary>
    public IList<string> AllowedMetrics { get; } = new List<string>();
}
