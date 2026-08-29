namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Governs which backend metrics the telemetry proxy is permitted to read.
/// </summary>
public enum LatticeTelemetryMetricAccessMode
{
    /// <summary>
    /// Read every metric the backend exposes. The default posture: the proxy
    /// places no allow-list restriction on metric names and forwards any queried
    /// series to the backend.
    /// </summary>
    ReadAll,

    /// <summary>
    /// Deny every metric by default and permit only those matching an entry in
    /// <see cref="LatticeTelemetryOptions.AllowedMetrics"/> (exact names or
    /// patterns). A deny-all posture with an empty allow-list exposes nothing and
    /// is rejected at validation as almost certainly a misconfiguration.
    /// </summary>
    DenyAllExceptAllowed,
}
