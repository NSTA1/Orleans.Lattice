namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// How the telemetry backend proxy authenticates itself to the read-only
/// Prometheus / PromQL-compatible backend. This is the <b>backend</b> credential
/// on the far side of the dual-credential trust boundary; it is entirely
/// distinct from the caller's Lattice credential, which the proxy never forwards
/// to the backend.
/// </summary>
public enum LatticeTelemetryBackendAuthMode
{
    /// <summary>
    /// No backend authentication. The proxy sends requests without an
    /// <c>Authorization</c> header or a client certificate. Appropriate only when
    /// the backend is reachable solely from the proxy over a trusted network.
    /// </summary>
    None,

    /// <summary>
    /// HTTP bearer-token authentication. The proxy stamps
    /// <c>Authorization: Bearer &lt;token&gt;</c> from
    /// <see cref="LatticeTelemetryBackendCredential.BearerToken"/> on every
    /// backend request.
    /// </summary>
    Bearer,

    /// <summary>
    /// HTTP basic authentication. The proxy stamps
    /// <c>Authorization: Basic &lt;base64(user:password)&gt;</c> from
    /// <see cref="LatticeTelemetryBackendCredential.BasicUsername"/> and
    /// <see cref="LatticeTelemetryBackendCredential.BasicPassword"/> on every
    /// backend request.
    /// </summary>
    Basic,

    /// <summary>
    /// Mutual-TLS authentication. The proxy presents
    /// <see cref="LatticeTelemetryBackendCredential.ClientCertificate"/> on the
    /// backend connection. No <c>Authorization</c> header is stamped.
    /// </summary>
    MutualTls,
}
