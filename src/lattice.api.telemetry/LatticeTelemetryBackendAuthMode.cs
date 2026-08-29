namespace Orleans.Lattice.Api.Telemetry;

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

    /// <summary>
    /// Dynamic bearer-token authentication. Instead of a static token from
    /// <see cref="LatticeTelemetryBackendCredential"/>, the proxy acquires a fresh
    /// bearer token per request from a registered
    /// <see cref="ITelemetryBackendTokenProvider"/> and stamps it as
    /// <c>Authorization: Bearer &lt;token&gt;</c>. This mode suits backends fronted
    /// by a rotating, short-lived credential - for example an Entra (Azure AD)
    /// access token for a managed-Prometheus query endpoint, minted from a
    /// workload or managed identity. The provider owns token acquisition, caching,
    /// and refresh; the core package stays free of any cloud-identity dependency.
    /// A host selecting this mode must register an
    /// <see cref="ITelemetryBackendTokenProvider"/> (for Azure managed Prometheus,
    /// the <c>Orleans.Lattice.Api.Mcp.Telemetry.Azure</c> companion package
    /// supplies one).
    /// </summary>
    DynamicBearer,
}
