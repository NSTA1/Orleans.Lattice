namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Supplies a currently-valid <b>backend</b> bearer token for the telemetry proxy
/// when <see cref="LatticeApiMcpTelemetryOptions.AuthMode"/> is
/// <see cref="LatticeTelemetryBackendAuthMode.DynamicBearer"/>. The proxy calls
/// <see cref="GetAccessTokenAsync"/> before each backend request and stamps the
/// returned value as <c>Authorization: Bearer &lt;token&gt;</c>.
/// </summary>
/// <remarks>
/// This is the extensibility seam that keeps the core telemetry package free of
/// any cloud-identity dependency: an implementation owns token acquisition,
/// caching, and refresh (for example acquiring and rotating an Entra access token
/// for a managed-Prometheus endpoint). The token flows only toward the backend;
/// it is a backend credential and is never conflated with the caller's Lattice
/// credential. The <c>Orleans.Lattice.Api.Mcp.Telemetry.Azure</c> companion
/// package provides an Azure managed-identity implementation.
/// </remarks>
public interface ITelemetryBackendTokenProvider
{
    /// <summary>
    /// Returns a bearer token that is valid at the moment of the call. The proxy
    /// invokes this once per backend request, so an implementation should serve a
    /// cached token and only perform the (potentially remote) acquisition when the
    /// cached token is missing or near expiry.
    /// </summary>
    /// <param name="cancellationToken">Cancels a pending token acquisition.</param>
    /// <returns>A non-empty bearer token for the backend.</returns>
    ValueTask<string> GetAccessTokenAsync(CancellationToken cancellationToken);
}
