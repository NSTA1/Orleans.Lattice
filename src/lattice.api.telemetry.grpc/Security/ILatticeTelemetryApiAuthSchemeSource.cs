namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// Supplies the auth-scheme advertisement the telemetry endpoint returns from its
/// unauthenticated <c>GetAuthScheme</c> RPC. A host registers an implementation
/// (or configures the built-in options-backed one) to tell connecting clients how
/// to sign in - which schemes are accepted and the public parameters (OIDC
/// authority, tenant, client id, audience) each needs.
/// </summary>
/// <remarks>
/// Because the advertisement is served without a credential, an implementation
/// must return only public configuration. It must never surface a secret, a
/// signing key, or any user-specific data.
/// </remarks>
public interface ILatticeTelemetryApiAuthSchemeSource
{
    /// <summary>
    /// Returns the current advertisement. Called per RPC; may reflect live
    /// configuration. Returns an advertisement with no schemes when the endpoint
    /// advertises nothing (the default), leaving the client to fall back to a
    /// manually selected or Basic scheme.
    /// </summary>
    /// <returns>The advertisement to serve.</returns>
    AuthSchemeAdvertisement GetAdvertisement();
}
