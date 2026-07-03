namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Api.Auth.Grpc</c> server-side
/// binding - the remote membership and policy control plane. Administering
/// policy is the most sensitive surface in the cluster, so the defaults are
/// fail-closed.
/// </summary>
public sealed class LatticeAuthApiGrpcOptions
{
    /// <summary>
    /// Whether the authorization interceptor enforces
    /// <see cref="ILatticeAuthApiAuthorizer"/> on every inbound admin call.
    /// Defaults to <see langword="true"/> (default-deny): the binding fails
    /// closed unless a host either registers a permissive authorizer or
    /// explicitly turns enforcement off. Because this is the control plane for
    /// authorization itself, leaving this on is strongly recommended even when an
    /// outer boundary guards the endpoint. Note that turning it off does not open
    /// the surface: the facade's own per-call administrator check still runs.
    /// </summary>
    public bool RequireAuthorization { get; set; } = true;

    /// <summary>
    /// The inbound request-header (gRPC metadata) name that carries the caller's
    /// credential token, bridged into the ambient Lattice credential so the
    /// facade's administrator check can resolve the caller's subject. Defaults to
    /// <c>authorization</c>.
    /// </summary>
    public string CredentialHeaderName { get; set; } = "authorization";

    /// <summary>
    /// The authentication scheme stamped on the bridged
    /// <see cref="LatticeCredential"/>, matched by a registered
    /// <c>ILatticeCredentialAuthenticator</c> to resolve the caller's subject.
    /// Defaults to <c>Bearer</c>. A case-insensitive scheme prefix on the header
    /// value (for example <c>"Bearer "</c>) is stripped before the remaining
    /// token is used as the credential.
    /// </summary>
    public string CredentialScheme { get; set; } = "Bearer";
}
