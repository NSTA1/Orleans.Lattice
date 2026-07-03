namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Api.State.Grpc</c> server-side
/// binding.
/// </summary>
public sealed class LatticeStateApiGrpcOptions
{
    /// <summary>
    /// Whether the authorization interceptor enforces
    /// <see cref="ILatticeStateApiAuthorizer"/> on every inbound state-API
    /// call. Defaults to <see langword="true"/> (default-deny): the binding
    /// fails closed unless a host either registers a permissive authorizer or
    /// explicitly turns enforcement off. Set to <see langword="false"/> only
    /// when an outer authentication boundary already guards the endpoint.
    /// </summary>
    public bool RequireAuthorization { get; set; } = true;

    /// <summary>
    /// The inbound request-header (gRPC metadata) name that carries the caller's
    /// credential token, bridged into the ambient Lattice credential so the
    /// data-plane access gate can resolve the caller's subject and filter reads.
    /// Defaults to <c>authorization</c>. Only consulted when auth-backed
    /// visibility is active (the <c>Orleans.Lattice.Auth</c> add-on is
    /// registered); when it is not, no header is read and the state API behaves
    /// exactly as before.
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
