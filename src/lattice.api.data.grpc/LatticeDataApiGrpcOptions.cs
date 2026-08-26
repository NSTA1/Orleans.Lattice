namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Api.Data.Grpc</c> server-side
/// binding - the write-capable external data-plane surface.
/// </summary>
public sealed class LatticeDataApiGrpcOptions
{
    /// <summary>
    /// Whether the authorization interceptor enforces
    /// <see cref="ILatticeDataApiAuthorizer"/> on every inbound data-API call.
    /// Defaults to <see langword="true"/> (default-deny): the binding fails
    /// closed unless a host either registers a permissive authorizer or
    /// explicitly turns enforcement off. Because this is a write-capable
    /// surface, leaving this on is strongly recommended even when an outer
    /// boundary guards the endpoint.
    /// </summary>
    public bool RequireAuthorization { get; set; } = true;

    /// <summary>
    /// The inbound request-header (gRPC metadata) name that carries the caller's
    /// credential token, bridged into the ambient Lattice credential so the
    /// data-plane access gate can resolve the caller's subject and authorize
    /// every mutation and read. Defaults to <c>authorization</c>.
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

    /// <summary>
    /// The inbound request-header (gRPC metadata) name that carries the caller's
    /// asserted active tenant id, lifted onto the ambient
    /// <see cref="LatticeActiveTenantContext"/> for the duration of the call so
    /// the tenant-aware data plane (per-tenant write admission / quota, and
    /// tenant-scoped tree resolution) sees the caller's tenant rather than the
    /// reserved default. Defaults to <c>lattice-active-tenant</c>. The asserted
    /// tenant is validated against the caller's subject membership downstream; a
    /// missing or syntactically invalid value leaves the call with no active
    /// tenant asserted (fail-closed). Set to the empty string to disable
    /// active-tenant lifting entirely.
    /// </summary>
    public string ActiveTenantHeaderName { get; set; } = "lattice-active-tenant";
}
