namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Options controlling the <c>Orleans.Lattice.Api.TreeAdmin.Grpc</c> server-side
/// binding.
/// </summary>
public sealed class LatticeTreeAdminApiGrpcOptions
{
    /// <summary>
    /// Whether the authorization interceptor enforces
    /// <see cref="ILatticeTreeAdminApiAuthorizer"/> on every inbound
    /// tree-administration control-API call. Defaults to <see langword="true"/>
    /// (default-deny): the binding fails closed unless a host either registers a
    /// permissive authorizer or explicitly turns enforcement off. Set to
    /// <see langword="false"/> only when an outer authentication boundary already
    /// guards the endpoint.
    /// </summary>
    public bool RequireAuthorization { get; set; } = true;

    /// <summary>
    /// The inbound request-header (gRPC metadata) name that carries the caller's
    /// credential token, bridged into the ambient Lattice credential so the
    /// composed access gate can resolve the caller's subject and authorize each
    /// operation. Defaults to <c>authorization</c>. Only consulted when auth-backed
    /// control is active (the <c>Orleans.Lattice.Auth</c> add-on is registered);
    /// when it is not, no header is read.
    /// </summary>
    public string CredentialHeaderName { get; set; } = "authorization";

    /// <summary>
    /// The request header carrying the caller's asserted active tenant, lifted
    /// onto the ambient active-tenant context so this facade's tenant-scoped name
    /// resolution sees the caller's tenant rather than the reserved default.
    /// Defaults to <see cref="LatticeActiveTenantAssertion.DefaultHeaderName"/>,
    /// matching every other binding so one client works against all of them. Set
    /// to <c>null</c> or empty to disable the assertion.
    /// </summary>
    /// <remarks>
    /// The asserted tenant is a caller claim, re-validated against the caller's
    /// own subject membership by the tenancy add-on before it can scope anything;
    /// carrying it here grants no access. With no tenancy add-on registered the
    /// assertion resolves the reserved default tenant and changes nothing.
    /// </remarks>
    public string ActiveTenantHeaderName { get; set; } = LatticeActiveTenantAssertion.DefaultHeaderName;

    /// <summary>
    /// The authentication scheme stamped on the bridged
    /// <see cref="LatticeCredential"/>, matched by a registered
    /// <c>ILatticeCredentialAuthenticator</c> to resolve the caller's subject.
    /// Defaults to <c>Bearer</c>. A case-insensitive scheme prefix on the header
    /// value (for example <c>"Bearer "</c>) is stripped before the remaining token
    /// is used as the credential.
    /// </summary>
    public string CredentialScheme { get; set; } = "Bearer";

    /// <summary>
    /// The auth schemes the endpoint advertises from its unauthenticated
    /// <c>GetAuthScheme</c> RPC, in preference order. Empty by default (the
    /// endpoint advertises nothing, so a client falls back to manual or Basic
    /// selection). Populated by a host to tell clients how to sign in. Each
    /// descriptor must carry only public configuration - never a secret.
    /// </summary>
    public IList<AuthSchemeDescriptor> AdvertisedAuthSchemes { get; } = new List<AuthSchemeDescriptor>();
}
