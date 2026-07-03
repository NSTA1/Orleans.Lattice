namespace Orleans.Lattice;

/// <summary>
/// Opaque caller credential carried from the client edge to the silo on the
/// ambient Orleans <see cref="Orleans.Runtime.RequestContext"/> via
/// <see cref="LatticeCredentialContext"/>. This is a transport-only seam: the
/// core library never reads it, so an unset credential costs nothing and
/// changes no read/write semantics. The Membership layer (registered
/// separately) is the only component that resolves the credential into a
/// subject.
/// </summary>
/// <remarks>
/// <para>
/// The credential deliberately carries only what an authenticator needs to
/// select itself and resolve a principal without re-parsing the token: the
/// opaque <see cref="Token"/>, an optional <see cref="Scheme"/> hint, an
/// optional pre-resolved <see cref="PrincipalId"/>, and an optional
/// <see cref="Metadata"/> bag. It carries no infrastructure-owned provenance:
/// authoring-cluster identity is owned by the silo via
/// <see cref="LatticeOriginContext"/>, and system / maintenance provenance is
/// owned by <see cref="LatticeMaintenanceContext"/>. A library-internal
/// system-origin call therefore never populates this marker - see
/// <see cref="LatticeCredentialContext"/> for the suppression contract.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeCredential)]
[Immutable]
public readonly record struct LatticeCredential
{
    /// <summary>
    /// Initializes a new <see cref="LatticeCredential"/> carrying the supplied
    /// opaque <paramref name="token"/> and optional selection hints.
    /// </summary>
    /// <param name="token">
    /// The opaque credential / token string an authenticator later resolves
    /// into a subject. Never inspected by the core library.
    /// </param>
    /// <param name="scheme">
    /// Optional scheme / issuer hint (for example <c>"Bearer"</c> or an issuer
    /// URL) letting an authenticator select itself without re-parsing
    /// <paramref name="token"/>. <c>null</c> when unspecified.
    /// </param>
    /// <param name="principalId">
    /// Optional pre-resolved principal identifier, supplied when the edge has
    /// already established the caller's identity and wants to short-circuit
    /// re-resolution on the silo. <c>null</c> when unspecified.
    /// </param>
    /// <param name="metadata">
    /// Optional small metadata bag (for example claims already parsed at the
    /// edge) an authenticator may consult without re-parsing
    /// <paramref name="token"/>. <c>null</c> when unspecified.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="token"/> is <c>null</c>.
    /// </exception>
    public LatticeCredential(
        string token,
        string? scheme = null,
        string? principalId = null,
        IReadOnlyDictionary<string, string>? metadata = null)
    {
        ArgumentNullException.ThrowIfNull(token);
        Token = token;
        Scheme = scheme;
        PrincipalId = principalId;
        Metadata = metadata;
    }

    /// <summary>
    /// The opaque credential / token string the edge stamped. Resolved into a
    /// subject by the Membership layer; never inspected by the core library.
    /// </summary>
    [Id(0)]
    public string Token { get; init; }

    /// <summary>
    /// Optional scheme / issuer hint letting an authenticator select itself
    /// without re-parsing <see cref="Token"/>, or <c>null</c> when unspecified.
    /// </summary>
    [Id(1)]
    public string? Scheme { get; init; }

    /// <summary>
    /// Optional pre-resolved principal identifier established at the edge, or
    /// <c>null</c> when the silo should resolve the principal from
    /// <see cref="Token"/>.
    /// </summary>
    [Id(2)]
    public string? PrincipalId { get; init; }

    /// <summary>
    /// Optional small metadata bag an authenticator may consult without
    /// re-parsing <see cref="Token"/>, or <c>null</c> when unspecified.
    /// </summary>
    [Id(3)]
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }
}
