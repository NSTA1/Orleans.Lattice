namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// One authentication scheme the tenant-administration control-API endpoint
/// advertises to a connecting client: a stable id the client matches to a login
/// provider, a friendly display name, and the public parameters the provider
/// needs to run the challenge (for example an Entra authority, tenant, client id,
/// and audience).
/// </summary>
/// <remarks>
/// This descriptor carries only public configuration. It is returned by the
/// unauthenticated advertisement RPC, so it must never contain a secret, a
/// signing key, or any user-specific data - only the values a client already
/// needs (and could discover from the OIDC metadata endpoint) to begin an
/// interactive sign-in.
/// </remarks>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.AuthSchemeDescriptor)]
[Immutable]
public sealed record AuthSchemeDescriptor
{
    /// <summary>The stable scheme id (for example <c>basic</c> or <c>entra</c>).</summary>
    [Id(0)] public required string SchemeId { get; init; }

    /// <summary>A friendly, human-readable name for the scheme.</summary>
    [Id(1)] public string DisplayName { get; init; } = string.Empty;

    /// <summary>The public parameters a client needs to run the challenge.</summary>
    [Id(2)] public IReadOnlyDictionary<string, string> Parameters { get; init; } =
        new Dictionary<string, string>(StringComparer.Ordinal);
}
