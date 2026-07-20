namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire request naming a single directory principal by its exact id. A
/// serializable envelope for <c>ResolveDirectoryPrincipalAsync</c>, whose only
/// argument is a principal id.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthPrincipalRef)]
[Immutable]
public sealed record AuthPrincipalRef
{
    /// <summary>The exact directory principal id to resolve.</summary>
    [Id(0)] public required string PrincipalId { get; init; }
}
