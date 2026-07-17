namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// Wire response wrapping a nullable directory-principal lookup. gRPC unary
/// responses cannot be <see langword="null"/>, so
/// <c>ResolveDirectoryPrincipalAsync</c> returns this envelope with
/// <see cref="Principal"/> set to the resolved descriptor or
/// <see langword="null"/> when no such principal exists (or no directory is
/// configured).
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthDirectoryPrincipalResult)]
[Immutable]
public sealed record AuthDirectoryPrincipalResult
{
    /// <summary>The resolved principal, or <see langword="null"/> when none exists.</summary>
    [Id(0)] public DirectoryPrincipalDescriptor? Principal { get; init; }
}
