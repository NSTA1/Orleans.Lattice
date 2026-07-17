using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// A principal (user or group) as it exists in the configured external identity
/// source, surfaced by <see cref="ILatticeAuthAdmin.SearchDirectoryAsync"/> and
/// <see cref="ILatticeAuthAdmin.ResolveDirectoryPrincipalAsync"/>. The
/// transport-agnostic, serializable wire form of the membership layer's
/// <see cref="DirectoryPrincipal"/> (which is transport-free by design and never
/// crosses a wire), so the facade owns its own DTO rather than leaking the
/// server-only model.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.DirectoryPrincipalDescriptor)]
[Immutable]
public sealed record DirectoryPrincipalDescriptor
{
    /// <summary>The stable principal id in the external identity source.</summary>
    [Id(0)] public required string Id { get; init; }

    /// <summary>A human-readable display name for the principal.</summary>
    [Id(1)] public required string DisplayName { get; init; }

    /// <summary>Whether the principal is a user or a group.</summary>
    [Id(2)] public DirectoryPrincipalKind Kind { get; init; }

    /// <summary>
    /// An optional flat claim bag carried from the identity source, or
    /// <see langword="null"/> when the provider surfaces no claims for the
    /// principal.
    /// </summary>
    [Id(3)] public IReadOnlyDictionary<string, string>? Claims { get; init; }
}
