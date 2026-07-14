namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// A group record as surfaced by the admin facade: a stable
/// <see cref="GroupId"/> and an optional human-readable
/// <see cref="DisplayName"/>. The transport-agnostic, serializable wire form of
/// the membership directory's group record. Group <em>membership</em> edges are
/// administered separately through the facade's add / remove member operations.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthGroup)]
[Immutable]
public sealed record AuthGroup
{
    /// <summary>The stable group id (the directory key).</summary>
    [Id(0)] public required string GroupId { get; init; }

    /// <summary>An optional human-readable display name.</summary>
    [Id(1)] public string? DisplayName { get; init; }
}
