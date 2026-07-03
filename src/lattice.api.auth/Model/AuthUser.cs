namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// A user record as surfaced by the admin facade: a stable
/// <see cref="UserId"/>, an optional human-readable <see cref="DisplayName"/>,
/// and an optional flat claim bag. The transport-agnostic, serializable wire
/// form of the membership directory's user record.
/// </summary>
/// <remarks>
/// Deliberately <b>not</b> marked <c>[Immutable]</c>: it carries a caller-owned
/// <see cref="Claims"/> dictionary on the write path, so leaving the type
/// copy-eligible forces Orleans to deep-copy it across the grain-proxy boundary
/// rather than alias the caller's instance.
/// </remarks>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthUser)]
public sealed record AuthUser
{
    /// <summary>The stable user id (the directory key).</summary>
    [Id(0)] public required string UserId { get; init; }

    /// <summary>An optional human-readable display name.</summary>
    [Id(1)] public string? DisplayName { get; init; }

    /// <summary>An optional flat claim bag stored alongside the user.</summary>
    [Id(2)] public IReadOnlyDictionary<string, string>? Claims { get; init; }
}
