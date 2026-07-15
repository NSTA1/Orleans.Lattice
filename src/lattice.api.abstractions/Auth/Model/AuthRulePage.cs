using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// One page of the authorization rule catalog. Entries are the durable
/// <see cref="LatticeAuthorizationRule"/> policy model surfaced directly, so a
/// binding sees the same rule shape the store persists.
/// <see cref="NextPageToken"/> is the cursor to pass back in the next
/// <see cref="AuthPageRequest"/> to continue enumeration; it is
/// <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthRulePage)]
[Immutable]
public sealed record AuthRulePage
{
    /// <summary>
    /// The rules on this page, ordered by <c>(governed tree id, rule id)</c>.
    /// </summary>
    [Id(0)] public IReadOnlyList<LatticeAuthorizationRule> Entries { get; init; } = Array.Empty<LatticeAuthorizationRule>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
