namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// One page of the user catalog. <see cref="NextPageToken"/> is the cursor to
/// pass back in the next <see cref="AuthPageRequest"/> to continue enumeration;
/// it is <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthUserPage)]
[Immutable]
public sealed record AuthUserPage
{
    /// <summary>The user records on this page, ordered by user id.</summary>
    [Id(0)] public IReadOnlyList<AuthUser> Entries { get; init; } = Array.Empty<AuthUser>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
