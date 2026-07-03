namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// One page of the group catalog. <see cref="NextPageToken"/> is the cursor to
/// pass back in the next <see cref="AuthPageRequest"/> to continue enumeration;
/// it is <see langword="null"/> on the final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.AuthGroupPage)]
[Immutable]
public sealed record AuthGroupPage
{
    /// <summary>The group records on this page, ordered by group id.</summary>
    [Id(0)] public IReadOnlyList<AuthGroup> Entries { get; init; } = Array.Empty<AuthGroup>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/>
    /// when this is the last page.
    /// </summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
