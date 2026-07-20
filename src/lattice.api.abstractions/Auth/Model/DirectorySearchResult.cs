namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// One page of identity-directory search results returned by
/// <see cref="ILatticeAuthAdmin.SearchDirectoryAsync"/>: the matched principals,
/// an optional continuation token for a 'load more' follow-up, and an
/// <see cref="Available"/> flag distinguishing 'no principals matched' from 'no
/// identity directory is configured'.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.DirectorySearchResult)]
[Immutable]
public sealed record DirectorySearchResult
{
    /// <summary>
    /// The matched principals in provider-defined order. Empty when the query
    /// matched nothing, or when no directory is configured; never
    /// <see langword="null"/>.
    /// </summary>
    [Id(0)] public IReadOnlyList<DirectoryPrincipalDescriptor> Principals { get; init; }
        = Array.Empty<DirectoryPrincipalDescriptor>();

    /// <summary>
    /// An opaque token to pass as
    /// <see cref="DirectorySearchRequest.ContinuationToken"/> on a follow-up
    /// query to fetch the next page, or <see langword="null"/> when this is the
    /// final page.
    /// </summary>
    [Id(1)] public string? ContinuationToken { get; init; }

    /// <summary>
    /// <see langword="true"/> when a searchable identity directory is configured
    /// and produced this page; <see langword="false"/> when no directory is
    /// available, in which case <see cref="Principals"/> is always empty. Lets a
    /// UI distinguish 'nothing matched' from 'directory validation is off'.
    /// </summary>
    [Id(2)] public bool Available { get; init; }

    /// <summary>
    /// The shared result returned when no searchable identity directory is
    /// configured: no principals, no continuation token, and
    /// <see cref="Available"/> is <see langword="false"/>.
    /// </summary>
    public static DirectorySearchResult Unavailable { get; } = new();
}
