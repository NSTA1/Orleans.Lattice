using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// A typeahead / browse request against the configured identity directory,
/// forwarded by <see cref="ILatticeAuthAdmin.SearchDirectoryAsync"/> to validate
/// candidate principal ids before an operator grants them access. The
/// transport-agnostic, serializable wire form of the membership layer's
/// <see cref="DirectorySearchQuery"/>.
/// </summary>
[GenerateSerializer]
[Alias(ApiAuthTypeAliases.DirectorySearchRequest)]
[Immutable]
public sealed record DirectorySearchRequest
{
    /// <summary>
    /// The search term to match against principal ids / display names. An empty
    /// term (the default) requests an unfiltered browse of the first page,
    /// subject to the provider's own conventions.
    /// </summary>
    [Id(0)] public string Term { get; init; } = string.Empty;

    /// <summary>
    /// An optional filter restricting results to users or groups only.
    /// <see langword="null"/> (the default) returns both kinds.
    /// </summary>
    [Id(1)] public DirectoryPrincipalKind? Kind { get; init; }

    /// <summary>
    /// The requested maximum number of principals in the page. <c>0</c> (the
    /// default) asks the provider to apply its configured default page size.
    /// </summary>
    [Id(2)] public int PageSize { get; init; }

    /// <summary>
    /// An opaque continuation token from a prior
    /// <see cref="DirectorySearchResult"/>, or <see langword="null"/> (the
    /// default) to request the first page.
    /// </summary>
    [Id(3)] public string? ContinuationToken { get; init; }
}
