namespace Orleans.Lattice.Membership;

/// <summary>
/// A typeahead / browse query issued against <see cref="ILatticeIdentityDirectory"/>.
/// A lightweight value type: the search term, an optional principal-kind filter,
/// the requested page size, and an optional continuation token carried back from
/// a prior <see cref="DirectorySearchPage"/> to fetch the next page.
/// </summary>
/// <param name="Term">
/// The search term to match against principal ids / display names. An empty term
/// requests an unfiltered browse (the first page of all principals), subject to
/// the provider's own conventions.
/// </param>
/// <param name="Kind">
/// An optional filter restricting results to users or groups only. <c>null</c>
/// (the default) returns both kinds.
/// </param>
/// <param name="PageSize">
/// The requested maximum number of principals in the page. <c>0</c> (the default)
/// asks the provider to apply its configured default page size.
/// </param>
/// <param name="ContinuationToken">
/// An opaque continuation token from a prior <see cref="DirectorySearchPage"/>, or
/// <c>null</c> (the default) to request the first page.
/// </param>
public readonly record struct DirectorySearchQuery(
    string Term,
    DirectoryPrincipalKind? Kind = null,
    int PageSize = 0,
    string? ContinuationToken = null);
