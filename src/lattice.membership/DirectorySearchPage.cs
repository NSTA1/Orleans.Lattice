namespace Orleans.Lattice.Membership;

/// <summary>
/// One page of <see cref="DirectoryPrincipal"/> results returned by
/// <see cref="ILatticeIdentityDirectory.SearchAsync(DirectorySearchQuery, CancellationToken)"/>,
/// carrying the matched principals plus an optional continuation token for a
/// 'load more' follow-up query.
/// </summary>
/// <param name="Principals">
/// The matched principals in provider-defined order. Empty when the query matches
/// nothing; never <c>null</c>.
/// </param>
/// <param name="ContinuationToken">
/// An opaque token to pass as
/// <see cref="DirectorySearchQuery.ContinuationToken"/> on a follow-up query to
/// fetch the next page, or <c>null</c> when this is the final page.
/// </param>
public sealed record DirectorySearchPage(
    IReadOnlyList<DirectoryPrincipal> Principals,
    string? ContinuationToken = null)
{
    /// <summary>
    /// A shared empty page (no principals, no continuation token). Returned by
    /// providers - such as <see cref="NullIdentityDirectory"/> - that match
    /// nothing, so the empty-result path allocates nothing per call.
    /// </summary>
    public static DirectorySearchPage Empty { get; } = new(Array.Empty<DirectoryPrincipal>());
}
