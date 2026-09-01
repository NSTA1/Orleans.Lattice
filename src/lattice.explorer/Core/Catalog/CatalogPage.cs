namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// One page of navigation items together with the continuation cursor returned
/// by the state API. The panel appends pages as the user loads more.
/// </summary>
public sealed record CatalogPage
{
    /// <summary>The items on this page, in catalog order.</summary>
    public IReadOnlyList<CatalogItem> Items { get; init; } = Array.Empty<CatalogItem>();

    /// <summary>
    /// The tenant this page was read under, or <see langword="null"/> when it was
    /// not tenant-scoped.
    /// </summary>
    public string? ScopedToTenantId { get; init; }

    /// <summary>
    /// How many items the tenant scope removed from the server's page.
    /// </summary>
    /// <remarks>
    /// Reported so a caller can tell an empty list that was <em>filtered</em> from
    /// one that was empty already. Scoping used to be applied and then discarded,
    /// which made those two byte-identical, so the catalog could only ever say
    /// "nothing here" - the least useful of the two answers, and the wrong one
    /// under a tenant scope.
    /// <para>
    /// Judge "scoped out" on this count being non-zero, never on
    /// <see cref="ScopedToTenantId"/> being set: a tenant-scoped cluster that
    /// genuinely holds nothing must still say it is empty. Claiming a filter that
    /// removed nothing is the same class of untruth as hiding one that did.
    /// </para>
    /// </remarks>
    public int ScopeFilteredCount { get; init; }

    /// <summary>
    /// The continuation cursor to pass back for the next page, or
    /// <see langword="null"/> when this is the final page.
    /// </summary>
    public string? NextPageToken { get; init; }

    /// <summary><see langword="true"/> when more pages are available.</summary>
    public bool HasMore => NextPageToken is not null;
}
