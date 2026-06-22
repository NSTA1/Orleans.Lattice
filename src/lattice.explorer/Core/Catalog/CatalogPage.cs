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
    /// The continuation cursor to pass back for the next page, or
    /// <see langword="null"/> when this is the final page.
    /// </summary>
    public string? NextPageToken { get; init; }

    /// <summary><see langword="true"/> when more pages are available.</summary>
    public bool HasMore => NextPageToken is not null;
}
