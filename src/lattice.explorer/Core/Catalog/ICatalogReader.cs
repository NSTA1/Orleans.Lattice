namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Reads the cluster catalog through the shared state-API connection and
/// projects it into the uniform <see cref="CatalogItem"/> shape the navigation
/// panel renders. The reader treats ids opaquely and never relies on any
/// internal naming convention.
/// </summary>
public interface ICatalogReader
{
    /// <summary>
    /// Loads a single page of the requested catalog kind. Pass the previous
    /// page's <see cref="CatalogPage.NextPageToken"/> to continue enumeration,
    /// or <see langword="null"/> to start from the beginning.
    /// </summary>
    Task<CatalogPage> LoadAsync(
        CatalogKind kind,
        string? pageToken,
        int pageSize,
        CancellationToken cancellationToken = default);
}
