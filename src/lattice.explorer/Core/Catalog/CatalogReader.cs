using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Default <see cref="ICatalogReader"/> that fans out to the state API's
/// discovery endpoints and maps the responses to <see cref="CatalogItem"/>s.
/// </summary>
public sealed class CatalogReader(ILatticeStateClient client) : ICatalogReader
{
    private readonly ILatticeStateClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<CatalogPage> LoadAsync(
        CatalogKind kind,
        string? pageToken,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        var request = new CatalogRequest
        {
            PageSize = pageSize,
            PageToken = pageToken,
        };

        return kind switch
        {
            CatalogKind.Trees => await LoadTreesAsync(request, cancellationToken).ConfigureAwait(false),
            CatalogKind.Views => await LoadViewsAsync(request, cancellationToken).ConfigureAwait(false),
            CatalogKind.TagIndexes => await LoadTagIndexesAsync(request, cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown catalog kind."),
        };
    }

    private async Task<CatalogPage> LoadTreesAsync(CatalogRequest request, CancellationToken cancellationToken)
    {
        var page = await _client.ListTreesAsync(request, cancellationToken).ConfigureAwait(false);

        var items = new List<CatalogItem>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            items.Add(new CatalogItem
            {
                Id = entry.TreeId,
                Kind = CatalogKind.Trees,
                Lifecycle = entry.Lifecycle.ToString(),
                ShardCount = entry.ShardCount,
            });
        }

        return new CatalogPage { Items = items, NextPageToken = page.NextPageToken };
    }

    private async Task<CatalogPage> LoadViewsAsync(CatalogRequest request, CancellationToken cancellationToken)
    {
        var page = await _client.ListViewsAsync(request, cancellationToken).ConfigureAwait(false);

        var items = new List<CatalogItem>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            items.Add(new CatalogItem
            {
                Id = entry.ViewName,
                Kind = CatalogKind.Views,
                SourceTreeId = entry.SourceTreeId,
                IsAggregation = entry.IsAggregation,
            });
        }

        return new CatalogPage { Items = items, NextPageToken = page.NextPageToken };
    }

    private async Task<CatalogPage> LoadTagIndexesAsync(CatalogRequest request, CancellationToken cancellationToken)
    {
        var page = await _client.ListTagIndexesAsync(request, cancellationToken).ConfigureAwait(false);

        var items = new List<CatalogItem>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            items.Add(new CatalogItem
            {
                Id = entry.TreeId,
                Kind = CatalogKind.TagIndexes,
                IndexName = entry.IndexName,
                ShardCount = entry.ShardCount,
            });
        }

        return new CatalogPage { Items = items, NextPageToken = page.NextPageToken };
    }
}
