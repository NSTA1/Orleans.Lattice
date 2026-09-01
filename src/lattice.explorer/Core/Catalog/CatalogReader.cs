using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Default <see cref="ICatalogReader"/> that fans out to the state API's
/// discovery endpoints and maps the responses to <see cref="CatalogItem"/>s.
/// </summary>
public sealed class CatalogReader : ICatalogReader
{
    /// <summary>
    /// Reserved tree-name prefix for materialised-view trees, mirrored from
    /// <c>LatticeConstants.ViewTreePrefix</c>. The explorer's Core project must not
    /// reference Orleans.Lattice, so the literal is held locally; a view named
    /// <c>v1</c> is physically the tree <c>view-v1</c>, and the detail tabs query
    /// that physical id while the list shows the bare name.
    /// </summary>
    private const string ViewTreePrefix = "view-";

    private static readonly Func<CatalogItem, string> TreeIdOf = static item => item.Id;

    private readonly ILatticeStateClient _client;
    private readonly IExplorerTenantView _tenantView;

    /// <summary>
    /// Creates a catalog reader over the state API <paramref name="client"/>. When
    /// <paramref name="tenantView"/> is supplied and active, the trees catalog is
    /// scoped to the caller's active tenant; when it is <see langword="null"/> or
    /// inactive (the default when tenant scoping is not enabled) the catalog is
    /// returned exactly as the server sent it, so a non-tenant cluster is
    /// byte-for-byte unchanged.
    /// </summary>
    /// <param name="client">The read-only state API client. Must not be <see langword="null"/>.</param>
    /// <param name="tenantView">
    /// The optional fail-closed tenant-view seam. Defaults to <see langword="null"/>,
    /// treated as the inactive view.
    /// </param>
    public CatalogReader(ILatticeStateClient client, IExplorerTenantView? tenantView = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _tenantView = tenantView ?? NullExplorerTenantView.Instance;
    }

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
                RestoreShadowOfTreeId = entry.RestoreShadowOfTreeId,
            });
        }

        // Scope the trees catalog to the caller's active tenant. When tenant
        // scoping is not enabled the view is inactive and this branch is skipped
        // entirely, so the catalog is byte-for-byte identical to a non-tenant
        // cluster (the server already scopes a tenant caller's catalog; this is
        // the client-side view layer and the operator all-tenant toggle).
        IReadOnlyList<CatalogItem> scoped = items;
        var scopedToTenantId = (string?)null;
        var scopeFilteredCount = 0;
        if (_tenantView.IsActive)
        {
            scoped = await _tenantView.ScopeAsync(items, TreeIdOf, cancellationToken).ConfigureAwait(false);
            scopedToTenantId = _tenantView.ActiveTenant?.Value;
            scopeFilteredCount = items.Count - scoped.Count;
        }

        return new CatalogPage
        {
            Items = scoped,
            NextPageToken = page.NextPageToken,
            ScopedToTenantId = scopedToTenantId,
            ScopeFilteredCount = scopeFilteredCount,
        };
    }

    private async Task<CatalogPage> LoadViewsAsync(CatalogRequest request, CancellationToken cancellationToken)
    {
        var page = await _client.ListViewsAsync(request, cancellationToken).ConfigureAwait(false);

        var items = new List<CatalogItem>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            items.Add(new CatalogItem
            {
                Id = $"{ViewTreePrefix}{entry.ViewName}",
                DisplayName = entry.ViewName,
                Kind = CatalogKind.Views,
                SourceTreeId = entry.SourceTreeId,
                IsAggregation = entry.IsAggregation,
                IsHistory = entry.IsHistory,
                ProjectionProviderKey = entry.ProjectionProviderKey,
                ProjectionVersion = entry.ProjectionVersion,
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
