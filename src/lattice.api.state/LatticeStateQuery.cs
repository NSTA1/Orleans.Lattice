using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Default <see cref="ILatticeStateQuery"/> implementation. Registered as a
/// silo singleton by <c>AddLatticeStateApi</c>; it dials the core
/// <see cref="ILattice"/> grain surface via the cluster grain factory and
/// resolves effective options via the named-options monitor.
/// </summary>
internal sealed class LatticeStateQuery(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> options,
    IServiceProvider services) : ILatticeStateQuery
{
    private readonly IGrainFactory _grainFactory = grainFactory
        ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly IOptionsMonitor<LatticeOptions> _options = options
        ?? throw new ArgumentNullException(nameof(options));

    private readonly IServiceProvider _services = services
        ?? throw new ArgumentNullException(nameof(services));

    public async Task<TreeSummaryResult> GetTreeSummaryAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        if (IsReservedTree(treeId) || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return TreeSummaryResult.NotFound(treeId);
        }

        var report = await tree.DiagnoseAsync(deep, cancellationToken).ConfigureAwait(false);
        return TreeSummaryResult.Found(MapTree(treeId, report, BuildConfig(treeId, report)));
    }

    public async Task<ShardSummariesResult> GetShardSummariesAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        if (IsReservedTree(treeId) || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return ShardSummariesResult.NotFound(treeId);
        }

        var report = await tree.DiagnoseAsync(deep, cancellationToken).ConfigureAwait(false);
        var shards = report.Shards.IsDefault
            ? Array.Empty<ShardStateSummary>()
            : report.Shards
                .OrderBy(s => s.ShardIndex)
                .Select(MapShard)
                .ToArray();

        return ShardSummariesResult.Found(treeId, shards);
    }

    public async Task<TreeCatalogPage> ListTreesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var allIds = await registry.GetAllTreeIdsAsync().ConfigureAwait(false);

        var ordered = allIds
            .Where(id => request.IncludeSystemTrees || !IsReservedTree(id))
            .Where(id => request.PageToken is null || string.CompareOrdinal(id, request.PageToken) > 0)
            .OrderBy(id => id, StringComparer.Ordinal);

        var pageSize = request.EffectivePageSize;
        var entries = new List<TreeCatalogEntry>(pageSize);
        string? nextToken = null;

        foreach (var id in ordered)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entries.Count == pageSize)
            {
                nextToken = entries[^1].TreeId;
                break;
            }

            var entry = await registry.GetEntryAsync(id).ConfigureAwait(false);
            var deletion = _grainFactory.GetGrain<ITreeDeletionGrain>(id);
            var isDeleted = await deletion.IsDeletedAsync().ConfigureAwait(false);

            entries.Add(MapCatalogEntry(id, entry, isDeleted));
        }

        return new TreeCatalogPage { Entries = entries, NextPageToken = nextToken };
    }

    public async Task<ViewCatalogPage> ListViewsAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        var registrations = await CollectViewsAsync(cancellationToken).ConfigureAwait(false);

        var ordered = registrations
            .Where(r => request.PageToken is null || string.CompareOrdinal(r.ViewName, request.PageToken) > 0)
            .OrderBy(r => r.ViewName, StringComparer.Ordinal)
            .ToArray();

        var pageSize = request.EffectivePageSize;
        var factory = request.IncludeViewStats ? _services.GetService<ILatticeViewFactory>() : null;
        var entries = new List<ViewStateSummary>(Math.Min(pageSize, ordered.Length));
        string? nextToken = null;

        foreach (var registration in ordered)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entries.Count == pageSize)
            {
                nextToken = entries[^1].ViewName;
                break;
            }

            long? lag = null;
            long? entryCount = null;
            if (factory is not null)
            {
                (lag, entryCount) = await SampleViewAsync(factory, registration.ViewName, cancellationToken)
                    .ConfigureAwait(false);
            }

            entries.Add(new ViewStateSummary
            {
                ViewName = registration.ViewName,
                SourceTreeId = registration.SourceTreeId,
                Lag = lag,
                EntryCount = entryCount,
                IsAggregation = registration.IsAggregation,
            });
        }

        return new ViewCatalogPage { Entries = entries, NextPageToken = nextToken };
    }

    private static async Task<(long? Lag, long? EntryCount)> SampleViewAsync(
        ILatticeViewFactory factory,
        string viewName,
        CancellationToken cancellationToken)
    {
        try
        {
            var view = await factory.GetAsync(viewName, cancellationToken).ConfigureAwait(false);
            if (view is null)
            {
                return (null, null);
            }

            var lag = await view.GetLagAsync(cancellationToken).ConfigureAwait(false);
            var count = await view.CountAsync(cancellationToken).ConfigureAwait(false);
            return (lag, count);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // A view whose maintainer cannot be sampled (e.g. a ship-view consumer
            // or a transient activation failure) is still listed; its statistics
            // are simply reported as unavailable.
            return (null, null);
        }
    }

    private static bool IsReservedTree(string treeId) =>
        treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
        || treeId.StartsWith(LatticeConstants.ViewTreePrefix, StringComparison.Ordinal);

    /// <summary>
    /// Collects the cluster-wide set of materialised views, deduplicated by name.
    /// The local <see cref="IViewCatalog"/> only holds the views that have been
    /// activated on the serving silo (every startup-declared view plus any runtime
    /// view that has rehydrated here), so runtime views created on another silo are
    /// missed by a single-silo enumeration. The durable
    /// <see cref="IViewRegistryGrain"/> holds every runtime view cluster-wide; the
    /// union of the two, with the local catalog winning on a name conflict (startup
    /// declarations are authoritative and never recorded in the registry), yields a
    /// consistent listing regardless of which silo serves the request.
    /// </summary>
    private async Task<IReadOnlyCollection<ViewListing>> CollectViewsAsync(CancellationToken cancellationToken)
    {
        var byName = new Dictionary<string, ViewListing>(StringComparer.Ordinal);

        var registry = _grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
        try
        {
            var runtime = await registry.ListAsync().ConfigureAwait(false);
            foreach (var registration in runtime)
            {
                byName[registration.ViewName] = new ViewListing(
                    registration.ViewName,
                    registration.SourceTreeId,
                    registration.IsAggregation);
            }
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // A transient registry-activation failure must not blind the listing to
            // the locally-known views, so fall back to the catalog-only union.
        }

        var catalog = _services.GetService<IViewCatalog>();
        if (catalog is not null)
        {
            foreach (var registration in catalog.All())
            {
                byName[registration.ViewName] = new ViewListing(
                    registration.ViewName,
                    registration.SourceTreeId,
                    registration.IsAggregation);
            }
        }

        return byName.Values;
    }

    private readonly record struct ViewListing(string ViewName, string SourceTreeId, bool IsAggregation);

    private TreeCatalogEntry MapCatalogEntry(string treeId, TreeRegistryEntry? entry, bool isDeleted)
    {
        var physicalTreeId = entry?.PhysicalTreeId;
        var opts = _options.Get(treeId);
        var shardCount = entry?.ShardCount ?? LatticeConstants.DefaultShardCount;

        return new TreeCatalogEntry
        {
            TreeId = treeId,
            IsAlias = physicalTreeId is not null,
            PhysicalTreeId = physicalTreeId,
            Lifecycle = isDeleted ? TreeLifecycleState.SoftDeleted : TreeLifecycleState.Active,
            ShardCount = shardCount,
            Config = new TreeConfigSummary
            {
                ShardCount = shardCount,
                VirtualShardCount = LatticeConstants.DefaultVirtualShardCount,
                MaxLeafKeys = entry?.MaxLeafKeys ?? LatticeConstants.DefaultMaxLeafKeys,
                MaxInternalChildren = entry?.MaxInternalChildren ?? LatticeConstants.DefaultMaxInternalChildren,
                WalPartitions = entry?.WalPartitions ?? opts.WalPartitions,
                SoftDeleteDuration = opts.SoftDeleteDuration,
            },
        };
    }

    private TreeConfigSummary BuildConfig(string treeId, TreeDiagnosticReport report)
    {
        var opts = _options.Get(treeId);
        return new TreeConfigSummary
        {
            ShardCount = report.ShardCount,
            VirtualShardCount = report.VirtualShardCount,
            WalPartitions = opts.WalPartitions,
            SoftDeleteDuration = opts.SoftDeleteDuration,
        };
    }

    private static TreeStateSummary MapTree(string treeId, TreeDiagnosticReport report, TreeConfigSummary config)
    {
        var minDepth = 0;
        var maxDepth = 0;
        var splitting = 0;
        if (!report.Shards.IsDefaultOrEmpty)
        {
            minDepth = int.MaxValue;
            foreach (var shard in report.Shards)
            {
                if (shard.Depth < minDepth) minDepth = shard.Depth;
                if (shard.Depth > maxDepth) maxDepth = shard.Depth;
                if (shard.SplitInProgress) splitting++;
            }
        }

        return new TreeStateSummary
        {
            TreeId = treeId,
            Lifecycle = TreeLifecycleState.Active,
            ShardCount = report.ShardCount,
            TotalLiveKeys = report.TotalLiveKeys,
            TombstoneCount = report.TotalTombstones,
            MinDepth = minDepth == int.MaxValue ? 0 : minDepth,
            MaxDepth = maxDepth,
            ShardsSplitting = splitting,
            Config = config,
            SampledAt = report.SampledAt,
        };
    }

    private static ShardStateSummary MapShard(ShardDiagnosticReport shard) => new()
    {
        ShardIndex = shard.ShardIndex,
        Depth = shard.Depth,
        RootIsLeaf = shard.RootIsLeaf,
        LiveKeys = shard.LiveKeys,
        Tombstones = shard.Tombstones,
        OpsPerSecond = shard.OpsPerSecond,
        SplitInProgress = shard.SplitInProgress,
    };
}
