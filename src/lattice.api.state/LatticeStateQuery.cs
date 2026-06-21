using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;
using Orleans.Runtime;

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
    IOptions<LatticeApiStateOptions> apiOptions,
    IServiceProvider services) : ILatticeStateQuery
{
    private readonly IGrainFactory _grainFactory = grainFactory
        ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly IOptionsMonitor<LatticeOptions> _options = options
        ?? throw new ArgumentNullException(nameof(options));

    private readonly LatticeApiStateOptions _apiOptions = (apiOptions
        ?? throw new ArgumentNullException(nameof(apiOptions))).Value;

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

    public async Task<TreeStructureResult> GetTreeStructureAsync(
        StructureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(request.TreeId);
        if (IsReservedTree(request.TreeId) || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return TreeStructureResult.NotFound(request.TreeId);
        }

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(request.TreeId).ConfigureAwait(false);
        var depthLimit = request.EffectiveDepthLimit;
        var budget = new NodeBudget { Remaining = request.EffectiveMaxNodes };

        // Sub-path descent: bind the named internal node directly and return
        // only its subtree, without reading any unrelated shard.
        if (!string.IsNullOrEmpty(request.SubPathNodeId))
        {
            if (!GrainId.TryParse(request.SubPathNodeId, out var grainId))
            {
                return TreeStructureResult.NotFound(request.TreeId);
            }

            var node = _grainFactory.GetGrain<IBPlusInternalGrain>(grainId);

            // The sub-path node id is an opaque, caller-supplied grain id, and an
            // internal node is keyed only by an unscoped guid - nothing in the id
            // binds it to a tree. Verify the bound node actually belongs to the
            // requested tree before returning its subtree, so a structure query
            // labelled with one tree id cannot be steered at another tree's node.
            var nodeTreeId = await node.GetTreeIdAsync().ConfigureAwait(false);
            if (!string.Equals(nodeTreeId, physicalTreeId, StringComparison.Ordinal))
            {
                return TreeStructureResult.NotFound(request.TreeId);
            }

            var subtree = await node.GetTopologyAsync(depthLimit).ConfigureAwait(false);
            var shardIndex = request.ShardIndex ?? subtree.ShardIndex ?? 0;

            var roots = new List<NodeStateSummary>(1);
            if (budget.Remaining > 0)
            {
                budget.Remaining--;
                roots.Add(MapTopologyNode(subtree, shardIndex, depth: 0, budget));
            }

            return TreeStructureResult.Found(request.TreeId, roots, budget.AnyTruncated);
        }

        var shardCount = await ResolveShardCountAsync(registry, request.TreeId).ConfigureAwait(false);
        var rootNodes = new List<NodeStateSummary>();

        var startShard = request.ShardIndex ?? 0;
        var endShard = request.ShardIndex.HasValue ? request.ShardIndex.Value + 1 : shardCount;

        for (var shardIndex = startShard; shardIndex < endShard; shardIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // One structural read per shard root: O(shards), never per-leaf.
            var shard = _grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var snapshot = await shard.GetTopologySnapshotAsync(depthLimit, cancellationToken).ConfigureAwait(false);
            if (snapshot is null)
            {
                continue;
            }

            if (budget.Remaining <= 0)
            {
                budget.AnyTruncated = true;
                break;
            }

            budget.Remaining--;
            rootNodes.Add(MapTopologyNode(snapshot, shardIndex, depth: 0, budget));
        }

        return TreeStructureResult.Found(request.TreeId, rootNodes, budget.AnyTruncated);
    }

    public async Task<EntryScanResult> ScanEntriesAsync(
        EntryScanRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(request.TreeId);

        var fresh = string.IsNullOrEmpty(request.ContinuationToken);
        string cursorId;
        if (fresh)
        {
            if (IsReservedTree(request.TreeId) || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
            {
                return EntryScanResult.NotFound(request.TreeId);
            }

            // A point-in-time snapshot cursor: every page reads the frozen view
            // captured here, so a multi-page scan never observes a torn write.
            cursorId = request.Predicate is { } predicate
                ? await tree.OpenSnapshotEntryCursorWherePredicateAsync(
                    predicate, request.StartInclusive, request.EndExclusive, request.Reverse, cancellationToken)
                    .ConfigureAwait(false)
                : await tree.OpenSnapshotEntryCursorAsync(
                    request.StartInclusive, request.EndExclusive, request.Reverse, cancellationToken)
                    .ConfigureAwait(false);
        }
        else
        {
            cursorId = request.ContinuationToken!;
        }

        var pageSize = ClampPageSize(request.PageSize);
        var previewBudget = ClampScanPreviewBudget(request.ValuePreviewBudget);

        LatticeCursorEntriesPage page;
        try
        {
            page = await tree.NextEntriesAsync(cursorId, pageSize, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException ex) when (!fresh)
        {
            // A client-supplied continuation token that names an unknown, drained,
            // or already-closed cursor is a malformed request, not a server fault.
            throw new ArgumentException(
                $"The continuation token '{request.ContinuationToken}' is invalid or has expired.",
                nameof(request),
                ex);
        }

        var records = new List<EntryRecord>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            records.Add(await BuildEntryRecordAsync(tree, entry.Key, entry.Value, previewBudget, cancellationToken)
                .ConfigureAwait(false));
        }

        string? continuation = page.HasMore ? cursorId : null;
        if (!page.HasMore)
        {
            // Drained: release the server-side cursor / snapshot pin promptly.
            await tree.CloseCursorAsync(cursorId, cancellationToken).ConfigureAwait(false);
        }

        return EntryScanResult.Found(request.TreeId, records, continuation);
    }

    public async Task<EntryDetailResult> GetEntryAsync(
        string treeId,
        string key,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        if (IsReservedTree(treeId) || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return EntryDetailResult.TreeNotFound(treeId, key);
        }

        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        if (versioned.Value is null)
        {
            // Absent or tombstoned: the public read contract reports both as a
            // missing key for the detail pane.
            return EntryDetailResult.KeyNotFound(treeId, key);
        }

        var record = BuildEntryRecord(
            key,
            versioned.Value,
            versioned.Version,
            versioned.ExpiresAtTicks,
            _apiOptions.SingleEntryValuePreviewBytes);

        return EntryDetailResult.Found(treeId, record);
    }

    private async Task<EntryRecord> BuildEntryRecordAsync(
        ILattice tree,
        string key,
        byte[] value,
        int previewBudget,
        CancellationToken cancellationToken)
    {
        // The cursor page carries the snapshot value bytes (used verbatim for
        // the preview and full length); the per-entry version / TTL is overlaid
        // from a metadata read so the record carries HLC and expiry.
        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        return BuildEntryRecord(key, value, versioned.Version, versioned.ExpiresAtTicks, previewBudget);
    }

    private static EntryRecord BuildEntryRecord(
        string key,
        byte[] value,
        Orleans.Lattice.HybridLogicalClock version,
        long expiresAtTicks,
        int previewBudget)
    {
        var fullLength = value.Length;
        var truncated = fullLength > previewBudget;
        var preview = truncated ? value[..previewBudget] : value;

        return new EntryRecord
        {
            Key = key,
            ValuePreview = preview,
            ValueLength = fullLength,
            Truncated = truncated,
            Hlc = version,
            IsTombstone = false,
            ExpiresAtTicks = expiresAtTicks,
            CrdtShape = null,
        };
    }

    private int ClampPageSize(int requested) => requested switch
    {
        < 1 => _apiOptions.DefaultScanPageSize,
        _ when requested > _apiOptions.MaxScanPageSize => _apiOptions.MaxScanPageSize,
        _ => requested,
    };

    private int ClampScanPreviewBudget(int requested) => requested switch
    {
        < 1 => _apiOptions.DefaultScanValuePreviewBytes,
        _ when requested > _apiOptions.MaxScanValuePreviewBytes => _apiOptions.MaxScanValuePreviewBytes,
        _ => requested,
    };

    private async Task<int> ResolveShardCountAsync(ILatticeRegistry registry, string treeId)
    {
        var entry = await registry.GetEntryAsync(treeId).ConfigureAwait(false);
        return entry?.ShardCount ?? LatticeConstants.DefaultShardCount;
    }

    private sealed class NodeBudget
    {
        public int Remaining;
        public bool AnyTruncated;
    }

    /// <summary>
    /// Maps a core <see cref="ShardTopologyNode"/> into the public
    /// <see cref="NodeStateSummary"/>, expanding children in deterministic
    /// key-range order while the shared node budget allows. Children omitted
    /// because the budget ran out (or because the snapshot itself was
    /// depth-truncated) leave the node flagged
    /// <see cref="NodeStateSummary.HasMoreChildren"/>.
    /// </summary>
    private static NodeStateSummary MapTopologyNode(
        ShardTopologyNode node,
        int shardIndex,
        int depth,
        NodeBudget budget)
    {
        var children = new List<NodeStateSummary>(node.Children.Count);
        var hasMore = node.ChildrenTruncated;

        foreach (var child in node.Children)
        {
            if (budget.Remaining <= 0)
            {
                hasMore = true;
                break;
            }

            budget.Remaining--;
            children.Add(MapTopologyNode(child, shardIndex, depth + 1, budget));
        }

        if (children.Count < node.Children.Count)
        {
            hasMore = true;
        }

        if (hasMore)
        {
            budget.AnyTruncated = true;
        }

        return new NodeStateSummary
        {
            Kind = node.IsLeaf ? NodeKind.Leaf : NodeKind.Internal,
            NodeId = node.NodeId,
            ShardIndex = shardIndex,
            Depth = depth,
            KeyRangeLow = node.LowKeyInclusive,
            KeyRangeHigh = node.HighKeyExclusive,
            ChildCount = node.ChildFanout,
            SubtreeKeyCount = node.LiveCount,
            SubtreeTombstoneCount = node.TombstoneCount,
            SplitInProgress = false,
            HasMoreChildren = hasMore,
            Children = children,
        };
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
