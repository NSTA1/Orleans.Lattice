using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Schema;
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

    private readonly LatticeStateVisibilityFilter _visibility =
        new(services, (apiOptions ?? throw new ArgumentNullException(nameof(apiOptions))).Value);

    /// <summary>
    /// Applies auth-backed visibility to a tree-scoped read. Resolves the caller
    /// subject (only when visibility is enabled) and returns <see langword="true"/>
    /// when the read must be refused - reported as not-found - because the subject
    /// may not read <paramref name="treeId"/>. Returns <see langword="false"/>
    /// (proceed as normal) when visibility is disabled (zero cost, no subject
    /// resolution), when the tree is a reserved system / materialised-view tree
    /// governed elsewhere, or when the subject has read access. A partial (prefix)
    /// grant proceeds: the gated <see cref="ILattice"/> data-plane surface prunes
    /// the individual keys the subject may not observe.
    /// </summary>
    private async ValueTask<bool> IsTreeReadHiddenAsync(string treeId, CancellationToken cancellationToken)
    {
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (subject is not { } resolved)
        {
            return false;
        }

        // A materialised-view (view-*) tree read is authorised by the readability
        // of its SOURCE tree, mirroring ListViewsAsync. This is essential: the
        // read paths open a ViewReadContext scope that makes the access gate
        // bypass itself (LatticeAccessGateContext.IsGateBypassed), so the source
        // tree's read grant is the ONLY authorization boundary a view read has. An
        // anonymous subject has no read grant on any source and is refused; a
        // subject that can read the source sees the view; a prefix-granted subject
        // still passes here and the gated data-plane surface prunes the individual
        // keys it may not observe. Fail closed (hidden) when the source cannot be
        // resolved.
        if (IsViewTree(treeId))
        {
            var sourceTreeId = await ResolveViewSourceTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
            if (sourceTreeId is null)
            {
                return true;
            }

            return !await _visibility.CanReadAnyKeyAsync(sourceTreeId, resolved, cancellationToken).ConfigureAwait(false);
        }

        // System trees are already hidden at each call site by the existence
        // checks; data-tree policy does not gate them here.
        if (IsSystemTree(treeId))
        {
            return false;
        }

        return !await _visibility.CanReadAnyKeyAsync(treeId, resolved, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the source tree id backing a materialised-view (<c>view-*</c>)
    /// tree so a view read can be gated by the readability of its source. Prefers
    /// the allocation-free local <see cref="IViewCatalog"/> (every startup-declared
    /// view and any runtime view rehydrated on this silo), then falls back to the
    /// durable cluster-wide <see cref="IViewRegistryGrain"/> for a runtime view
    /// created on another silo. Returns <see langword="null"/> when the view name
    /// cannot be recovered or the source cannot be resolved, so the caller fails
    /// closed and hides the view rather than leaking it.
    /// </summary>
    private async ValueTask<string?> ResolveViewSourceTreeIdAsync(string treeId, CancellationToken cancellationToken)
    {
        var viewName = ViewNameFromTreeId(treeId);
        if (viewName.Length == 0)
        {
            return null;
        }

        var local = _services.GetService<IViewCatalog>()?.TryGet(viewName);
        if (local is { } registration)
        {
            return registration.SourceTreeId;
        }

        try
        {
            IReadOnlyList<RuntimeViewRegistration> runtime;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                var registry = _grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
                runtime = await registry.ListAsync().ConfigureAwait(false);
            }

            foreach (var reg in runtime)
            {
                if (string.Equals(reg.ViewName, viewName, StringComparison.Ordinal))
                {
                    return reg.SourceTreeId;
                }
            }
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // Fail closed below on any transient registry-activation failure.
        }

        return null;
    }

    /// <summary>
    /// Decides whether a catalog entry may be surfaced to a subject with
    /// visibility enabled: a materialised-view tree is gated by the readability of
    /// its source (so it never leaks the existence of data over an unreadable
    /// source), and every other reserved / ordinary tree is gated by its own read
    /// grant (so a system tree is visible only to a caller that may read it). Fails
    /// closed when a view's source cannot be resolved.
    /// </summary>
    private async ValueTask<bool> IsCatalogEntryVisibleAsync(
        string treeId,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        if (IsViewTree(treeId))
        {
            var sourceTreeId = await ResolveViewSourceTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
            return sourceTreeId is not null
                && await _visibility.CanReadAnyKeyAsync(sourceTreeId, subject, cancellationToken).ConfigureAwait(false);
        }

        return await _visibility.CanReadAnyKeyAsync(treeId, subject, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves the per-key read predicate that must be applied to a
    /// materialised-view (<c>view-*</c>) data read for the current caller. A view
    /// read binds under a <c>ViewReadContext</c> scope that bypasses the data-plane
    /// access gate, so - unlike an ordinary tree, whose gated cursor / point read
    /// prunes unreadable keys automatically - the source tree's partial (prefix)
    /// grant is not otherwise enforced on the view rows. This resolves the source
    /// tree's key filter so the read paths can prune the view to exactly the keys
    /// the caller may read on the source. Returns <see langword="null"/> when no
    /// per-key pruning is required: visibility disabled, an anonymous / unresolved
    /// caller (the whole-view hidden check has already refused that read), the tree
    /// is not a view, the source cannot be resolved, or the caller holds a
    /// whole-tree read grant on the source (a <see langword="null"/> filter admits
    /// every key). The predicate keeps a key when it returns <see langword="true"/>.
    /// </summary>
    private async ValueTask<Func<string, bool>?> ResolveViewKeyFilterAsync(
        string treeId,
        CancellationToken cancellationToken)
    {
        if (!_visibility.Enabled || !IsViewTree(treeId))
        {
            return null;
        }

        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (subject is not { } resolved)
        {
            return null;
        }

        var sourceTreeId = await ResolveViewSourceTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (sourceTreeId is null)
        {
            return null;
        }

        var (_, keyFilter) = await _visibility
            .ResolveTreeReadAccessAsync(sourceTreeId, resolved, cancellationToken)
            .ConfigureAwait(false);
        return keyFilter;
    }

    public async Task<TreeSummaryResult> GetTreeSummaryAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (await IsTreeReadHiddenAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            return TreeSummaryResult.NotFound(treeId);
        }

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

        // A materialised view is a read-only tree backed by a real shard set, so
        // its metrics are legitimately inspectable (the Explorer Metrics tab reads
        // them). Bind under an authorised view-read scope and resolve the active
        // generation, exactly as the entry read paths do; only system trees stay
        // hidden. The result keeps the requested id so the caller keys on it.
        if (await IsTreeReadHiddenAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            return ShardSummariesResult.NotFound(treeId);
        }

        using var viewScope = OpenViewReadScopeIfNeeded(treeId);
        var bindTreeId = await ResolveReadTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        var tree = bindTreeId is null ? null : _grainFactory.GetGrain<ILattice>(bindTreeId);
        if (IsSystemTree(treeId) || tree is null || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
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

    public async Task<int?> GetPhysicalShardCountAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Mirror GetShardSummariesAsync's view handling so the saturated-tree
        // degraded metrics path also serves materialised views rather than
        // dropping them; only system trees stay hidden.
        if (await IsTreeReadHiddenAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        using var viewScope = OpenViewReadScopeIfNeeded(treeId);
        var bindTreeId = await ResolveReadTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        var tree = bindTreeId is null ? null : _grainFactory.GetGrain<ILattice>(bindTreeId);
        if (IsSystemTree(treeId) || tree is null || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        // Routing carries the physical shard map, so this is one grain call with
        // no per-shard fan-out - safe against a saturated tree's contended roots.
        var routing = await tree.GetRoutingAsync(cancellationToken).ConfigureAwait(false);
        return routing.Map.GetPhysicalShardIndices().Count;
    }

    public async Task<TreeCatalogPage> ListTreesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        // Resolve the caller subject once for the whole page. Null means
        // visibility is disabled (no auth gate registered, or opted out): the
        // catalog is returned unfiltered exactly as before, at zero cost.
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Enumerating the registry is infrastructure: run it under a system-origin
        // scope so the enumeration is not itself filtered or denied by the gate.
        IEnumerable<string> allIds;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            allIds = await registry.GetAllTreeIdsAsync().ConfigureAwait(false);
        }

        var ordered = allIds
            .Where(id => !IsTagIndexTree(id))
            .Where(id => request.IncludeSystemTrees || (!IsReservedTree(id) && !IsSystemDataTree(id)))
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

            // Scope the catalog to trees the subject may observe: omit any
            // ordinary data tree the caller has no read access to, gate a
            // materialised-view tree by the readability of its SOURCE (so it never
            // leaks the existence of data over an unreadable source, mirroring
            // ListViewsAsync), and gate a system tree by its own read grant (so it
            // is visible only to a caller that may read it). An anonymous subject
            // can observe none. Visibility disabled (subject null) returns the
            // catalog unfiltered exactly as before, at zero cost.
            if (subject is { } resolved
                && !await IsCatalogEntryVisibleAsync(id, resolved, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            TreeRegistryEntry? entry;
            bool isDeleted;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                entry = await registry.GetEntryAsync(id).ConfigureAwait(false);
                var deletion = _grainFactory.GetGrain<ITreeDeletionGrain>(id);
                isDeleted = await deletion.IsDeletedAsync().ConfigureAwait(false);
            }

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

        // Resolve the caller subject once; null means visibility is disabled and
        // the view catalog is returned unfiltered exactly as before.
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);

        // Enumerating the view registry is infrastructure: run it under a
        // system-origin scope so it is not itself filtered by the gate.
        IReadOnlyCollection<ViewListing> registrations;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            registrations = await CollectViewsAsync(cancellationToken).ConfigureAwait(false);
        }

        var candidates = registrations
            // Hide system views (those named with the system-data prefix, e.g. the
            // backup catalog index/history) from the listing unless the caller
            // explicitly opts in, mirroring how ListTreesAsync hides system trees.
            .Where(r => request.IncludeSystemTrees || !IsSystemDataTree(r.ViewName))
            .Where(r => request.PageToken is null || string.CompareOrdinal(r.ViewName, request.PageToken) > 0)
            .OrderBy(r => r.ViewName, StringComparer.Ordinal)
            .ToArray();

        // Scope views to those whose source tree the subject may read, so a view
        // does not leak the existence of data over an unreadable source. An
        // anonymous subject sees no views.
        ViewListing[] ordered;
        if (subject is { } resolved)
        {
            var visible = new List<ViewListing>(candidates.Length);
            foreach (var candidate in candidates)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await _visibility.CanReadTreeAsync(candidate.SourceTreeId, resolved, cancellationToken)
                        .ConfigureAwait(false))
                {
                    visible.Add(candidate);
                }
            }

            ordered = visible.ToArray();
        }
        else
        {
            ordered = candidates;
        }

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
                IsHistory = registration.IsHistory,
            });
        }

        return new ViewCatalogPage { Entries = entries, NextPageToken = nextToken };
    }

    public Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var cluster = _services.GetService<IOptions<ClusterOptions>>()?.Value;
        return Task.FromResult(new ClusterInfo
        {
            ClusterId = cluster?.ClusterId ?? string.Empty,
            ServiceId = cluster?.ServiceId ?? string.Empty,
        });
    }

    public async Task<TagIndexCatalogPage> ListTagIndexesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();

        // Resolve the caller subject once; null means visibility is disabled and
        // the tag-index catalog is returned unfiltered exactly as before.
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);

        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

        // Enumerating the registry is infrastructure: run it under a system-origin
        // scope so the enumeration is not itself filtered or denied by the gate.
        IEnumerable<string> allIds;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            allIds = await registry.GetAllTreeIdsAsync().ConfigureAwait(false);
        }

        var ordered = allIds
            .Where(IsTagIndexTree)
            .Where(id => request.PageToken is null || string.CompareOrdinal(id, request.PageToken) > 0)
            .OrderBy(id => id, StringComparer.Ordinal);

        // The source-tree filter needs a factory; auth-backed visibility also needs
        // one to resolve each index's covered trees. Resolve it when either is in
        // play. When visibility is on but no factory is registered we cannot prove
        // readability, so the catalog is fail-closed to empty below.
        var needsFactory = request.SourceTreeId is not null || subject is not null;
        var tagFactory = needsFactory ? _services.GetService<ILatticeTagIndexFactory>() : null;
        var pageSize = request.EffectivePageSize;
        var entries = new List<TagIndexStateSummary>(pageSize);
        string? nextToken = null;

        foreach (var id in ordered)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entries.Count == pageSize)
            {
                nextToken = entries[^1].TreeId;
                break;
            }

            var indexName = id[LatticeConstants.TagIndexTreePrefix.Length..];

            IReadOnlyList<string>? covered = null;
            if (tagFactory is not null && (request.SourceTreeId is not null || subject is not null))
            {
                using (LatticeAccessGateContext.EnterSystemOrigin())
                {
                    covered = await tagFactory.CreateMultiTree(indexName)
                        .CoveredTreesAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            if (request.SourceTreeId is not null)
            {
                if (covered is null || !covered.Contains(request.SourceTreeId, StringComparer.Ordinal))
                {
                    continue;
                }
            }

            // Auth-backed visibility: expose a tag index only when the subject can
            // read at least one of its covered trees, so the catalog does not leak
            // the existence of an index over data the caller cannot see. An
            // anonymous subject, or the absence of a factory to resolve coverage,
            // hides every index (fail-closed).
            if (subject is { } resolved)
            {
                if (covered is null || !await AnyCoveredTreeReadableAsync(covered, resolved, cancellationToken)
                        .ConfigureAwait(false))
                {
                    continue;
                }
            }

            TreeRegistryEntry? entry;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                entry = await registry.GetEntryAsync(id).ConfigureAwait(false);
            }

            entries.Add(new TagIndexStateSummary
            {
                IndexName = indexName,
                TreeId = id,
                ShardCount = entry?.ShardCount ?? 0,
            });
        }

        return new TagIndexCatalogPage { Entries = entries, NextPageToken = nextToken };
    }

    private async ValueTask<bool> AnyCoveredTreeReadableAsync(
        IReadOnlyList<string> coveredTrees,
        LatticeSubject subject,
        CancellationToken cancellationToken)
    {
        foreach (var treeId in coveredTrees)
        {
            if (await _visibility.CanReadTreeAsync(treeId, subject, cancellationToken).ConfigureAwait(false))
            {
                return true;
            }
        }

        return false;
    }

    public async Task<TagValueCatalogPage> ListTagValuesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.SourceTreeId);
        ArgumentException.ThrowIfNullOrEmpty(request.IndexName);
        cancellationToken.ThrowIfCancellationRequested();

        // Auth-backed visibility: tag values of a tree the caller may not read are
        // not disclosed (they leak which values exist in unreadable data).
        if (await IsTreeReadHiddenAsync(request.SourceTreeId, cancellationToken).ConfigureAwait(false))
        {
            return new TagValueCatalogPage();
        }

        var tree = _grainFactory.GetGrain<ILattice>(request.SourceTreeId);
        if (IsReservedTree(request.SourceTreeId)
            || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return new TagValueCatalogPage();
        }

        var tagFactory = _services.GetService<ILatticeTagIndexFactory>();
        if (tagFactory is null)
        {
            return new TagValueCatalogPage();
        }

        var index = tagFactory.Create(tree, request.IndexName);
        var pageSize = request.EffectivePageSize;
        var values = new List<string>(pageSize);
        string? nextToken = null;

        // TagsAsync yields the distinct tags in ascending ordinal order; the page
        // token is the last tag returned, applied as an exclusive lower bound.
        await foreach (var tag in index.TagsAsync(cancellationToken).ConfigureAwait(false))
        {
            if (request.PageToken is not null && string.CompareOrdinal(tag, request.PageToken) <= 0)
            {
                continue;
            }

            if (values.Count == pageSize)
            {
                nextToken = values[^1];
                break;
            }

            values.Add(tag);
        }

        return new TagValueCatalogPage { Entries = values, NextPageToken = nextToken };
    }

    /// <inheritdoc />
    public async Task<CoveredTreeCatalogPage> ListCoveredTreesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.IndexName);
        cancellationToken.ThrowIfCancellationRequested();

        var tagFactory = _services.GetService<ILatticeTagIndexFactory>();
        if (tagFactory is null)
        {
            return new CoveredTreeCatalogPage();
        }

        // Resolve the caller subject once; null means visibility is disabled and
        // the covered-tree list is returned unfiltered exactly as before.
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);

        // Enumerating the covered set is infrastructure: run it under a
        // system-origin scope so it is not itself filtered by the gate.
        IReadOnlyCollection<string> covered;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            covered = await tagFactory.CreateMultiTree(request.IndexName)
                .CoveredTreesAsync(cancellationToken).ConfigureAwait(false);
        }

        var pageSize = request.EffectivePageSize;
        var entries = new List<string>(Math.Min(pageSize, covered.Count));
        string? nextToken = null;

        // CoveredTreesAsync already yields ordinal-sorted ids; the explicit sort
        // keeps the page contract independent of that implementation detail.
        foreach (var treeId in covered.OrderBy(id => id, StringComparer.Ordinal))
        {
            if (request.PageToken is not null && string.CompareOrdinal(treeId, request.PageToken) <= 0)
            {
                continue;
            }

            // Omit any covered tree the subject may not read (an anonymous subject
            // sees none), so the covered-tree list does not leak unreadable trees.
            if (subject is { } resolved
                && !await _visibility.CanReadTreeAsync(treeId, resolved, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            if (entries.Count == pageSize)
            {
                nextToken = entries[^1];
                break;
            }

            entries.Add(treeId);
        }

        return new CoveredTreeCatalogPage { Entries = entries, NextPageToken = nextToken };
    }

    /// <inheritdoc />
    public async Task<TagValueCatalogPage> ListIndexTagsAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.IndexName);
        cancellationToken.ThrowIfCancellationRequested();

        var tagFactory = _services.GetService<ILatticeTagIndexFactory>();
        if (tagFactory is null)
        {
            return new TagValueCatalogPage();
        }

        var index = tagFactory.CreateMultiTree(request.IndexName);

        // Auth-backed visibility: an index-wide tag list aggregates tags across
        // every covered tree, so a single tag can reveal that a value exists in a
        // covered tree the subject cannot read. This surface cannot attribute a
        // tag to a specific tree, so it is conservatively all-or-nothing: the tag
        // list is disclosed only when the subject can read every covered tree;
        // otherwise it is empty. (Per-tree tag values remain available through
        // ListTagValuesAsync, which is scoped to a single readable source tree.)
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (subject is { } resolved)
        {
            IReadOnlyCollection<string> covered;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                covered = await index.CoveredTreesAsync(cancellationToken).ConfigureAwait(false);
            }

            foreach (var treeId in covered)
            {
                if (!await _visibility.CanReadTreeAsync(treeId, resolved, cancellationToken).ConfigureAwait(false))
                {
                    return new TagValueCatalogPage();
                }
            }
        }

        var pageSize = request.EffectivePageSize;
        var values = new List<string>(pageSize);
        string? nextToken = null;

        // TagsAsync yields the index-wide distinct tags in ascending ordinal
        // order; the page token is the last tag returned, an exclusive lower bound.
        await foreach (var tag in index.TagsAsync(cancellationToken).ConfigureAwait(false))
        {
            if (request.PageToken is not null && string.CompareOrdinal(tag, request.PageToken) <= 0)
            {
                continue;
            }

            if (values.Count == pageSize)
            {
                nextToken = values[^1];
                break;
            }

            values.Add(tag);
        }

        return new TagValueCatalogPage { Entries = values, NextPageToken = nextToken };
    }

    /// <inheritdoc />
    public async Task<TagMemberScanPage> ScanTagMembersAsync(
        TagMemberScanRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.IndexName);
        ArgumentException.ThrowIfNullOrEmpty(request.Tag);
        cancellationToken.ThrowIfCancellationRequested();

        var tagFactory = _services.GetService<ILatticeTagIndexFactory>();
        if (tagFactory is null)
        {
            return new TagMemberScanPage();
        }

        // Auth-backed visibility: an unresolved/anonymous caller is denied outright
        // (fail-closed) regardless of the policy default effect. For a resolved
        // subject the per-member presence probe (ExistsAsync) below runs through
        // the gated data-plane surface, so members in trees the subject may not
        // read are dropped automatically; no extra per-tree check is needed here.
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);
        if (subject is { } resolved && LatticeStateVisibilityFilter.DeniesAllReads(resolved))
        {
            return new TagMemberScanPage();
        }

        var index = tagFactory.CreateMultiTree(request.IndexName);
        var pageSize = request.EffectivePageSize;
        DecodeTagMemberToken(request.PageToken, out var afterTree, out var afterKey);

        var members = new List<TagMember>(pageSize);
        string? nextToken = null;

        // The multi-tree query yields (treeId, key) pairs ordered by covered tree
        // then by key, both ascending ordinal; the continuation token is the last
        // (treeId, key) returned, applied as an exclusive lower bound.
        await foreach (var tagged in index.WithAllTags(request.Tag)
            .WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (afterTree is not null
                && CompareTaggedKey(tagged.TreeId, tagged.Key, afterTree, afterKey) <= 0)
            {
                continue;
            }

            if (members.Count == pageSize)
            {
                nextToken = EncodeTagMemberToken(members[^1].TreeId, members[^1].Key);
                break;
            }

            // Live-only: a membership row can outlive its primary key until the
            // next reconcile. ExistsAsync is a cheap presence probe (no value
            // fetch) that keeps the browse showing real rows.
            var tree = _grainFactory.GetGrain<ILattice>(tagged.TreeId);
            if (!await tree.ExistsAsync(tagged.Key, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            members.Add(new TagMember { TreeId = tagged.TreeId, Key = tagged.Key });
        }

        return new TagMemberScanPage { Entries = members, NextPageToken = nextToken };
    }

    // NUL joins the (treeId, key) continuation token: a tree id never contains
    // NUL (write-time validation forbids it), so the first NUL is the delimiter
    // and the remainder is the key (which may itself contain NUL).
    private const char TagMemberTokenSeparator = '\0';

    private static string EncodeTagMemberToken(string treeId, string key) =>
        string.Concat(treeId, "\0", key);

    private static void DecodeTagMemberToken(string? token, out string? treeId, out string key)
    {
        if (string.IsNullOrEmpty(token))
        {
            treeId = null;
            key = string.Empty;
            return;
        }

        var sep = token.IndexOf(TagMemberTokenSeparator);
        if (sep < 0)
        {
            treeId = token;
            key = string.Empty;
            return;
        }

        treeId = token[..sep];
        key = token[(sep + 1)..];
    }

    private static int CompareTaggedKey(string leftTree, string leftKey, string rightTree, string rightKey)
    {
        var byTree = string.CompareOrdinal(leftTree, rightTree);
        return byTree != 0 ? byTree : string.CompareOrdinal(leftKey, rightKey);
    }

    public async Task<TreeStructureResult> GetTreeStructureAsync(
        StructureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Auth-backed visibility: the structure of a tree the caller may not read
        // is itself not disclosed (node shape leaks the existence of data). An
        // unresolved/anonymous caller sees no structure.
        if (await IsTreeReadHiddenAsync(request.TreeId, cancellationToken).ConfigureAwait(false))
        {
            return TreeStructureResult.NotFound(request.TreeId);
        }

        // A materialised view is a read-only tree; permit it on the read path by
        // binding under an authorised view-read scope, while system trees stay
        // hidden everywhere.
        using var viewScope = OpenViewReadScopeIfNeeded(request.TreeId);

        // A view tree binds to its active generation (a shadow-swap rebuild moves
        // the live data off the generation-0 alias), so resolve the read id here.
        var bindTreeId = await ResolveReadTreeIdAsync(request.TreeId, cancellationToken).ConfigureAwait(false);
        if (IsSystemTree(request.TreeId) || bindTreeId is null)
        {
            return TreeStructureResult.NotFound(request.TreeId);
        }
        var tree = _grainFactory.GetGrain<ILattice>(bindTreeId);
        if (!await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return TreeStructureResult.NotFound(request.TreeId);
        }
        var registry = _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(bindTreeId).ConfigureAwait(false);
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

        var shardCount = await ResolveShardCountAsync(registry, bindTreeId).ConfigureAwait(false);
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

        // Auth-backed visibility: hide the whole tree from a caller with no read
        // access (an unresolved/anonymous caller sees nothing), reported as
        // not-found. A partial (prefix) grant proceeds; the gated cursor surface
        // prunes the individual keys the subject may not observe.
        if (await IsTreeReadHiddenAsync(request.TreeId, cancellationToken).ConfigureAwait(false))
        {
            return EntryScanResult.NotFound(request.TreeId);
        }

        if (!string.IsNullOrEmpty(request.IndexName) && request.Tag is not null)
        {
            return await ScanByTagAsync(request, cancellationToken).ConfigureAwait(false);
        }

        // A materialised view is a read-only tree; permit it on the read path by
        // binding under an authorised view-read scope, while system trees stay
        // hidden everywhere. The scope must remain active for the cursor open and
        // every page read, so it wraps the whole method.
        using var viewScope = OpenViewReadScopeIfNeeded(request.TreeId);

        // A view tree binds to its active generation (a shadow-swap rebuild moves
        // the live data off the generation-0 alias), so resolve the read id here.
        var bindTreeId = await ResolveReadTreeIdAsync(request.TreeId, cancellationToken).ConfigureAwait(false);
        if (bindTreeId is null)
        {
            return EntryScanResult.NotFound(request.TreeId);
        }

        var tree = _grainFactory.GetGrain<ILattice>(bindTreeId);

        var fresh = string.IsNullOrEmpty(request.ContinuationToken);
        string cursorId;
        if (fresh)
        {
            if (IsSystemTree(request.TreeId) || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
            {
                return EntryScanResult.NotFound(request.TreeId);
            }

            // An aggregation (grouped-reduce or custom-fold) view keeps its
            // internal accumulator / inverse / membership / fold-inverse rows
            // under the reserved NUL prefix (see AggregationRowCodec). The
            // canonical view read surface floors every unbounded scan above that
            // range so readers see only the materialised group values; mirror
            // that here so the state API never leaks the internal rows.
            var startInclusive = ClampViewScanStart(request.TreeId, request.StartInclusive);

            // Open the cursor selected by the request mode. Snapshot opens a
            // point-in-time cursor that captures an all-shard frozen baseline so
            // every page reads a torn-free instant; the live modes open a
            // baseline-free cursor whose paging is keyed on the last yielded
            // key, so later pages can observe writes committed after the open.
            cursorId = request.Mode == EntryScanMode.Snapshot
                ? request.Predicate is { } snapshotPredicate
                    ? await tree.OpenSnapshotEntryCursorWherePredicateAsync(
                        snapshotPredicate, startInclusive, request.EndExclusive, request.Reverse, cancellationToken)
                        .ConfigureAwait(false)
                    : await tree.OpenSnapshotEntryCursorAsync(
                        startInclusive, request.EndExclusive, request.Reverse, cancellationToken)
                        .ConfigureAwait(false)
                : request.Predicate is { } livePredicate
                    ? await tree.OpenEntryCursorWherePredicateAsync(
                        livePredicate, startInclusive, request.EndExclusive, request.Reverse,
                        pointInTime: request.Mode == EntryScanMode.LivePointInTime, cancellationToken)
                        .ConfigureAwait(false)
                    : await tree.OpenEntryCursorAsync(
                        startInclusive, request.EndExclusive, request.Reverse,
                        pointInTime: request.Mode == EntryScanMode.LivePointInTime, cancellationToken)
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

        // The merge mode is declared per tree, so the CRDT shape tag is the
        // same for every entry on the page; resolve it once and stamp each
        // record with it (null for an opaque last-writer-wins tree).
        var crdtShape = ResolveCrdtShape(request.TreeId);

        // A view read bypasses the data-plane gate (it runs under a view-read
        // scope), so a prefix-granted caller's per-key pruning on the source is
        // applied here rather than by the gated cursor. Null for an ordinary tree
        // or a whole-tree grant (every key admitted).
        var viewKeyFilter = await ResolveViewKeyFilterAsync(request.TreeId, cancellationToken).ConfigureAwait(false);

        var records = new List<EntryRecord>(page.Entries.Count);
        foreach (var entry in page.Entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (viewKeyFilter is not null && !viewKeyFilter(entry.Key))
            {
                // Outside the caller's prefix grant on the view's source tree.
                continue;
            }

            var record = await BuildEntryRecordAsync(tree, entry.Key, entry.Value, previewBudget, crdtShape, cancellationToken)
                .ConfigureAwait(false);
            // Decode current members off the full snapshot value bytes (before the
            // preview clip) so the Data list shows a CRDT entry's materialised
            // members, matching the single-entry detail view.
            records.Add(WithCurrentMembers(record, request.TreeId, entry.Value));
        }

        string? continuation = page.HasMore ? cursorId : null;
        if (!page.HasMore)
        {
            // Drained: release the server-side cursor (and, for a snapshot or
            // point-in-time cursor, its WAL-retention pin) promptly.
            await tree.CloseCursorAsync(cursorId, cancellationToken).ConfigureAwait(false);
        }

        return EntryScanResult.Found(request.TreeId, records, continuation);
    }

    private async Task<EntryScanResult> ScanByTagAsync(
        EntryScanRequest request,
        CancellationToken cancellationToken)
    {
        var tree = _grainFactory.GetGrain<ILattice>(request.TreeId);
        if (IsReservedTree(request.TreeId) || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return EntryScanResult.NotFound(request.TreeId);
        }

        var tagFactory = _services.GetService<ILatticeTagIndexFactory>();
        if (tagFactory is null)
        {
            return EntryScanResult.Found(request.TreeId, Array.Empty<EntryRecord>(), continuationToken: null);
        }

        // A tag index materialises as an internal membership tree named
        // "tag-{indexName}". If that tree was never created, the index name is a
        // typo (or the index has never been populated) - report IndexNotFound so
        // the caller can tell a mistyped index from a real-but-empty one, which
        // both otherwise return an empty Found page. Only check on a fresh open:
        // a valid continuation token already implies the index existed at open,
        // so paging does not pay a per-page existence round-trip.
        if (string.IsNullOrEmpty(request.ContinuationToken))
        {
            var indexTreeId = LatticeConstants.TagIndexTreePrefix + request.IndexName!;
            var indexTree = _grainFactory.GetGrain<ILattice>(indexTreeId);
            bool indexExists;
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                indexExists = await indexTree.TreeExistsAsync(cancellationToken).ConfigureAwait(false);
            }

            if (!indexExists)
            {
                return EntryScanResult.IndexNotFound(request.TreeId);
            }
        }

        var index = tagFactory.Create(tree, request.IndexName!);
        var pageSize = ClampPageSize(request.PageSize);
        var previewBudget = ClampScanPreviewBudget(request.ValuePreviewBudget);
        var crdtShape = ResolveCrdtShape(request.TreeId);

        // Tag-filtered paging is keyed on the source key (the membership query
        // yields keys in ascending ordinal order); the continuation token is the
        // last source key returned, applied as an exclusive lower bound.
        var after = string.IsNullOrEmpty(request.ContinuationToken) ? null : request.ContinuationToken;

        var records = new List<EntryRecord>(pageSize);
        string? nextToken = null;

        await foreach (var key in index.WithAllTags(request.Tag!)
            .WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (after is not null && string.CompareOrdinal(key, after) <= 0)
            {
                continue;
            }

            if (records.Count == pageSize)
            {
                nextToken = records[^1].Key;
                break;
            }

            var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
            if (versioned.Value is null)
            {
                // The membership row outlived its primary key (pending reconcile);
                // skip it so the filtered view only shows live rows.
                continue;
            }

            records.Add(WithCurrentMembers(
                BuildEntryRecord(key, versioned.Value, versioned.Version, versioned.ExpiresAtTicks, previewBudget, crdtShape),
                request.TreeId,
                versioned.Value));
        }

        return EntryScanResult.Found(request.TreeId, records, nextToken);
    }

    public async Task<EntryDetailResult> GetEntryAsync(
        string treeId,
        string key,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(key);
        cancellationToken.ThrowIfCancellationRequested();

        // Auth-backed visibility: a caller with no read access to the tree (an
        // unresolved/anonymous caller included) gets not-found, never a value.
        // A partial (prefix) grant proceeds; the gated point read below returns
        // not-found for any individual key the subject may not observe.
        if (await IsTreeReadHiddenAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            return EntryDetailResult.TreeNotFound(treeId, key);
        }

        // A view point read bypasses the data-plane gate; apply the caller's
        // per-key grant on the view's source so a prefix-granted caller cannot read
        // a view key outside its grant. Null for an ordinary tree or whole-tree grant.
        var viewKeyFilter = await ResolveViewKeyFilterAsync(treeId, cancellationToken).ConfigureAwait(false);
        if (viewKeyFilter is not null && !viewKeyFilter(key))
        {
            return EntryDetailResult.KeyNotFound(treeId, key);
        }

        // A materialised view is a read-only tree; permit it on the read path by
        // binding under an authorised view-read scope, while system trees stay
        // hidden everywhere.
        using var viewScope = OpenViewReadScopeIfNeeded(treeId);

        // A view tree binds to its active generation (a shadow-swap rebuild moves
        // the live data off the generation-0 alias), so resolve the read id here.
        var bindTreeId = await ResolveReadTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        var tree = bindTreeId is null ? null : _grainFactory.GetGrain<ILattice>(bindTreeId);
        if (IsSystemTree(treeId) || tree is null || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
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
            _apiOptions.SingleEntryValuePreviewBytes,
            ResolveCrdtShape(treeId));

        // For a typed CRDT entry, decode the full folded state into its current
        // member set so the consumer (the Explorer Data tab) can render the
        // materialised value instead of an opaque blob. Decoded off the full
        // value bytes the read returned, before the preview clip; an LWW value,
        // a minimal deployment without the CRDT shape registry, or a decode
        // failure all degrade to no members rather than failing the read.
        record = WithCurrentMembers(record, treeId, versioned.Value);

        return EntryDetailResult.Found(treeId, record);
    }

    public async Task<EntryHistoryResult> GetEntryHistoryAsync(
        EntryHistoryRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);
        ArgumentNullException.ThrowIfNull(request.Key);
        cancellationToken.ThrowIfCancellationRequested();

        // Auth-backed visibility: hide the tree from a caller with no read access
        // (unresolved/anonymous included). A partial grant proceeds; the gated
        // history read below is already filtered per key.
        if (await IsTreeReadHiddenAsync(request.TreeId, cancellationToken).ConfigureAwait(false))
        {
            return EntryHistoryResult.TreeNotFound(request.TreeId, request.Key);
        }

        // A view history read bypasses the data-plane gate; apply the caller's
        // per-key grant on the view's source so a prefix-granted caller cannot read
        // the history of a view key outside its grant.
        var viewKeyFilter = await ResolveViewKeyFilterAsync(request.TreeId, cancellationToken).ConfigureAwait(false);
        if (viewKeyFilter is not null && !viewKeyFilter(request.Key))
        {
            return EntryHistoryResult.KeyNotFound(request.TreeId, request.Key);
        }

        // A materialised view is a read-only tree; permit it on the read path by
        // binding under an authorised view-read scope, while system trees stay
        // hidden everywhere.
        using var viewScope = OpenViewReadScopeIfNeeded(request.TreeId);

        // A view tree binds to its active generation (a shadow-swap rebuild moves
        // the live data off the generation-0 alias), so resolve the read id here.
        var bindTreeId = await ResolveReadTreeIdAsync(request.TreeId, cancellationToken).ConfigureAwait(false);
        var tree = bindTreeId is null ? null : _grainFactory.GetGrain<ILattice>(bindTreeId);
        if (IsSystemTree(request.TreeId) || tree is null || !await tree.TreeExistsAsync(cancellationToken).ConfigureAwait(false))
        {
            return EntryHistoryResult.TreeNotFound(request.TreeId, request.Key);
        }

        var limit = ClampHistoryLimit(request.Limit);
        var previewBudget = ClampHistoryPreviewBudget(request.ValuePreviewBudget);

        var page = await tree
            .ScanEntryHistoryAsync(request.Key, request.FromHlc, request.ToHlc, limit, request.ContinuationToken, cancellationToken)
            .ConfigureAwait(false);

        // The merge mode is declared per tree, so the CRDT shape (and therefore
        // the decoder) is the same for every revision; resolve the registries
        // once and decode per revision off the bytes the read path retained.
        var shapeRegistry = _services.GetService<CrdtShapeRegistry>();
        var decoderRegistry = _services.GetService<CrdtProvenanceDecoderRegistry>()
            ?? CrdtProvenanceDecoderRegistry.Default;

        var records = new List<EntryRevisionRecord>(page.Revisions.Count);
        for (var i = 0; i < page.Revisions.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            records.Add(MapRevision(request.TreeId, page.Revisions[i], previewBudget, shapeRegistry, decoderRegistry));
        }

        if (request.Reverse)
        {
            // Paging advances oldest-to-newest through the timeline; Reverse only
            // flips the order of the revisions within this page (newest-first).
            records.Reverse();
        }

        var bound = MapHistoryBound(page);
        var earliest = bound == EntryHistoryBound.Truncated ? page.EarliestAvailable : HybridLogicalClock.Zero;

        // Distinguish "key never existed" from "exists with an empty/aged-out
        // timeline". On a fresh read (no continuation token) that yields zero
        // revisions and whose timeline is durable-bounded (not truncated), the
        // key has no history at all - report KeyNotFound. When the timeline is
        // Truncated the key may exist but its revisions aged out of the durable
        // window, so it stays Found with an empty page and the truncation bound.
        var freshRead = string.IsNullOrEmpty(request.ContinuationToken);
        if (freshRead && records.Count == 0 && bound != EntryHistoryBound.Truncated)
        {
            return EntryHistoryResult.KeyNotFound(request.TreeId, request.Key);
        }

        return EntryHistoryResult.Found(request.TreeId, request.Key, records, page.Continuation, bound, earliest);
    }

    /// <summary>
    /// Maps a core <see cref="EntryRevision"/> onto the public
    /// <see cref="EntryRevisionRecord"/>: re-clips the value / delta preview to
    /// the (smaller) request budget, derives the per-row retention descriptor,
    /// and decodes the CRDT member changes when the revision retained its bytes
    /// in full.
    /// </summary>
    private static EntryRevisionRecord MapRevision(
        string treeId,
        in EntryRevision revision,
        int previewBudget,
        CrdtShapeRegistry? shapeRegistry,
        CrdtProvenanceDecoderRegistry decoderRegistry)
    {
        var truncated = revision.ValueTruncated;

        var valuePreview = revision.ValuePreview;
        if (valuePreview is not null && valuePreview.Length > previewBudget)
        {
            valuePreview = valuePreview[..previewBudget];
            truncated = true;
        }

        var deltaPreview = revision.Delta;
        if (deltaPreview is not null && deltaPreview.Length > previewBudget)
        {
            deltaPreview = deltaPreview[..previewBudget];
            truncated = true;
        }

        var memberChanges = DecodeMemberChanges(treeId, revision, shapeRegistry, decoderRegistry);

        return new EntryRevisionRecord
        {
            Hlc = revision.Hlc,
            Kind = revision.Kind,
            Category = MutationCategory.User,
            SourceKey = revision.SourceKey,
            OriginClusterId = revision.OriginClusterId,
            ValuePreview = valuePreview,
            ValueLength = revision.ValueLength,
            Truncated = truncated,
            ValueHash = revision.ValueHash,
            Delta = deltaPreview,
            Mode = revision.Mode,
            MemberChanges = memberChanges,
            Retention = new RevisionRetention
            {
                Mode = revision.RetentionShape,
                ValueRetained = revision.ValuePreview is not null || revision.Delta is not null,
            },
            EndKey = revision.EndKey,
        };
    }

    /// <summary>
    /// Decodes the element-level member changes for a CRDT revision whose bytes
    /// were retained in full (an untruncated delta or full-state value), or
    /// returns an empty list for an LWW revision, a non-CRDT tree, a
    /// metadata-only or truncated CRDT revision, or an unregistered shape. A
    /// decode failure (e.g. a forward-incompatible delta) is swallowed and
    /// yields no member changes rather than failing the whole history read.
    /// </summary>
    private static IReadOnlyList<CrdtMemberChange> DecodeMemberChanges(
        string treeId,
        in EntryRevision revision,
        CrdtShapeRegistry? shapeRegistry,
        CrdtProvenanceDecoderRegistry decoderRegistry)
    {
        if (revision.Mode == LatticeMergeMode.LwwRegister
            || shapeRegistry is null
            || revision.ValueTruncated
            || !decoderRegistry.TryGet(revision.Mode, out var decoder))
        {
            return Array.Empty<CrdtMemberChange>();
        }

        var shape = shapeRegistry.TryGet(treeId, revision.Mode);
        if (shape is null)
        {
            return Array.Empty<CrdtMemberChange>();
        }

        try
        {
            if (revision.Kind == HistoryRowKind.CrdtDelta && revision.Delta is { } deltaBytes)
            {
                var delta = shape.DeserializeDelta(deltaBytes);
                return decoder.DecodeDeltas(new[] { new CrdtProvenanceDelta(delta, revision.Hlc) });
            }

            if (revision.Kind == HistoryRowKind.Set && revision.ValuePreview is { } valueBytes)
            {
                var state = shape.DeserializeState(valueBytes);
                return decoder.DecodeState(state);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A revision whose retained bytes cannot be decoded (partial,
            // corrupt, or forward-incompatible) carries no member changes rather
            // than failing the page.
            return Array.Empty<CrdtMemberChange>();
        }

        return Array.Empty<CrdtMemberChange>();
    }

    /// <summary>
    /// Decodes the element-level members of a typed CRDT entry's <em>current</em>
    /// folded state into its live, present members only - the materialised value
    /// of the CRDT - via <see cref="ICrdtProvenanceDecoder.DecodeCurrentValue"/>.
    /// Removed elements are excluded (a fully-removed OR-Set element does not
    /// appear), unlike a provenance decode which surfaces lingering causal dots.
    /// Returns an empty list for an opaque last-writer-wins value
    /// (<paramref name="crdtShape"/> is <see langword="null"/>), a minimal
    /// deployment that did not register the <see cref="CrdtShapeRegistry"/> or
    /// the tree's shape, an unregistered shape, or a decode failure (corrupt or
    /// forward-incompatible bytes are swallowed rather than failing the read).
    /// </summary>
    private IReadOnlyList<CrdtMemberValue> DecodeCurrentStateMembers(
        string treeId,
        byte[] value,
        string? crdtShape)
    {
        if (crdtShape is null)
        {
            return Array.Empty<CrdtMemberValue>();
        }

        var shapeRegistry = _services.GetService<CrdtShapeRegistry>();
        if (shapeRegistry is null)
        {
            return Array.Empty<CrdtMemberValue>();
        }

        var decoderRegistry = _services.GetService<CrdtProvenanceDecoderRegistry>()
            ?? CrdtProvenanceDecoderRegistry.Default;
        if (!decoderRegistry.TryGet(crdtShape, out var decoder))
        {
            return Array.Empty<CrdtMemberValue>();
        }

        var shape = shapeRegistry.TryGet(treeId, decoder.Mode);
        if (shape is null)
        {
            return Array.Empty<CrdtMemberValue>();
        }

        try
        {
            var state = shape.DeserializeState(value);
            return decoder.DecodeCurrentValue(state);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A value whose bytes cannot be decoded (corrupt or
            // forward-incompatible) carries no members rather than failing the
            // detail read.
            return Array.Empty<CrdtMemberValue>();
        }
    }

    /// <summary>
    /// Returns <paramref name="record"/> with its current-state members decoded
    /// from <paramref name="fullValue"/> (the full value bytes, before any preview
    /// clip), when the record is a typed CRDT and its current state has live
    /// members; otherwise returns it unchanged. Used by every list and detail path
    /// so a CRDT entry renders its materialised value rather than an opaque blob.
    /// </summary>
    private EntryRecord WithCurrentMembers(EntryRecord record, string treeId, byte[] fullValue)
    {
        if (record.CrdtShape is null)
        {
            return record;
        }

        var members = DecodeCurrentStateMembers(treeId, fullValue, record.CrdtShape);
        return members.Count > 0 ? record with { CurrentMembers = members } : record;
    }

    /// <summary>Maps the backing history substrate and truncation flag onto the public bound classification.</summary>
    private static EntryHistoryBound MapHistoryBound(EntryHistoryPage page) => page.Source switch
    {
        EntryHistorySource.View => EntryHistoryBound.BoundedByAge,
        EntryHistorySource.WalWindow when page.Truncated => EntryHistoryBound.Truncated,
        _ => EntryHistoryBound.WalWindowFallback,
    };

    private int ClampHistoryLimit(int requested) => requested switch
    {
        < 1 => _apiOptions.DefaultHistoryPageSize,
        _ when requested > _apiOptions.MaxHistoryPageSize => _apiOptions.MaxHistoryPageSize,
        _ => requested,
    };

    private int ClampHistoryPreviewBudget(int requested) => requested switch
    {
        < 1 => _apiOptions.DefaultHistoryValuePreviewBytes,
        _ when requested > _apiOptions.MaxHistoryValuePreviewBytes => _apiOptions.MaxHistoryValuePreviewBytes,
        _ => requested,
    };

    public async Task CancelScanAsync(
        string treeId,
        string? continuationToken,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        // An empty token names no cursor, and reserved system trees never expose
        // a scan cursor: both are a no-op rather than a grain call.
        if (string.IsNullOrEmpty(continuationToken) || IsReservedTree(treeId))
        {
            return;
        }

        var tree = _grainFactory.GetGrain<ILattice>(treeId);
        try
        {
            await tree.CloseCursorAsync(continuationToken, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // The token names an unknown, already-drained, or already-closed
            // cursor (including an expired snapshot pin): a best-effort cancel
            // tolerates every one of these as a successful no-op.
        }
    }

    /// <inheritdoc />
    public async Task<int> GetDeadLetterCountAsync(
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        // Auth-backed visibility: a tree the caller may not read discloses
        // nothing, not even that it has a dead-letter queue.
        if (await IsTreeReadHiddenAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            return 0;
        }

        // The dead-letter store is an OPTIONAL capability, present only when the
        // host registered schema enforcement. A cluster without it has no
        // dead-letter queue, so the count is zero and no hard runtime dependency
        // on the enforcement layer is taken (the state API depends solely on the
        // public ILatticeSchemaDeadLetterStore read interface).
        var store = _services.GetService<ILatticeSchemaDeadLetterStore>();
        if (store is null)
        {
            return 0;
        }

        return await store.CountAsync(treeId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<DeadLetterQueuePage> ListDeadLettersAsync(
        DeadLetterQueueRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrEmpty(request.TreeId);
        cancellationToken.ThrowIfCancellationRequested();

        if (await IsTreeReadHiddenAsync(request.TreeId, cancellationToken).ConfigureAwait(false))
        {
            return new DeadLetterQueuePage();
        }

        var store = _services.GetService<ILatticeSchemaDeadLetterStore>();
        if (store is null)
        {
            return new DeadLetterQueuePage();
        }

        // The store enumerates the queue as a single time-ordered prefix scan and
        // exposes no per-entry cursor, so the page cursor is the append offset of
        // the next unread entry. Append-only semantics make an offset a stable
        // resume point for this read-only surface (this feature never removes
        // entries). Only the requested window is projected, so the whole queue is
        // never materialised in memory - the scan stops one entry past the page.
        var offset = DecodeOffset(request.PageToken);
        var pageSize = request.EffectivePageSize;
        var entries = new List<DeadLetterEntryRecord>(pageSize);
        var index = 0;
        string? nextToken = null;

        await foreach (var entry in store.ListAsync(request.TreeId, cancellationToken).ConfigureAwait(false))
        {
            if (index < offset)
            {
                index++;
                continue;
            }

            if (entries.Count == pageSize)
            {
                // A further entry exists past this page: hand back the offset that
                // resumes exactly after the last projected entry.
                nextToken = EncodeOffset(offset + pageSize);
                break;
            }

            entries.Add(MapDeadLetter(entry));
            index++;
        }

        return new DeadLetterQueuePage { Entries = entries, NextPageToken = nextToken };
    }

    /// <summary>
    /// Projects a schema-package dead-letter entry onto the state-API read model.
    /// The bounded preview bytes are shared by reference (the record is immutable),
    /// so the projection allocates only the wrapper record.
    /// </summary>
    private static DeadLetterEntryRecord MapDeadLetter(LatticeSchemaDeadLetterEntry entry) =>
        new()
        {
            Key = entry.Key,
            ValuePreview = entry.ValuePreview,
            ValueByteLength = entry.ValueByteLength,
            Reason = entry.Reason,
            Source = MapSource(entry.Source),
            TimestampUtc = entry.TimestampUtc,
            PreviewTruncated = entry.ValueByteLength > entry.ValuePreview.Length,
        };

    /// <summary>Maps the enforcement dead-letter source onto the API-owned kind.</summary>
    private static DeadLetterSourceKind MapSource(LatticeSchemaDeadLetterSource source) => source switch
    {
        LatticeSchemaDeadLetterSource.Replication => DeadLetterSourceKind.Replication,
        LatticeSchemaDeadLetterSource.Restore => DeadLetterSourceKind.Restore,
        LatticeSchemaDeadLetterSource.LocalRejected => DeadLetterSourceKind.LocalRejected,
        _ => DeadLetterSourceKind.Unknown,
    };

    /// <summary>
    /// Decodes a dead-letter page token to an append offset. A null, empty, or
    /// malformed token resolves to <c>0</c> (start from the oldest entry) so a
    /// hostile or stale cursor degrades to a fresh read rather than a fault.
    /// </summary>
    private static int DecodeOffset(string? token) =>
        !string.IsNullOrEmpty(token)
            && int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out var value)
            && value >= 0
            ? value
            : 0;

    /// <summary>Encodes an append offset as a dead-letter page token.</summary>
    private static string EncodeOffset(int offset) => offset.ToString(CultureInfo.InvariantCulture);

    private async Task<EntryRecord> BuildEntryRecordAsync(
        ILattice tree,
        string key,
        byte[] value,
        int previewBudget,
        string? crdtShape,
        CancellationToken cancellationToken)
    {
        // The cursor page carries the snapshot value bytes (used verbatim for
        // the preview and full length); the per-entry version / TTL is overlaid
        // from a metadata read so the record carries HLC and expiry.
        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        return BuildEntryRecord(key, value, versioned.Version, versioned.ExpiresAtTicks, previewBudget, crdtShape);
    }

    private static EntryRecord BuildEntryRecord(
        string key,
        byte[] value,
        Orleans.Lattice.HybridLogicalClock version,
        long expiresAtTicks,
        int previewBudget,
        string? crdtShape)
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
            CrdtShape = crdtShape,
        };
    }

    /// <summary>
    /// Resolves the CRDT shape tag for <paramref name="treeId"/>.
    /// <para>
    /// An ordinary tree's shape is its declared per-tree
    /// <see cref="LatticeMergeMode"/> (the only thing the system knows about a
    /// value's shape - the core stores every value as opaque bytes), so an
    /// undeclared (single-cluster) or last-writer-wins tree yields
    /// <see langword="null"/> and a typed CRDT tree yields the mode name (e.g.
    /// <c>"OrSet"</c>).
    /// </para>
    /// <para>
    /// A materialised-view tree (<c>view-*</c>) has no entry in the merge-mode map
    /// (views are not replicated trees), so its shape is derived from its view
    /// kind: a predicate / key-preserving view stores its source tree's value
    /// verbatim and therefore mirrors the source tree's merge mode, while an
    /// aggregation view (aggregation rows) and a history / accumulative view
    /// (<c>HistoryRow</c> blobs) are not member CRDTs and yield
    /// <see langword="null"/> (rendered as an opaque blob).
    /// </para>
    /// <para>
    /// A tag-index membership tree (<c>tag-*</c>) is resolved through the same
    /// merge-mode map as any other tree: a flag-membership index declared as
    /// <c>OrFlag</c> / <c>RwFlag</c> yields that shape and renders its current
    /// boolean state, while a default (last-writer-wins) index stores a one-byte
    /// presence sentinel and correctly yields <see langword="null"/>.
    /// </para>
    /// </summary>
    private string? ResolveCrdtShape(string treeId)
    {
        if (IsViewTree(treeId))
        {
            return ResolveViewCrdtShape(treeId);
        }

        var mode = _services.GetService<ILatticeMergeModeResolver>()?.Resolve(treeId);
        return mode is null or LatticeMergeMode.LwwRegister ? null : mode.Value.ToString();
    }

    /// <summary>
    /// Resolves the effective CRDT shape tag for a materialised-view tree by
    /// looking up its registration in the local <see cref="IViewCatalog"/>: a
    /// predicate / key-preserving view mirrors its source tree's merge mode; an
    /// aggregation or accumulative (history) view is not a member CRDT and yields
    /// <see langword="null"/>. Degrades to <see langword="null"/> (opaque blob)
    /// when the view infrastructure is not registered (a minimal deployment), the
    /// view name is unrecoverable, or the view is not in the local catalog.
    /// </summary>
    private string? ResolveViewCrdtShape(string treeId)
    {
        var catalog = _services.GetService<IViewCatalog>();
        if (catalog is null)
        {
            return null;
        }

        var viewName = ViewNameFromTreeId(treeId);
        if (viewName.Length == 0)
        {
            return null;
        }

        ViewRegistration? registration;
        try
        {
            registration = catalog.TryGet(viewName);
        }
        catch (Exception)
        {
            return null;
        }

        // Only a predicate / key-preserving view stores a member CRDT value (the
        // source value verbatim). Aggregation rows and history (accumulative)
        // rows are bespoke blobs with no member projection.
        if (registration is null || registration.IsAggregation || registration.Accumulative)
        {
            return null;
        }

        var mode = _services.GetService<ILatticeMergeModeResolver>()?.Resolve(registration.SourceTreeId);
        return mode is null or LatticeMergeMode.LwwRegister ? null : mode.Value.ToString();
    }

    /// <summary>
    /// Clamps an entry-scan's start bound up to
    /// <see cref="AggregationRowCodec.FirstNonReservedKey"/> when
    /// <paramref name="treeId"/> names an aggregation (grouped-reduce or
    /// custom-fold) view tree and the caller's start is unset or sorts inside the
    /// reserved region. This mirrors <c>LatticeView</c>'s reserved floor so the
    /// state API's scan of a view returns only its materialised group values,
    /// never the internal accumulator / inverse / membership / fold-inverse rows
    /// kept under the reserved NUL prefix. Any other tree - and any non-reserved
    /// caller-supplied start - is returned unchanged.
    /// </summary>
    private string? ClampViewScanStart(string treeId, string? startInclusive)
    {
        if (!IsAggregationViewTree(treeId))
        {
            return startInclusive;
        }

        return string.IsNullOrEmpty(startInclusive)
            || string.CompareOrdinal(startInclusive, AggregationRowCodec.FirstNonReservedKey) < 0
            ? AggregationRowCodec.FirstNonReservedKey
            : startInclusive;
    }

    /// <summary>
    /// Tests whether <paramref name="treeId"/> names an aggregation
    /// (grouped-reduce or custom-fold) materialised-view tree by looking up its
    /// registration in the local <see cref="IViewCatalog"/>. Degrades to
    /// <see langword="false"/> (no clamping) when the view infrastructure is not
    /// registered, the view name is unrecoverable, or the view is not in the local
    /// catalog - the same conservative fallback as <see cref="ResolveViewCrdtShape"/>.
    /// </summary>
    private bool IsAggregationViewTree(string treeId)
    {
        if (!IsViewTree(treeId))
        {
            return false;
        }

        var catalog = _services.GetService<IViewCatalog>();
        if (catalog is null)
        {
            return false;
        }

        var viewName = ViewNameFromTreeId(treeId);
        if (viewName.Length == 0)
        {
            return false;
        }

        try
        {
            return catalog.TryGet(viewName) is { IsAggregation: true };
        }
        catch (Exception)
        {
            return false;
        }
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
    /// Tests whether <paramref name="treeId"/> names a dogfooded system-data tree
    /// (the <see cref="LatticeConstants.SystemDataTreePrefix"/>) owned by the
    /// identity / authorization add-ons. These are real registered, individually
    /// inspectable trees - so they are deliberately <b>not</b> treated as
    /// <see cref="IsReservedTree"/> for the per-tree read / scan / cursor paths -
    /// but they are hidden from the default tree catalog, appearing only when a
    /// <see cref="CatalogRequest.IncludeSystemTrees"/> request opts in.
    /// </summary>
    private static bool IsSystemDataTree(string treeId) =>
        treeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal);

    /// <summary>
    /// Tests whether <paramref name="treeId"/> names a silo-internal system tree
    /// (the <see cref="LatticeConstants.SystemTreePrefix"/>). System trees are
    /// hidden from every public surface; unlike materialised-view trees they are
    /// never inspectable.
    /// </summary>
    private static bool IsSystemTree(string treeId) =>
        treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);

    /// <summary>
    /// Tests whether <paramref name="treeId"/> names a materialised-view tree
    /// (the <see cref="LatticeConstants.ViewTreePrefix"/>). A view tree is a
    /// read-only tree that requires an authorised <see cref="ViewReadContext"/>
    /// scope to read its contents.
    /// </summary>
    private static bool IsViewTree(string treeId) =>
        treeId.StartsWith(LatticeConstants.ViewTreePrefix, StringComparison.Ordinal);

    /// <summary>
    /// Opens an authorised <see cref="ViewReadContext"/> read scope when
    /// <paramref name="treeId"/> names a materialised-view (<c>view-*</c>) tree, so
    /// the read-only inspection paths may read its active generation; returns
    /// <see langword="null"/> for ordinary trees, which need no scope.
    /// </summary>
    private static IDisposable? OpenViewReadScopeIfNeeded(string treeId) =>
        IsViewTree(treeId) ? ViewReadContext.BeginScope() : null;

    /// <summary>
    /// Resolves the physical tree id to bind for a read. An ordinary tree id is
    /// returned unchanged; a materialised-view (<c>view-*</c>) tree id is resolved
    /// through its <see cref="IViewMaintainerGrain"/> to the active generation's
    /// tree id, so a read follows a shadow-swap rebuild rather than binding the
    /// stale generation-0 alias. Returns <see langword="null"/> when the view tree
    /// id carries no recoverable view name.
    /// </summary>
    private async Task<string?> ResolveReadTreeIdAsync(string treeId, CancellationToken cancellationToken)
    {
        if (!IsViewTree(treeId))
        {
            return treeId;
        }

        var viewName = ViewNameFromTreeId(treeId);
        if (viewName.Length == 0)
        {
            return null;
        }

        try
        {
            var maintainer = _grainFactory.GetGrain<IViewMaintainerGrain>(viewName);
            return await maintainer.GetActiveTreeIdAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // No locally-resolvable maintainer (e.g. a ship-view consumer, a view
            // fronted directly by its stable generation-0 tree, or a transient
            // activation failure): fall back to the stable "view-{name}" id, which
            // the caller's existence check still gates.
            return treeId;
        }
    }

    /// <summary>
    /// Recovers the maintainer key (the bare view name) from a materialised-view
    /// tree id by stripping the reserved <see cref="LatticeConstants.ViewTreePrefix"/>
    /// and any explicit <c>#g{N}</c> generation suffix.
    /// </summary>
    private static string ViewNameFromTreeId(string treeId)
    {
        var name = treeId.AsSpan(LatticeConstants.ViewTreePrefix.Length);
        var hash = name.IndexOf('#');
        return (hash >= 0 ? name[..hash] : name).ToString();
    }

    private static bool IsTagIndexTree(string treeId) =>
        treeId.StartsWith(LatticeConstants.TagIndexTreePrefix, StringComparison.Ordinal);

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
                    registration.IsAggregation,
                    registration.Accumulative);
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
                    registration.IsAggregation,
                    registration.Accumulative);
            }
        }

        return byName.Values;
    }

    private readonly record struct ViewListing(string ViewName, string SourceTreeId, bool IsAggregation, bool IsHistory);

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
            RestoreShadowOfTreeId = entry?.RestoreShadowOfTreeId,
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
