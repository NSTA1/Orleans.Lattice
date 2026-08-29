using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Bulk-load, tree deletion, recovery, and purge operations.
/// </summary>
internal sealed partial class LatticeGrain
{
    public async Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfUserOriginSystemDataTree();
        ThrowIfProtectedView();
        ThrowIfLwwWriteToCrdtReplicatedTree();
        ArgumentNullException.ThrowIfNull(entries);
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.BulkLoad, cancellationToken);
        await ThrowIfWriteNotAdmittedAsync(cancellationToken);
        IReadOnlyList<KeyValuePair<string, byte[]>> effectiveEntries = entries;
        if (WriteInterceptionActive)
        {
            var list = entries as List<KeyValuePair<string, byte[]>>
                ?? new List<KeyValuePair<string, byte[]>>(entries);
            effectiveEntries = await InterceptEntriesAsync(
                LatticeOperation.BulkLoad, list, atomic: false, cancellationToken);
        }
        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();
        var physicalShards = shardMap.GetPhysicalShardIndices();
        var operationId = Guid.NewGuid().ToString("N");

        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>(physicalShards.Count);
        foreach (var idx in physicalShards)
            shardBuckets[idx] = [];

        foreach (var entry in effectiveEntries)
        {
            var idx = shardMap.Resolve(entry.Key);
            shardBuckets[idx].Add(entry);
        }

        // Fan out to *every* physical shard, including those whose bucket
        // is empty. Each shard's own `RootNodeId is not null` guard inside
        // `ShardRootGrain.BulkLoadAsync` fires before the empty-list
        // short-circuit, so a shard that already contains data on this
        // tree (from a prior `SetAsync` or earlier bulk load) rejects the
        // call with `InvalidOperationException`. Skipping empty buckets
        // would silently miss the case where pre-existing data lives on
        // a shard that the current batch's keys do not partition into.
        // Per the contract on `ILattice.BulkLoadAsync`, "Throws
        // InvalidOperationException if any shard already contains data".
        //
        // Per-shard buckets are sorted up-front. Each shard's RPC is
        // wrapped in its own ShardActivationRetry envelope so a single
        // shard's seed-timeout only retries that shard, not every
        // sibling. The shard-side `operationId` deduplication keeps the
        // per-shard retry idempotent.
        foreach (var (_, bucket) in shardBuckets)
        {
            if (bucket.Count > 0)
                bucket.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
        }

        var tasks = new List<Task>(shardBuckets.Count);
        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            var capturedShardIdx = shardIdx;
            var capturedBucket = bucket;
            tasks.Add(ShardActivationRetry.RunAsync(
                () => shard.BulkLoadAsync($"{operationId}-{capturedShardIdx}", capturedBucket),
                cancellationToken));
        }

        await Task.WhenAll(tasks);
    }

    public async Task<int> BulkAppendChunkAsync(
        string operationId,
        IReadOnlyList<KeyValuePair<string, byte[]>> sortedEntries,
        CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfUserOriginSystemDataTree();
        ThrowIfProtectedView();
        ThrowIfLwwWriteToCrdtReplicatedTree();
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentNullException.ThrowIfNull(sortedEntries);
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.BulkLoad, cancellationToken);
        await ThrowIfWriteNotAdmittedAsync(cancellationToken);

        if (sortedEntries.Count == 0)
            return 0;

        IReadOnlyList<KeyValuePair<string, byte[]>> effectiveEntries = sortedEntries;
        if (WriteInterceptionActive)
        {
            var list = sortedEntries as List<KeyValuePair<string, byte[]>>
                ?? new List<KeyValuePair<string, byte[]>>(sortedEntries);
            effectiveEntries = await InterceptEntriesAsync(
                LatticeOperation.BulkLoad, list, atomic: false, cancellationToken);
        }

        if (effectiveEntries.Count == 0)
            return 0;

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Partition this chunk across physical shards. Only shards that actually
        // receive an entry are grafted, so a sparse (post-split) map costs
        // nothing for the shards this chunk does not touch.
        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        foreach (var entry in effectiveEntries)
        {
            var idx = shardMap.Resolve(entry.Key);
            if (!shardBuckets.TryGetValue(idx, out var bucket))
            {
                bucket = [];
                shardBuckets[idx] = bucket;
            }
            bucket.Add(entry);
        }

        // Each shard's graft is idempotent under the deterministic per-shard
        // operation id "{operationId}-{shardIndex}": re-driving the same chunk
        // reissues the same ids, which the shard's LastCompletedBulkOperationId
        // dedup short-circuits. Each shard's RPC is wrapped in its own
        // ShardActivationRetry envelope so a single shard's seed timeout retries
        // only that shard, not its siblings.
        var tasks = new List<Task>(shardBuckets.Count);
        foreach (var (shardIdx, bucket) in shardBuckets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bucket.Sort(static (a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIdx}");
            var capturedShardIdx = shardIdx;
            var capturedBucket = bucket;
            tasks.Add(ShardActivationRetry.RunAsync(
                () => shard.BulkAppendAsync($"{operationId}-{capturedShardIdx}", capturedBucket),
                cancellationToken));
        }

        await Task.WhenAll(tasks);
        return effectiveEntries.Count;
    }

    public async Task DeleteTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.TreeLifecycle, cancellationToken);
        await ThrowIfSourceOfMaterialisedViewAsync(cancellationToken);
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => deletion.DeleteTreeAsync(),
            cancellationToken);

        // The tree's catalogue row is gone, so drop the sticky registration memo
        // this activation may be holding: without the reset a delete arriving on
        // the same activation would route into the shard roots and re-provision
        // the tree that was just retired.
        InvalidateRegistrationMemo();
    }

    /// <summary>
    /// Rejects deleting this tree while it is the source of one or more
    /// materialised views. A view derives its contents from its source tree's
    /// write-ahead log; deleting the source out from under a live view would
    /// freeze the view at stale state, leak the maintainer's source-WAL cursor
    /// pin, and fault any later view rebuild. The dependent view(s) must be torn
    /// down first via <c>ILatticeViewFactory.DeleteAsync</c>. The guard is an
    /// optional service registered only by <c>AddLatticeViews</c>, so a host
    /// without materialised views resolves nothing and is unaffected.
    /// </summary>
    private async Task ThrowIfSourceOfMaterialisedViewAsync(CancellationToken cancellationToken)
    {
        var guard = services.GetService<IViewSourceGuard>();
        if (guard is null)
        {
            return;
        }

        var dependents = await guard.FindDependentViewsAsync(TreeId, cancellationToken);
        if (dependents.Count > 0)
        {
            throw new InvalidOperationException(
                $"Tree '{TreeId}' cannot be deleted because {dependents.Count} materialised view(s) derive from it: {string.Join(", ", dependents)}. Delete the dependent view(s) first via ILatticeViewFactory.DeleteAsync, then delete the source tree.");
        }
    }

    public async Task RecoverTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.TreeLifecycle, cancellationToken);
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => deletion.RecoverAsync(),
            cancellationToken);
    }

    public async Task PurgeTreeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.TreeLifecycle, cancellationToken);
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => deletion.PurgeNowAsync(),
            cancellationToken);
        InvalidateRegistrationMemo();
    }

    public async Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.TreeLifecycle, cancellationToken);
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => resize.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren),
            cancellationToken);
    }

    public async Task UndoResizeAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.TreeLifecycle, cancellationToken);
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => resize.UndoResizeAsync(),
            cancellationToken);
    }

    public async Task SnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => snapshot.SnapshotAsync(destinationTreeId, mode, maxLeafKeys, maxInternalChildren),
            cancellationToken);
    }

    public async Task<bool> TreeExistsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // A direct, unqualified existence oracle: it previously performed no gate
        // call at all, so any caller able to address the grain could probe whether
        // an arbitrary tree id is registered - and, because a tenant tree id is a
        // caller-composable `t/{tenant}/{name}` string, enumerate the tenant roster
        // and each tenant's tree names by dictionary probing.
        //
        // Denial reports the tree as ABSENT rather than throwing: the security
        // posture guarantees a caller who cannot read a tree's source data cannot
        // distinguish "exists but I cannot read it" from "does not exist", and
        // every consumer of this verb already treats false as a clean not-found.
        // A partial (prefix) allow still reports existence - the caller may read
        // part of the tree, so its existence is not a secret from them.
        if (!await IsWholeTreeReadAllowedAsync(cancellationToken))
        {
            return false;
        }

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.ExistsAsync(TreeId);
    }

    public async Task<IReadOnlyList<string>> GetAllTreeIdsAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // Enumerating every tree id in the cluster is a whole-tree read of the
        // registry, so it is gated exactly as its siblings in this file are
        // (SnapshotAsync, SetPublishEventsEnabledAsync, SetHistoryRetentionAsync).
        // This call previously performed no gate call at all, so an in-cluster
        // client could enumerate the whole catalog - including every other
        // tenant's tree ids - with only the (tenant-conditional) filter below
        // between it and the registry.
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var treeIds = await registry.GetAllTreeIdsAsync();
        return FilterTreeIdsByActiveTenant(treeIds);
    }

    /// <summary>
    /// Prunes a tree-id enumeration to the trees the ambient active tenant may
    /// observe. When tenancy is off - no <see cref="ITenantEnumerationFilter"/>
    /// is registered, the registered filter is inactive, or no active tenant is
    /// stamped on <see cref="LatticeActiveTenantContext"/> - the original list is
    /// returned unchanged (same reference, zero allocation), so an enumeration is
    /// byte-for-byte identical to a non-tenant cluster. Only when a filter is
    /// active and a tenant is present does the seam allocate the filtered subset.
    /// </summary>
    private IReadOnlyList<string> FilterTreeIdsByActiveTenant(IReadOnlyList<string> treeIds)
    {
        var filter = services.GetService<ITenantEnumerationFilter>();
        if (filter is not { IsActive: true })
        {
            return treeIds;
        }

        if (LatticeActiveTenantContext.Current is not { } tenant)
        {
            return treeIds;
        }

        return filter.Filter(tenant, treeIds);
    }

    public async Task SetPublishEventsEnabledAsync(bool? enabled, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetPublishEventsAsync(TreeId, enabled);
        // Make sure this activation re-reads the registry next time it publishes
        // so the override takes effect immediately locally.
        _eventsGate.Invalidate();
        LatticeMetrics.ConfigChanged.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagConfig, "publish_events"),
            LatticeTenantLabel.ForTree(TreeId));
    }

    public async Task SetHistoryRetentionAsync(HistoryRetentionMode? mode, TimeSpan? window, CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetHistoryRetentionAsync(TreeId, mode, window);
        LatticeMetrics.ConfigChanged.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagConfig, "history_retention"),
            LatticeTenantLabel.ForTree(TreeId));
    }

    public async Task<HistoryRetentionSettings> GetHistoryRetentionAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // Reading the tree's retention policy is a whole-tree registry read that
        // both discloses configuration and answers existence (an unregistered tree
        // yields the documented defaults). It is gated exactly as its write-side
        // sibling SetHistoryRetentionAsync is, and as the external tree-admin
        // facade already gates the same read, but on Read rather than Admin: this
        // verb only observes.
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = await registry.GetEntryAsync(TreeId);
        return new HistoryRetentionSettings
        {
            Mode = entry?.HistoryRetentionMode ?? HistoryRetentionMode.MetadataOnly,
            Window = entry?.HistoryRetentionWindowTicks is { } ticks
                ? TimeSpan.FromTicks(ticks)
                : TimeSpan.Zero,
        };
    }

    public async Task MergeAsync(string sourceTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sourceTreeId);
        ThrowIfSystemTree();
        ThrowIfProtectedView();
        ThrowIfReservedMergeSource(sourceTreeId);
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Admin, cancellationToken);

        // The caller-supplied source is a second, independent authorization
        // boundary. A merge drains every entry of `sourceTreeId` into this tree,
        // where it is then readable under *this* tree's own read policy, so
        // authorizing only the destination would let a caller holding Admin on a
        // tree it owns siphon any other tree in the cluster - the dogfooded
        // control-plane trees, or another tenant's data - into one it can read.
        // The source must be uniformly readable by the caller: a filtered
        // (partial-coverage) allow is refused rather than narrowed, because a
        // merge that silently copied only an authorized key subset would diverge
        // from the source without telling anyone. Mirrors the source-side guard
        // LatticeTreeAdmin.CreateViewAsync applies to a view's source tree.
        await EnforceSourceTreeReadAsync(sourceTreeId, cancellationToken);

        var merge = grainFactory.GetGrain<ITreeMergeGrain>(TreeId);
        await ShardActivationRetry.RunAsync(
            () => merge.MergeAsync(sourceTreeId),
            cancellationToken);
    }

    /// <summary>
    /// Rejects a user-origin merge whose <paramref name="sourceTreeId"/> names a
    /// tree in a reserved namespace: the internal <c>_lattice_</c> system
    /// namespace, the dogfooded <c>sys-</c> system-data namespace (authorization
    /// policy, membership, backup catalogs), or the structural <c>t/</c> tenant
    /// namespace. Those trees hold control-plane or cross-tenant state that a
    /// user-origin caller must never be able to bulk-copy into an ordinary tree
    /// governed by that tree's own read policy, and - unlike a plain read - a
    /// merge launders the contents past the namespace's protection. Suppressed
    /// under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> so
    /// first-party machinery that legitimately composes these ids is unaffected,
    /// exactly as <see cref="ThrowIfUserOriginSystemDataTree"/> is on the
    /// mutation surface.
    /// </summary>
    /// <param name="sourceTreeId">The caller-supplied merge source.</param>
    private static void ThrowIfReservedMergeSource(string sourceTreeId)
    {
        if (LatticeAccessGateContext.IsSystemOrigin)
        {
            return;
        }

        if (sourceTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)
            || sourceTreeId.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
            || LatticeTenantTrees.IsTenantScoped(sourceTreeId))
        {
            throw new InvalidOperationException(
                $"Merge source tree ID '{sourceTreeId}' is reserved: a merge source may not name a tree in the " +
                $"internal '{LatticeConstants.SystemTreePrefix}' namespace, the '{LatticeConstants.SystemDataTreePrefix}' " +
                $"system-data namespace, or the '{LatticeTenantTrees.SegmentPrefix}' tenant namespace. Merging such a " +
                "tree would copy control-plane or cross-tenant state into a tree governed only by its own read policy.");
        }
    }

    /// <remarks>
    /// Gated on <see cref="LatticeOperation.Read"/>: the verb that
    /// <em>initiates</em> the corresponding operation enforces Admin or
    /// TreeLifecycle, but this status verb previously enforced nothing, so an
    /// unauthorized in-cluster caller could confirm that the tree exists and
    /// observe its lifecycle state. Read (not Admin) because the verb only
    /// observes, matching the choice #1722 made for the observe-only metadata
    /// verbs.
    /// <para>
    /// <b>Internal pollers must carry system origin.</b>
    /// <see cref="HotShardMonitorGrain.RunSamplingPassAsync"/> polls this verb
    /// (and its three siblings) on a timer with no caller identity, so it opens
    /// a <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> scope around
    /// those polls. Without it a deny-by-default tree would deny the monitor and
    /// its catch-and-warn handler would swallow the denial, silently disabling
    /// auto-split.
    /// </para>
    /// </remarks>
    public async Task<bool> IsMergeCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);
        var merge = grainFactory.GetGrain<ITreeMergeGrain>(TreeId);
        return await merge.IsCompleteAsync();
    }

    /// <inheritdoc cref="IsMergeCompleteAsync" />
    public async Task<bool> IsSnapshotCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);
        var snapshot = grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId);
        return await snapshot.IsIdleAsync();
    }

    /// <inheritdoc cref="IsMergeCompleteAsync" />
    public async Task<bool> IsResizeCompleteAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();
        await EnforceWholeTreeAsync(LatticeOperation.Read, cancellationToken);
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        return await resize.IsIdleAsync();
    }
}
