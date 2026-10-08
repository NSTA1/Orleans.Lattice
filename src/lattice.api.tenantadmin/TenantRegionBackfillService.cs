using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Advances one tenant's local-region backfill by one retryable phase step.
/// Durable scheduling and restart recovery belong to
/// <see cref="TenantRegionBackfillCoordinatorGrain"/>.
/// </summary>
internal sealed class TenantRegionBackfillService(
    ITenantRegistry registry,
    TenantRegionLifecycleDriver lifecycle,
    IGrainFactory grainFactory,
    IOptions<ClusterOptions> clusterOptions,
    ILogger<TenantRegionBackfillService> logger,
    ILatticeBootstrapCoordinator? bootstrap = null,
    ILatticeReplicationDeadLetters? deadLetters = null,
    IOptionsMonitor<LatticeReplicationOptions>? replicationOptions = null,
    ILatticeReplicationContext? replicationContext = null,
    ILatticeReplicationConfigAuthority? replicationConfigAuthority = null)
{
    internal string LocalRegionId { get; } = string.IsNullOrEmpty(clusterOptions.Value.ClusterId)
        ? "default"
        : clusterOptions.Value.ClusterId;

    /// <summary>Reads the progress visible for the local region without advancing it.</summary>
    internal async Task<TenantRegionBackfillProgress> GetProgressAsync(
        TenantId tenant,
        string regionId,
        CancellationToken cancellationToken = default)
    {
        if (!string.Equals(regionId, LocalRegionId, StringComparison.Ordinal))
        {
            return new TenantRegionBackfillProgress { Phase = "NotLocal", Trees = [] };
        }

        (List<string> Trees, string? StallReason) discovery;
        try
        {
            discovery = await GetTenantTreesAsync(tenant, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new TenantRegionBackfillProgress
            {
                Phase = "Unavailable",
                StallReason = "Tenant-tree discovery failed; backfill remains safely paused.",
                Trees = [],
            };
        }

        var trees = discovery.Trees;
        if (discovery.StallReason is { } stallReason)
        {
            return new TenantRegionBackfillProgress
            {
                Phase = "Blocked",
                StallReason = stallReason,
                Trees = [],
            };
        }

        if (trees.Count == 0)
        {
            return new TenantRegionBackfillProgress { Phase = "Complete", Trees = [] };
        }

        if (bootstrap is null)
        {
            return new TenantRegionBackfillProgress
            {
                Phase = "Unavailable",
                StallReason = "Replication bootstrap is not registered in this region.",
                Trees = [],
            };
        }

        if (deadLetters is null)
        {
            return new TenantRegionBackfillProgress
            {
                Phase = "Unavailable",
                StallReason = "Replication dead-letter replay is not registered in this region.",
                Trees = [],
            };
        }

        var record = await registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        var onlineSources = record?.RegionStatusEntries
            .Where(entry => entry.Value == TenantRegionStatus.Online
                && !string.Equals(entry.Key, LocalRegionId, StringComparison.Ordinal))
            .Select(entry => entry.Key)
            .ToHashSet(StringComparer.Ordinal) ?? new HashSet<string>(StringComparer.Ordinal);

        var statuses = new List<TenantRegionBackfillTreeProgress>(trees.Count);
        try
        {
            foreach (var treeId in trees)
            {
                var status = await bootstrap.GetStatusAsync(treeId, cancellationToken).ConfigureAwait(false);
                var parked = await deadLetters.ListAsync(treeId, cancellationToken).ConfigureAwait(false);
                var pendingDeadLetters = parked.Count(entry =>
                    string.Equals(entry.ReasonTag, LatticeReplicationMetrics.ReasonTenantOffline, StringComparison.Ordinal));
                statuses.Add(new TenantRegionBackfillTreeProgress
                {
                    TreeId = treeId,
                    Phase = status.Phase.ToString(),
                    EntriesApplied = status.EntriesApplied,
                    SourceClusterId = status.SourceClusterId ?? status.CompletedSourceClusterId,
                    ReadFenced = status.ReadFenced,
                    PendingDeadLetters = pendingDeadLetters,
                });
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return new TenantRegionBackfillProgress
            {
                Phase = "Unavailable",
                StallReason = "Replication backfill status is temporarily unavailable; automatic reconciliation will retry.",
                Trees = statuses,
            };
        }

        var failed = statuses.Any(tree => string.Equals(tree.Phase, nameof(LatticeBootstrapState.Failed), StringComparison.Ordinal));
        var fenced = statuses.Any(tree => tree.ReadFenced);
        var pending = statuses.Any(tree => tree.PendingDeadLetters > 0);
        var offlineSource = statuses
            .Select(tree => tree.SourceClusterId)
            .FirstOrDefault(source => source is not null && !onlineSources.Contains(source));
        var incomplete = failed || fenced || pending
            || statuses.Any(tree => !string.Equals(tree.Phase, nameof(LatticeBootstrapState.LiveIncremental), StringComparison.Ordinal));

        return new TenantRegionBackfillProgress
        {
            Phase = failed || fenced || pending || offlineSource is not null
                || (trees.Count > 0 && onlineSources.Count == 0)
                ? "Blocked"
                : incomplete ? "Running" : "Complete",
            StallReason = failed
                ? "A tenant-tree bootstrap failed; automatic retries continue. Check replication peer and storage health."
                : fenced
                    ? "A tenant-tree replica is still read-fenced because bootstrap may be partial."
                    : pending
                        ? "Tenant-offline dead letters remain and must replay before the region can go Online."
                        : onlineSources.Count == 0
                            ? "No Online source region is available; backfill remains safely paused."
                            : offlineSource is not null
                                ? $"Bootstrap source region '{offlineSource}' is no longer Online; automatic retry will use an available source when the current attempt settles."
                                : incomplete
                            ? "Waiting for every tenant tree to finish its receiver bootstrap."
                            : null,
            Trees = statuses,
        };
    }

    internal async Task<TenantRegionStatus> AdvanceTenantAsync(
        TenantId tenant,
        string? sourceClusterId,
        CancellationToken cancellationToken)
    {
        var record = await registry.GetAsync(tenant, cancellationToken).ConfigureAwait(false);
        if (record is null)
        {
            return TenantRegionStatus.None;
        }

        var observedStatus = record.GetRegionStatus(LocalRegionId);
        if (observedStatus == TenantRegionStatus.Provisioning)
        {
            observedStatus = await lifecycle.BeginBackfillAsync(tenant, LocalRegionId, cancellationToken)
                .ConfigureAwait(false);
        }

        if (observedStatus != TenantRegionStatus.Backfilling)
        {
            return observedStatus;
        }

        var discovery = await GetTenantTreesAsync(tenant, cancellationToken).ConfigureAwait(false);
        if (discovery.StallReason is { } stallReason)
        {
            logger.LogWarning(
                "Backfill for tenant '{Tenant}' in region '{Region}' is blocked: {Reason}",
                tenant,
                LocalRegionId,
                stallReason);
            return TenantRegionStatus.Backfilling;
        }

        var tenantTrees = discovery.Trees;
        if (tenantTrees.Count == 0)
        {
            return await lifecycle.CompleteBackfillAsync(
                tenant, LocalRegionId, backfillVerified: true, cancellationToken)
                .ConfigureAwait(false);
        }

        if (bootstrap is null)
        {
            logger.LogWarning(
                "Tenant '{Tenant}' remains Backfilling in region '{Region}' because replication bootstrap is unavailable.",
                tenant,
                LocalRegionId);
            return TenantRegionStatus.Backfilling;
        }

        if (sourceClusterId is null
            || !record.RegionStatusEntries.Any(entry =>
                string.Equals(entry.Key, sourceClusterId, StringComparison.Ordinal)
                && entry.Value == TenantRegionStatus.Online))
        {
            logger.LogWarning(
                "Tenant '{Tenant}' remains Backfilling in region '{Region}': no Online source region is available.",
                tenant,
                LocalRegionId);
            return TenantRegionStatus.Backfilling;
        }

        foreach (var treeId in tenantTrees)
        {
            var status = await bootstrap.GetStatusAsync(treeId, cancellationToken).ConfigureAwait(false);
            var bootstrapSource = status.SourceClusterId ?? status.CompletedSourceClusterId;
            if (status.Phase is LatticeBootstrapState.Idle or LatticeBootstrapState.Failed
                || (status.Phase == LatticeBootstrapState.LiveIncremental
                    && !string.Equals(bootstrapSource, sourceClusterId, StringComparison.Ordinal)))
            {
                await bootstrap.BootstrapAsync(treeId, sourceClusterId, cancellationToken).ConfigureAwait(false);
                status = await bootstrap.GetStatusAsync(treeId, cancellationToken).ConfigureAwait(false);
                bootstrapSource = status.SourceClusterId ?? status.CompletedSourceClusterId;
            }

            if (!string.Equals(bootstrapSource, sourceClusterId, StringComparison.Ordinal)
                || status.Phase != LatticeBootstrapState.LiveIncremental
                || status.ReadFenced)
            {
                return TenantRegionStatus.Backfilling;
            }

            if (!await ReplayParkedTenantEntriesAsync(treeId, cancellationToken).ConfigureAwait(false))
            {
                return TenantRegionStatus.Backfilling;
            }
        }

        return await lifecycle.CompleteBackfillAsync(
            tenant, LocalRegionId, backfillVerified: true, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<(List<string> Trees, string? StallReason)> GetTenantTreesAsync(
        TenantId tenant, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var latticeRegistry = grainFactory.GetLatticeRegistry();
        var registered = await latticeRegistry
            .GetAllTreeIdsAsync(LatticeTenantTrees.ComposePrefix(tenant))
            .ConfigureAwait(false);

        IReadOnlyDictionary<string, LatticeReplicationTreeStatus>? replicationTrees = null;
        if (replicationConfigAuthority is not null)
        {
            replicationTrees = await replicationConfigAuthority.GetAllTreeStatusesAsync(cancellationToken)
                .ConfigureAwait(false);
        }

        var configuredTreeIds = new HashSet<string>(StringComparer.Ordinal);
        if (replicationTrees is not null)
        {
            configuredTreeIds.UnionWith(replicationTrees.Keys);
        }

        var staticReplicatedTrees = replicationOptions?.CurrentValue.ReplicatedTrees;
        if (staticReplicatedTrees is not null)
        {
            configuredTreeIds.UnionWith(staticReplicatedTrees.Keys);
        }

        var treeSet = new HashSet<string>(StringComparer.Ordinal);
        foreach (var treeId in registered)
        {
            if (LatticeTenantTrees.TryGetTenant(treeId, out var owner) && owner == tenant)
            {
                var aliases = await latticeRegistry.GetAliasesTargetingAsync(treeId).ConfigureAwait(false);
                var isConfiguredAliasTarget = aliases.Any(alias =>
                    configuredTreeIds.Contains(alias)
                    || replicationContext?.ResolveMergeMode(alias) is not null);
                if (!isConfiguredAliasTarget || configuredTreeIds.Contains(treeId))
                {
                    treeSet.Add(treeId);
                }
            }
        }

        if (replicationTrees is not null)
        {
            foreach (var treeId in replicationTrees.Keys)
            {
                if (LatticeTenantTrees.TryGetTenant(treeId, out var owner) && owner == tenant)
                {
                    treeSet.Add(treeId);
                }
            }
        }

        if (staticReplicatedTrees is not null)
        {
            foreach (var treeId in staticReplicatedTrees.Keys)
            {
                if (LatticeTenantTrees.TryGetTenant(treeId, out var owner) && owner == tenant)
                {
                    treeSet.Add(treeId);
                }
            }
        }

        var trees = treeSet.Order(StringComparer.Ordinal).ToList();
        foreach (var treeId in trees)
        {
            var enrolled = replicationContext is not null
                ? replicationContext.ResolveMergeMode(treeId) is not null
                : replicationTrees is not null
                    ? replicationTrees.TryGetValue(treeId, out var status)
                        && status.Enabled
                        && !status.Ambiguous
                        && status.Mode is not null
                    : replicationOptions?.Get(treeId).ReplicatedTrees?.ContainsKey(treeId) == true;
            if (!enrolled)
            {
                return (trees, $"Tenant tree '{treeId}' is not enrolled for replication in this region.");
            }
        }

        return (trees, null);
    }

    private async Task<bool> ReplayParkedTenantEntriesAsync(string treeId, CancellationToken cancellationToken)
    {
        if (deadLetters is null)
        {
            return false;
        }

        var parked = await deadLetters.ListAsync(treeId, cancellationToken).ConfigureAwait(false);
        foreach (var entry in parked)
        {
            if (!string.Equals(entry.ReasonTag, LatticeReplicationMetrics.ReasonTenantOffline, StringComparison.Ordinal))
            {
                continue;
            }

            var result = await deadLetters.ReplayAsync(treeId, entry.EntryId, cancellationToken).ConfigureAwait(false);
            if (result is { Deferred: true } or { SourceLineageRefused: true })
            {
                return false;
            }
        }

        var remaining = await deadLetters.ListAsync(treeId, cancellationToken).ConfigureAwait(false);
        return !remaining.Any(entry =>
            string.Equals(entry.ReasonTag, LatticeReplicationMetrics.ReasonTenantOffline, StringComparison.Ordinal));
    }
}
