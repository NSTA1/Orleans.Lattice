namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Public-surface forwarders for the orphaned-leaf inspection and repair
/// operator tooling (issue 3269). Fans out over every physical shard of the
/// tree, drives each shard's work-bounded batches to completion, and reduces
/// the per-shard findings into one report.
/// <para>
/// Guarded by <see cref="LatticeGrain.ThrowIfSystemTree"/> so reserved system
/// trees cannot be repaired through the public surface, and gated like its
/// siblings in <c>LatticeGrain.ProjectionAdmin</c>: inspection enforces
/// <see cref="LatticeOperation.Read"/> because it only observes, repair
/// enforces <see cref="LatticeOperation.Admin"/> because it removes leaves.
/// </para>
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public Task<OrphanedLeafRepairReport> InspectOrphanedLeavesAsync(
        CancellationToken cancellationToken = default) =>
        DriveOrphanedLeafPassAsync(dryRun: true, cancellationToken);

    /// <inheritdoc />
    public Task<OrphanedLeafRepairReport> RepairOrphanedLeavesAsync(
        CancellationToken cancellationToken = default) =>
        DriveOrphanedLeafPassAsync(dryRun: false, cancellationToken);

    private async Task<OrphanedLeafRepairReport> DriveOrphanedLeafPassAsync(
        bool dryRun,
        CancellationToken cancellationToken)
    {
        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // Read for the inspection, Admin for the repair. The split matters
        // because the whole point of the inspection verb is that an operator
        // can run it before deciding to take the Admin-gated action, and
        // gating both identically would remove that step.
        await EnforceWholeTreeAsync(
            dryRun ? LatticeOperation.Read : LatticeOperation.Admin,
            cancellationToken);

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        var physicalShards = shardMap.GetPhysicalShardIndices();

        var leavesWalked = 0;
        var findings = new List<OrphanedLeafFinding>();
        var gaps = new List<OrphanedLeafAuditGap>();

        // Sequential across shards, not concurrent, and deliberately unlike
        // GetMaterialiserLagAsync next door. That verb reads; this one walks
        // every leaf of every shard and may unsplice some of them, so fanning
        // out concurrently would activate the entire tree at once - which on a
        // tree large enough to have acquired an orphan is the cost the pass
        // exists to relieve, not one to add. An operator repair is not on a
        // latency budget.
        foreach (var shardIndex in physicalShards)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");

            string? cursor = null;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var currentCursor = cursor;
                var page = await ShardActivationRetry.RunAsync(
                    () => shard.RepairOrphanedLeavesAsync(currentCursor, dryRun, cancellationToken),
                    cancellationToken);

                leavesWalked += page.LeavesWalked;
                if (page.Findings is { Count: > 0 }) findings.AddRange(page.Findings);
                if (page.Gaps is { Count: > 0 }) gaps.AddRange(page.Gaps);

                if (page.ResumeFromInclusive is not { } next) break;
                cursor = next;
            }
        }

        return new OrphanedLeafRepairReport
        {
            DryRun = dryRun,
            LeavesWalked = leavesWalked,
            Findings = findings,
            Gaps = gaps,
        };
    }
}
