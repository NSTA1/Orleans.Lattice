namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Public-surface forwarders for the orphaned-leaf inspection and repair
/// operator tooling (issue 3269). Fans out over the physical shards of the
/// tree, drives each shard's work-bounded batches, and reduces the per-shard
/// findings into one report.
/// <para>
/// Guarded by <see cref="LatticeGrain.ThrowIfSystemTree"/> so reserved system
/// trees cannot be repaired through the public surface, and gated like its
/// siblings in <c>LatticeGrain.ProjectionAdmin</c>: inspection enforces
/// <see cref="LatticeOperation.Read"/> because it only observes, repair
/// enforces <see cref="LatticeOperation.Admin"/> because it removes leaves.
/// </para>
/// <para>
/// <b>The fan-out is itself bounded and resumable (issue 3302).</b> Each shard
/// batch was already bounded, but an earlier revision drove every shard and
/// every batch to completion inside one grain call, so on a damaged tree the
/// call outran the client's response deadline. The caller then saw a
/// <c>TimeoutException</c> while the grain went on to finish the repair
/// successfully - the operation reported as failed having entirely succeeded,
/// with its report and its per-leaf dispositions discarded along with the
/// exception. Worse, the obvious response to a timeout is to retry, which
/// started a second repair pass over a chain the first was still mutating.
/// This file now returns after a bounded batch with a cursor, so the deadline
/// is not the thing that decides whether an operator gets an answer.
/// </para>
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public Task<OrphanedLeafRepairReport> InspectOrphanedLeavesAsync(
        string? resumeFrom = null,
        CancellationToken cancellationToken = default) =>
        DriveOrphanedLeafPassAsync(dryRun: true, resumeFrom, cancellationToken);

    /// <inheritdoc />
    public Task<OrphanedLeafRepairReport> RepairOrphanedLeavesAsync(
        string? resumeFrom = null,
        CancellationToken cancellationToken = default) =>
        DriveOrphanedLeafPassAsync(dryRun: false, resumeFrom, cancellationToken);

    private async Task<OrphanedLeafRepairReport> DriveOrphanedLeafPassAsync(
        bool dryRun,
        string? resumeFrom,
        CancellationToken cancellationToken)
    {
        // Start the clock before the authorization and routing prologue rather
        // than at the fan-out loop. Both are runs of grain calls, and on a cold
        // activation they are slow ones; a deadline that excluded them would
        // grant a call that had already spent much of the client's budget a
        // further full budget of shard walking. Same reasoning as the per-shard
        // pass (issue 1992).
        var startTimestamp = LeafWalkBudget.StartClock();

        ThrowIfSystemTree();
        cancellationToken.ThrowIfCancellationRequested();

        // Decoded before the gate so a malformed token is rejected as the
        // argument error it is, rather than after an authorization round trip.
        var cursor = OrphanedLeafPassCursor.Decode(resumeFrom);

        // Read for the inspection, Admin for the repair. The split matters
        // because the whole point of the inspection verb is that an operator
        // can run it before deciding to take the Admin-gated action, and
        // gating both identically would remove that step.
        await EnforceWholeTreeAsync(
            dryRun ? LatticeOperation.Read : LatticeOperation.Admin,
            cancellationToken);

        var (physicalTreeId, shardMap) = await GetRoutingAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Sorted rather than taken in map order. The cursor names a shard by
        // INDEX, and resuming means "skip every shard below it", which is only
        // the same thing as "skip the ones already done" when the walk visits
        // indices in ascending order.
        //
        // This sort is DEFENSIVE AND CURRENTLY REDUNDANT, and that is stated
        // rather than hidden because the honest version is the useful one: both
        // of ShardMap.GetPhysicalShardIndices' paths happen to return ascending
        // order today (the bitmap fast path by construction, the HashSet
        // fallback by a trailing Array.Sort of its own), so removing this line
        // changes no observable behaviour and no test can currently go red for
        // it. What is absent is a *declared* ordering guarantee: the method's
        // contract does not promise sorted output, and its fallback branch is
        // documented as unexercised by any production path, so it is precisely
        // the branch a future change would feel free to alter. The cost here is
        // an already-sorted sort over the shard count; the cost of being wrong
        // is a pass that silently skips a shard and reports a clean tree.
        var physicalShards = shardMap.GetPhysicalShardIndices().ToArray();
        Array.Sort(physicalShards);

        var options = await optionsResolver.ResolveAsync(TreeId);
        var budget = LeafWalkBudget.ForOrphanedLeafPass(options, startTimestamp);

        var leavesWalked = 0;
        var findings = new List<OrphanedLeafFinding>();
        string? nextResumeFrom = null;

        // Sequential across shards, not concurrent, and deliberately unlike
        // GetMaterialiserLagAsync next door. That verb reads; this one walks
        // every leaf of every shard and may unsplice some of them, so fanning
        // out concurrently would activate the entire tree at once - which on a
        // tree large enough to have acquired an orphan is the cost the pass
        // exists to relieve, not one to add.
        for (var position = 0; position < physicalShards.Length && nextResumeFrom is null; position++)
        {
            var shardIndex = physicalShards[position];

            // Below the cursor means a previous batch walked this shard's
            // chain to the end. A shard the cursor names that no longer exists
            // (a reshard between batches) simply has nothing at or above it
            // here, which ends the pass - correctly, because the pass is
            // idempotent and re-running from null re-establishes the truth.
            if (shardIndex < cursor.ShardIndex) continue;

            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var shardCursor = shardIndex == cursor.ShardIndex ? cursor.ResumeFromInclusive : null;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var currentCursor = shardCursor;
                var page = await ShardActivationRetry.RunAsync(
                    () => shard.RepairOrphanedLeavesAsync(currentCursor, dryRun, cancellationToken),
                    cancellationToken);

                leavesWalked += page.LeavesWalked;
                if (page.Findings is { Count: > 0 }) findings.AddRange(page.Findings);

                shardCursor = page.ResumeFromInclusive;

                // This shard's chain is exhausted; fall through to the next.
                if (shardCursor is null) break;

                if (!budget.ShouldYield()) continue;

                nextResumeFrom = OrphanedLeafPassCursor.Encode(shardIndex, shardCursor);
                break;
            }

            // The shard finished. Yield between shards too, and only when
            // another shard actually remains: encoding a resume position past
            // the last shard would cost the operator a whole extra round trip
            // to be told there was nothing left, and would report a tree that
            // had in fact been examined end to end as incomplete.
            if (nextResumeFrom is null
                && position + 1 < physicalShards.Length
                && budget.ShouldYield())
            {
                nextResumeFrom = OrphanedLeafPassCursor.Encode(physicalShards[position + 1], null);
            }
        }

        return new OrphanedLeafRepairReport
        {
            DryRun = dryRun,
            LeavesWalked = leavesWalked,
            Findings = findings,
            ResumeFrom = nextResumeFrom,
        };
    }
}
