using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stops and re-arms a tree's per-tree autonomic loops across its deletion
/// lifecycle (issue #4219).
/// </summary>
/// <remarks>
/// <para>
/// The first write to a tree arms two timer-and-reminder loops keyed by its id:
/// the <see cref="IHotShardMonitorGrain"/> that splits hot shards and the
/// <see cref="IShardHealingOrchestratorGrain"/> that consolidates over-split
/// ones. Deletion used to stop only the tombstone compactor, so both loops kept
/// sweeping a deleted tree and outlived its purge, each sweep resolving the
/// purged id's options - and that resolve registered the id again.
/// </para>
/// <para>
/// A soft delete and a completed purge now stop both loops, and a recovery
/// re-arms them, mirroring the compactor. Stopping is best-effort and never
/// fails the lifecycle verb: a stop that does not land is covered by each loop
/// stopping itself on its next pass once it finds the id purged.
/// </para>
/// </remarks>
internal sealed partial class TreeDeletionGrain
{
    private async Task StopAutonomicLoopsAsync()
    {
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)) return;

        try
        {
            await Task.WhenAll(
                grainFactory.GetGrain<IHotShardMonitorGrain>(TreeId).StopAsync(),
                grainFactory.GetGrain<IShardHealingOrchestratorGrain>(TreeId).StopAsync());
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Tree {TreeId}: failed to stop the hot-shard monitor or the shard-healing orchestrator; each stops itself on its next pass once the tree is purged.",
                TreeId);
        }
    }

    private async Task ArmAutonomicLoopsAsync()
    {
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal)) return;

        try
        {
            await Task.WhenAll(
                grainFactory.GetGrain<IHotShardMonitorGrain>(TreeId).EnsureRunningAsync(),
                grainFactory.GetGrain<IShardHealingOrchestratorGrain>(TreeId).EnsureRunningAsync());
        }
        catch (Exception ex)
        {
            // The recovery itself has succeeded; the tree's next worker
            // activation or write arms the loops again.
            logger.LogWarning(
                ex,
                "Tree {TreeId}: failed to re-arm the hot-shard monitor or the shard-healing orchestrator after recovery; the next write re-arms them.",
                TreeId);
        }
    }
}
