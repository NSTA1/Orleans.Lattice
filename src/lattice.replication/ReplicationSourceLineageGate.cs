using Microsoft.Extensions.Logging;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The receiver's check that a pushed batch was read under the source lineage
/// this tree last drained from that source (issue #4673). After a source
/// restore, purge and recreate, or alias move re-stamps the source tree's
/// lineage, a batch the source read under the old lineage can still arrive - a
/// push already in flight, or a retry. Applied to a copy drained from the new
/// lineage, it plants a source-origin row the new lineage lacks, and a later
/// delete reconcile would fabricate its delete. The check refuses it.
/// </summary>
internal static class ReplicationSourceLineageGate
{
    /// <summary>The outcome of <see cref="CheckAsync"/>.</summary>
    internal enum Verdict
    {
        /// <summary>Apply the batch.</summary>
        Apply,

        /// <summary>
        /// Refuse the batch and tell the sender it was read under a lineage this
        /// tree does not hold: the sender re-resolves its binding, and re-seeds
        /// the peer when it is current.
        /// </summary>
        RefuseLineage,

        /// <summary>Refuse the batch for now; the sender retries it.</summary>
        RefuseTransient,
    }

    /// <summary>
    /// Decides whether a batch <paramref name="originClusterId"/> stamped with
    /// <paramref name="stampedLineage"/> may apply to <paramref name="treeName"/>.
    /// <list type="bullet">
    ///   <item>An unstamped batch (a sender that predates the stamp), or one from
    ///   a source this tree never drained, applies as before.</item>
    ///   <item>Once the tree frontier's epoch has moved past the drain - the
    ///   tree's contents were replaced since - the drain no longer describes
    ///   the tree, so every stamped batch is refused until the source is
    ///   drained again.</item>
    ///   <item>Otherwise a batch stamped with any lineage but the drained one is
    ///   refused, <see cref="Guid.Empty"/> (the sender cannot tell) included.</item>
    /// </list>
    /// A failure to read the drained lineage or the frontier epoch refuses
    /// transiently, so it never makes the sender re-seed the peer.
    /// </summary>
    public static async Task<Verdict> CheckAsync(
        IGrainFactory grainFactory,
        string treeName,
        string originClusterId,
        Guid? stampedLineage,
        Guid? currentFrontierEpoch,
        ILogger logger)
    {
        if (stampedLineage is not { } stamped)
        {
            return Verdict.Apply;
        }

        ReplicationDrainedLineage? drained;
        try
        {
            drained = await grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(treeName)
                .GetDrainedLineageAsync(originClusterId)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Tree '{TreeName}': could not read the source lineage last drained from '{Source}'; its batch is refused and "
                + "re-sent.",
                treeName, originClusterId);
            return Verdict.RefuseTransient;
        }

        if (drained is not { } record)
        {
            return Verdict.Apply;
        }

        if (currentFrontierEpoch is not { } epoch)
        {
            return Verdict.RefuseTransient;
        }

        string? reason = null;
        if (epoch != record.FrontierEpoch)
        {
            reason = LatticeReplicationMetrics.SourceLineageRefusedReplaced;
        }
        else if (stamped != record.Lineage)
        {
            reason = LatticeReplicationMetrics.SourceLineageRefusedStale;
        }

        if (reason is null)
        {
            return Verdict.Apply;
        }

        LatticeReplicationMetrics.ApplySourceLineageRefused.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, originClusterId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagReason, reason),
            LatticeTenantLabel.ForTree(treeName));
        return Verdict.RefuseLineage;
    }
}
