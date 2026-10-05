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
    /// The admission check every replicated apply entry runs (issue #4707):
    /// checks the stamp of the active <see cref="ReplicationSourceLineageScope"/>
    /// against the lineage <paramref name="treeName"/> last drained from the
    /// stamping source, through <see cref="CheckAsync"/>. An unstamped apply,
    /// and the bootstrap drain's own rows, are admitted without a grain call.
    /// The frontier epoch is the one the transport observed for the delivery,
    /// or read from the tree frontier when it supplied none. The verdict is
    /// cached on the scope, so a delivery split into several runs pays for one
    /// check.
    /// </summary>
    public static async ValueTask<Verdict> AdmitAsync(
        IGrainFactory grainFactory,
        string treeName,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (LatticeBootstrapApplyContext.IsActive
            || ReplicationSourceLineageScope.Active is not { Stamp: { } stamp } scope)
        {
            return Verdict.Apply;
        }

        if (scope.TryGetVerdict(treeName, out var cached))
        {
            return cached;
        }

        var epoch = scope.ObservedFrontierEpoch;
        if (epoch is null)
        {
            try
            {
                epoch = (await grainFactory.GetGrain<IReplicationTreeFrontierGrain>(treeName)
                    .GetAsync(cancellationToken)
                    .ConfigureAwait(false)).Epoch;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Without an epoch the check below refuses transiently, which
                // never discards the entry.
                logger.LogWarning(ex,
                    "Tree '{TreeName}': could not read the tree frontier epoch to check an entry stamped by '{Source}'.",
                    treeName, stamp.SourceClusterId);
            }
        }

        var verdict = await CheckAsync(grainFactory, treeName, stamp.SourceClusterId, stamp.Lineage, epoch, logger)
            .ConfigureAwait(false);
        scope.Record(treeName, verdict);
        return verdict;
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
