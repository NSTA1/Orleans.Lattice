using Microsoft.Extensions.Logging;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication.Grains;

internal sealed partial class LatticeBootstrapCoordinatorGrain
{
    /// <summary>
    /// The merge mode a drain applies under, once the receiver's enrolment gate is
    /// known to admit it (#5731). The applier drops every row of a tree its gate
    /// does not admit and reports the row as not applied, so a drain into a gate
    /// that refuses the tree would complete having imported nothing. When the
    /// gate refuses, this silo's enrolment view is refreshed once (one config
    /// read), because an enable committed elsewhere may not have been compiled
    /// here yet; if the gate still refuses, the attempt fails and the bootstrap
    /// is not completed. No call when the gate admits the tree.
    /// </summary>
    private async Task<LatticeMergeMode> ResolveAdmittedDrainModeAsync(string treeName, CancellationToken cancellationToken)
    {
        var mergeMode = _mergeModeResolver.Resolve(treeName) ?? LatticeMergeMode.LwwRegister;
        if (IsDrainAdmitted(treeName, mergeMode))
        {
            return mergeMode;
        }

        if (Context.ActivationServices?.GetService(typeof(IRefreshableReplicatedTreeMembership))
            is IRefreshableReplicatedTreeMembership refreshable)
        {
            await refreshable.RefreshAsync(cancellationToken).ConfigureAwait(true);
            mergeMode = _mergeModeResolver.Resolve(treeName) ?? LatticeMergeMode.LwwRegister;
            if (IsDrainAdmitted(treeName, mergeMode))
            {
                return mergeMode;
            }
        }

        throw RefusedByEnrolmentGate(treeName, "before the drain");
    }

    /// <summary>
    /// The detector at the end of a drain: a gate that stopped admitting the tree
    /// while the drain ran (a concurrent disable) dropped rows the drain counted
    /// as delivered, so the attempt fails rather than completing (#5731).
    /// </summary>
    private void ThrowIfDrainNoLongerAdmitted(string treeName, LatticeMergeMode mergeMode)
    {
        if (!IsDrainAdmitted(treeName, mergeMode))
        {
            throw RefusedByEnrolmentGate(treeName, "while the drain ran");
        }
    }

    /// <summary>
    /// The applier's own admission rule for a row of this drain. A host with no
    /// enrollment source at all cannot be judged here and is left to the applier.
    /// </summary>
    private bool IsDrainAdmitted(string treeName, LatticeMergeMode mergeMode)
    {
        var context = Context.ActivationServices?.GetService(typeof(ILatticeReplicationContext)) as ILatticeReplicationContext;
        var local = ReplicationInboundAdmission.ResolveLocalMergeMode(context, _optionsMonitor, treeName, out var hasEnrollmentSource);
        return !hasEnrollmentSource || local == mergeMode;
    }

    private InvalidOperationException RefusedByEnrolmentGate(string treeName, string when)
    {
        Logger.LogWarning(
            "Bootstrap of tree '{TreeName}' from source '{SourceClusterId}' fails: the tree is not enrolled for replication on this receiver {When}, so the receiver would drop every snapshot row and the import would complete empty",
            treeName, state.State.SourceClusterId, when);
        return new InvalidOperationException(
            $"Bootstrap of tree '{treeName}' cannot import: the tree is not enrolled for replication on this receiver {when}.");
    }
}
