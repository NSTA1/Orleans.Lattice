namespace Orleans.Lattice.Views;

internal static class ViewReplicationTopology
{
    internal enum MaintenanceRole
    {
        Maintain,
        Suppress,
        InferFromSource,
    }

    public static MaintenanceRole Resolve(
        string viewName,
        string sourceTreeId,
        LatticeViewOptions options,
        ILatticeReplicationContext replicationContext,
        string? activeViewTreeId = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ArgumentException.ThrowIfNullOrEmpty(sourceTreeId);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(replicationContext);

        if (!replicationContext.IsReplicationEnabled)
        {
            return options.ReplicationMode == LatticeViewReplicationMode.ShipView
                ? MaintenanceRole.InferFromSource
                : MaintenanceRole.Maintain;
        }

        var viewTreeId = activeViewTreeId ?? $"view-{viewName}";
        var viewReplicated = replicationContext.ResolveMergeMode(viewTreeId) is not null;
        if (options.ReplicationMode == LatticeViewReplicationMode.DeriveLocally)
        {
            if (viewReplicated)
            {
                throw new InvalidOperationException(
                    $"View '{viewName}' uses {nameof(LatticeViewReplicationMode)}."
                    + $"{nameof(LatticeViewReplicationMode.DeriveLocally)} but its view tree '{viewTreeId}' is replicated. "
                    + $"{nameof(LatticeViewReplicationMode.DeriveLocally)} runs the maintainer on every cluster, so "
                    + "replicating the view tree as well would create multiple writers.");
            }

            return MaintenanceRole.Maintain;
        }

        if (!viewReplicated)
        {
            throw new InvalidOperationException(
                $"View '{viewName}' uses {nameof(LatticeViewReplicationMode)}."
                + $"{nameof(LatticeViewReplicationMode.ShipView)} but its view tree '{viewTreeId}' is not replicated. "
                + "Consumers would never receive the materialised view.");
        }

        var sourceReplicated = replicationContext.ResolveMergeMode(sourceTreeId) is not null;
        var producerClusterId = options.ShipViewProducerClusterId;
        if (!sourceReplicated)
        {
            if (producerClusterId is not null)
            {
                throw new InvalidOperationException(
                    $"View '{viewName}' sets {nameof(LatticeViewOptions.ShipViewProducerClusterId)} but its source tree "
                    + $"'{sourceTreeId}' is not replicated. Source-less-consumer topology must infer the producer from "
                    + "local source-WAL ownership; remove the explicit producer designation.");
            }

            return MaintenanceRole.InferFromSource;
        }

        if (string.IsNullOrWhiteSpace(producerClusterId))
        {
            throw new InvalidOperationException(
                $"View '{viewName}' replicates both its source tree '{sourceTreeId}' and view tree '{viewTreeId}' under "
                + $"{nameof(LatticeViewReplicationMode)}.{nameof(LatticeViewReplicationMode.ShipView)}. "
                + $"Set {nameof(LatticeViewOptions.ShipViewProducerClusterId)} to exactly one stable "
                + $"{nameof(ILatticeReplicationContext.LocalReplicaId)} so only that cluster maintains the view.");
        }

        return string.Equals(
            producerClusterId,
            replicationContext.LocalReplicaId,
            StringComparison.Ordinal)
            ? MaintenanceRole.Maintain
            : MaintenanceRole.Suppress;
    }

    public static void ThrowIfNonStableShipViewGeneration(
        string viewName,
        LatticeViewOptions options,
        long activeGeneration)
    {
        if (options.ReplicationMode == LatticeViewReplicationMode.ShipView
            && activeGeneration != 0)
        {
            throw new InvalidOperationException(
                $"View '{viewName}' cannot use {nameof(LatticeViewReplicationMode)}."
                + $"{nameof(LatticeViewReplicationMode.ShipView)} after a shadow generation has been activated. "
                + "Replication topology is fixed for a view name; create a new view for the new topology.");
        }
    }

    public static IReadOnlyList<string> SourceCursorTreesToUnregister(
        string? boundPhysicalTreeId,
        string currentPhysicalTreeId) =>
        string.IsNullOrEmpty(boundPhysicalTreeId)
            || string.Equals(boundPhysicalTreeId, currentPhysicalTreeId, StringComparison.Ordinal)
                ? [currentPhysicalTreeId]
                : [boundPhysicalTreeId, currentPhysicalTreeId];
}
