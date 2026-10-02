using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The WAL reclamation read (issue #4195): <see cref="ILatticeWalReclamation"/>
/// over the core per-tree floor-holder probe.
/// </summary>
internal sealed partial class LatticeTreeAdmin : ILatticeWalReclamation
{
    /// <inheritdoc />
    public async Task<TreeWalReclamationReport> GetWalReclamationAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        var effectiveTreeId = await EffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeTreeReadAsync(effectiveTreeId, cancellationToken).ConfigureAwait(false);

        // The pins are published under the physical tree a leaf belongs to, so an
        // aliased (resized or restored) tree is probed at its physical target.
        var physicalTreeId = await _grainFactory.GetLatticeRegistry()
            .ResolveAsync(effectiveTreeId)
            .ConfigureAwait(false);

        var probe = await _grainFactory.GetGrain<ILatticeWalFloorHolderProbe>(physicalTreeId)
            .ProbeAsync(cancellationToken)
            .ConfigureAwait(false);

        return ToWalReclamationReport(probe, treeId);
    }

    /// <summary>
    /// Maps the core probe report onto the facade report, echoing the caller's own
    /// tree name so neither the tenant composition nor the physical alias target
    /// leaks onto the wire.
    /// </summary>
    /// <param name="probe">The core probe report.</param>
    /// <param name="reportedTreeId">The tree id as the caller named it.</param>
    /// <returns>The facade report.</returns>
    internal static TreeWalReclamationReport ToWalReclamationReport(WalFloorHolderProbeReport probe, string reportedTreeId) =>
        new()
        {
            TreeId = reportedTreeId,
            PinStoreReadable = probe.PinStoreReadable,
            PinCount = probe.PinCount,
            PinsWithoutOffset = probe.PinsWithoutOffset,
            FloorHolder = probe.ConsumerId is { } consumerId
                ? new TreeWalFloorHolder
                {
                    ConsumerId = consumerId,
                    LeafId = probe.LeafId,
                    Partition = probe.Partition,
                    PinOffset = probe.PinOffset,
                    PersistedCheckpoint = probe.PersistedCheckpoint,
                    State = ToFloorHolderState(probe.State),
                }
                : null,
        };

    private static TreeWalFloorHolderState ToFloorHolderState(WalGcBlockingPinState state) => state switch
    {
        WalGcBlockingPinState.CheckpointedUncovered => TreeWalFloorHolderState.CheckpointedUncovered,
        WalGcBlockingPinState.NeverCheckpointed => TreeWalFloorHolderState.NeverCheckpointed,
        WalGcBlockingPinState.NoDurableState => TreeWalFloorHolderState.NoDurableState,
        WalGcBlockingPinState.Orphaned => TreeWalFloorHolderState.Orphaned,
        WalGcBlockingPinState.CheckpointedCoverageUnknown => TreeWalFloorHolderState.CheckpointedCoverageUnknown,
        _ => TreeWalFloorHolderState.Unreadable,
    };
}
