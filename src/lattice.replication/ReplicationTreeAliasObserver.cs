using Microsoft.Extensions.Logging;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Replication-side <see cref="ITreeAliasObserver"/> that turns a tree-registry
/// physical-identity swap into an immediate, event-driven rebind of every
/// cross-cluster shipper for the affected logical tree. The core registry fires
/// <see cref="ITreeAliasObserver.OnTreeAliasChangedAsync"/> from its single
/// alias-mutation choke point when a shadow-cutover restore, resize, or reshard
/// repoints a logical tree onto a new physical WAL; this observer fans the new
/// physical id out to the per-peer shipper grains
/// (<c>{logicalTree}/{peer}</c>) so they rebind on their next tick without
/// re-reading the registry.
/// <para>
/// The push is best-effort: a shipper that misses it (transiently unavailable,
/// not yet activated, or predating this build) still heals via its own backstop
/// re-resolve (<see cref="LatticeReplicationOptions.ShipSourceIdentityBackstopInterval"/>),
/// so a per-peer failure here is logged and does not abort the remaining peers.
/// </para>
/// </summary>
internal sealed class ReplicationTreeAliasObserver(
    IGrainFactory grainFactory,
    IReplicationTopology topology,
    ILogger<ReplicationTreeAliasObserver> logger) : ITreeAliasObserver
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly IReplicationTopology _topology = topology ?? throw new ArgumentNullException(nameof(topology));
    private readonly ILogger<ReplicationTreeAliasObserver> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task OnTreeAliasChangedAsync(TreeAliasChange change, CancellationToken cancellationToken)
    {
        var peers = _topology.CurrentPeers;
        if (peers.Count == 0)
        {
            return;
        }

        foreach (var peer in peers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await _grainFactory
                    .GetGrain<IReplicationShipperGrain>($"{change.TreeId}/{peer}")
                    .NotifySourceIdentityChangedAsync(change.NewPhysicalTreeId, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Best-effort push: the peer's shipper will still pick the new
                // identity up via its backstop re-resolve. Log and continue so
                // one unreachable peer does not starve the others of the notify.
                _logger.LogWarning(ex,
                    "Failed to notify shipper {Tree}/{Peer} of source-identity change to '{NewPhysical}'; "
                    + "the shipper will rebind via its backstop re-resolve.",
                    change.TreeId, peer, change.NewPhysicalTreeId);
            }
        }
    }
}
