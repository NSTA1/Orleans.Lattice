using System.Collections.Immutable;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="ICrossTreePeerEnrolmentGrain"/>.</summary>
internal sealed class CrossTreePeerEnrolmentGrain(
    [PersistentState("replication-cross-tree-peers", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreePeerEnrolmentState> state,
    IGrainFactory grainFactory,
    IReplicationTopology topology) : Grain, ICrossTreePeerEnrolmentGrain
{
    /// <inheritdoc />
    public Task<ImmutableArray<string>> GetAsync() => Task.FromResult(state.State.Peers.ToImmutableArray());

    /// <inheritdoc />
    public async Task EnrolAsync(string peerClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);

        // A peer permanently decommissioned (issue #4684/#4723) must not be
        // re-enrolled by a stale or lagging shipper activation: the marker
        // only clears when the operator re-adds the peer to the topology
        // (ReplicationDriverActivationService), so a decommissioned peer
        // that is also not currently configured is refused here, silently
        // (the sole caller, ReplicationShipperGrain, discards the result).
        // A peer that IS configured again is allowed through even if the
        // clear has not yet landed, so a fresh bootstrap is never starved
        // by activation ordering.
        if (!topology.CurrentPeers.Contains(peerClusterId))
        {
            var registry = grainFactory.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(
                IReplicationDecommissionedPeerRegistryGrain.SingletonKey);
            if (await registry.IsDecommissionedAsync(peerClusterId))
            {
                return;
            }
        }

        if (!state.State.Peers.Add(peerClusterId))
        {
            return;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Peers.Remove(peerClusterId);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<bool> DecommissionAsync(string peerClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        if (!state.State.Peers.Remove(peerClusterId))
        {
            return false;
        }

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Peers.Add(peerClusterId);
            throw;
        }

        return true;
    }
}
