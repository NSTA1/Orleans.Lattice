using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="IReplicationDecommissionedPeerRegistryGrain"/>.</summary>
internal sealed class ReplicationDecommissionedPeerRegistryGrain(
    [PersistentState("replication-decommissioned-peers", LatticeOptions.StorageProviderName)]
    IPersistentState<ReplicationDecommissionedPeerRegistryState> state)
    : Grain, IReplicationDecommissionedPeerRegistryGrain
{
    /// <inheritdoc />
    public Task<bool> IsDecommissionedAsync(string peerClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        return Task.FromResult(state.State.DecommissionedAtUtc.ContainsKey(peerClusterId));
    }

    /// <inheritdoc />
    public async Task MarkDecommissionedAsync(string peerClusterId, DateTimeOffset decommissionedAtUtc)
    {
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        if (state.State.DecommissionedAtUtc.ContainsKey(peerClusterId))
        {
            return;
        }

        state.State.DecommissionedAtUtc[peerClusterId] = decommissionedAtUtc;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.DecommissionedAtUtc.Remove(peerClusterId);
            throw;
        }
    }
}
