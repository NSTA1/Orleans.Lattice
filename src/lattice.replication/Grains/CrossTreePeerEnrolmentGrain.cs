using System.Collections.Immutable;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>Default <see cref="ICrossTreePeerEnrolmentGrain"/>.</summary>
internal sealed class CrossTreePeerEnrolmentGrain(
    [PersistentState("replication-cross-tree-peers", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreePeerEnrolmentState> state) : Grain, ICrossTreePeerEnrolmentGrain
{
    /// <inheritdoc />
    public Task<ImmutableArray<string>> GetAsync() => Task.FromResult(state.State.Peers.ToImmutableArray());

    /// <inheritdoc />
    public async Task EnrolAsync(string peerClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
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
