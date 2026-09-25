using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable per-tree saga decision registry shard high-water mark. See
/// <see cref="ITxRegistryHighWaterGrain"/> for the contract. Non-reentrant, so a
/// raise and every read are totally ordered: a reader that observes a mark below
/// a shard's index observed it before that shard's raise completed, and hence
/// before that shard wrote anything.
/// </summary>
internal sealed class TxRegistryHighWaterGrain(
    IGrainContext context,
    [PersistentState("tx-registry-high-water", LatticeOptions.StorageProviderName)]
    IPersistentState<TxRegistryHighWaterState> state) : ITxRegistryHighWaterGrain, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public Task<int> GetShardHighWaterAsync() => Task.FromResult(Current);

    /// <inheritdoc />
    public async Task<int> RaiseShardHighWaterAsync(int shardCount)
    {
        var target = Math.Clamp(shardCount, 1, LatticeOptions.MaxTxRegistryShardCount);
        var previous = state.State.ShardHighWater;
        if (target <= previous)
        {
            return Current;
        }

        state.State.ShardHighWater = target;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.ShardHighWater = previous;
            throw;
        }

        return target;
    }

    private int Current => Math.Max(state.State.ShardHighWater, 0);
}
