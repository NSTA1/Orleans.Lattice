using System.IO.Hashing;
using System.Text;
using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateless worker that routes requests to the correct <see cref="IShardRootGrain"/>
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c>.
/// </summary>
[StatelessWorker]
public sealed class LatticeGrain(IGrainFactory grainFactory) : Grain, ILattice
{
    public async Task<byte[]?> GetAsync(string key)
    {
        var shard = GetShardGrain(key);
        return await shard.GetAsync(key);
    }

    public async Task SetAsync(string key, byte[] value)
    {
        var shard = GetShardGrain(key);
        await shard.SetAsync(key, value);
    }

    public async Task<bool> DeleteAsync(string key)
    {
        var shard = GetShardGrain(key);
        return await shard.DeleteAsync(key);
    }

    private IShardRootGrain GetShardGrain(string key)
    {
        var shardIndex = GetShardIndex(key, BPlusTreeOptions.DefaultShardCount);
        var shardKey = $"{this.GetPrimaryKeyString()}/{shardIndex}";
        return grainFactory.GetGrain<IShardRootGrain>(shardKey);
    }

    /// <summary>
    /// Computes a stable shard index for the given key using XxHash32.
    /// </summary>
    internal static int GetShardIndex(string key, int shardCount)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        var hash = XxHash32.HashToUInt32(bytes);
        return (int)(hash % (uint)shardCount);
    }
}
