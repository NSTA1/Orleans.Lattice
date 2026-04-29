using System.IO.Hashing;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Shard-side projection-digest implementation. Walks the leaf
/// chain (using the same leftmost-leaf + <see cref="IBPlusLeafGrain.GetNextSiblingAsync"/>
/// traversal already used by <see cref="ITombstoneCompactionGrain"/>) and
/// chains each leaf's digest through XxHash128 so cross-silo divergence at
/// any leaf surfaces in the shard total.
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <inheritdoc />
    public async Task<LeafProjectionDigest> GetShardProjectionDigestAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await PrepareForOperationAsync();
        cancellationToken.ThrowIfCancellationRequested();

        var hasher = new XxHash128();

        long totalEntries = 0;
        long totalCheckpointOffset = 0;

        var leafId = await GetLeftmostLeafIdAsync();
        while (leafId is not null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var leafDigest = await leaf.GetProjectionDigestAsync();

            // Chain XxHash128: feed every leaf's hash bytes into the running hasher.
            // The shard hash is therefore XxHash128(leaf_1.Hash || leaf_2.Hash || ...).
            hasher.Append(leafDigest.Hash);

            totalEntries += leafDigest.EntryCount;
            totalCheckpointOffset += leafDigest.CheckpointOffset;

            leafId = await leaf.GetNextSiblingAsync();
        }

        return new LeafProjectionDigest
        {
            Hash = hasher.GetHashAndReset(),
            EntryCount = totalEntries,
            CheckpointOffset = totalCheckpointOffset,
        };
    }
}
