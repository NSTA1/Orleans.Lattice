using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Retires one leaf's WAL append fence rows across a tree's partitions (epic #5605):
/// the sequence a leaf's clear runs, and the one <see cref="WalAppendFenceSweeper"/>
/// re-runs for a leaf whose clear left its rows behind (#5634).
/// </summary>
/// <remarks>
/// The sequence: read the leaf's epoch row; retire the leaf's entry at every
/// partition, flooring each at the higher of its entry and the row value read
/// (#5641); then delete the row conditionally on the ETag read. A row that moved
/// since the read is kept: either a re-created incarnation claimed it, or the
/// sequence is not atomic and a pre-retirement duplicate, refused by a floor just
/// raised, bumped it in between. Both are safe: the row is kept, so epochs do not
/// recur, and the next claimant goes above the bumped value. Retiring a leaf that is
/// in fact still live is safe for the same reason; it only costs that activation a
/// deactivation and a re-claim.
/// </remarks>
internal static class WalAppendFenceRetirement
{
    /// <summary>
    /// Retires <paramref name="leafId"/>'s fence rows. A leaf with no epoch row is
    /// retired only at <paramref name="partitionsWithoutRow"/>, the partitions whose
    /// directory still lists it; for a leaf clear that is none, so a leaf that never
    /// claimed costs one storage read.
    /// </summary>
    /// <param name="grainFactory">The grain factory, for the tree's WAL shards.</param>
    /// <param name="storage">The tree's grain storage provider.</param>
    /// <param name="treeId">The tree whose WAL partitions the leaf may have entries at.</param>
    /// <param name="partitions">Resolves the tree's WAL partition count; called only for a leaf with an epoch row.</param>
    /// <param name="leafId">The leaf's grain key.</param>
    /// <param name="partitionsWithoutRow">Where to retire the leaf when it has no epoch row.</param>
    /// <returns><see langword="true"/> when the leaf had an epoch row.</returns>
    internal static async Task<bool> RetireLeafAsync(
        IGrainFactory grainFactory,
        IGrainStorage storage,
        string treeId,
        Func<ValueTask<int>> partitions,
        Guid leafId,
        IReadOnlyCollection<int> partitionsWithoutRow)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(partitions);
        ArgumentNullException.ThrowIfNull(partitionsWithoutRow);

        var row = await WalAppendFenceRows.ReadLeafEpochAsync(storage, leafId).ConfigureAwait(true);
        if (!row.RecordExists)
        {
            if (partitionsWithoutRow.Count > 0)
            {
                var stale = new List<Task>(partitionsWithoutRow.Count);
                foreach (var partition in partitionsWithoutRow)
                {
                    stale.Add(grainFactory.GetGrain<IWalShardGrain>($"{treeId}/{partition}")
                        .RetireAppendEpochAsync(leafId, 0, CancellationToken.None));
                }

                await Task.WhenAll(stale).ConfigureAwait(true);
            }

            return false;
        }

        var count = await partitions().ConfigureAwait(true);
        var retirements = new Task[count];
        for (var partition = 0; partition < count; partition++)
        {
            retirements[partition] = grainFactory
                .GetGrain<IWalShardGrain>($"{treeId}/{partition}")
                .RetireAppendEpochAsync(leafId, row.State.Epoch, CancellationToken.None);
        }

        // The epoch row goes only once every partition has retired its entry.
        await Task.WhenAll(retirements).ConfigureAwait(true);
        try
        {
            await WalAppendFenceRows.ClearAsync(
                storage,
                WalAppendFenceRows.LeafEpochGrainType,
                WalAppendFenceRows.LeafEpochKey(leafId),
                row).ConfigureAwait(true);
        }
        catch (InconsistentStateException)
        {
            // The row moved since the read, so it is kept (see the remarks).
        }

        return true;
    }
}
