using System.Globalization;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The durable hand-off marker rows (epic #5605, member 2; design note, the row
/// table under "The design"), each under its own reserved grain type in the
/// <c>_lattice_</c> namespace and written straight through the tree's grain storage
/// provider, as <see cref="WalAppendFenceRows"/> writes the fence rows. No existing
/// state type gains a field.
/// </summary>
/// <remarks>
/// Every update is a read, a pure mutation and a write conditional on the ETag read,
/// re-read on a conflict, so concurrent writers of one marker (a split's donor and
/// its sibling's birth, say) never lose each other's tokens.
/// </remarks>
internal static class HandOffMarkerRows
{
    /// <summary>Reserved grain type and state name of a leaf's hand-off marker.</summary>
    internal const string LeafGrainType = "_lattice_leaf-handoff";

    /// <summary>Reserved grain type and state name of a shard root's hand-off marker.</summary>
    internal const string ShardGrainType = "_lattice_shard-handoff";

    /// <summary>How many times an update re-reads after losing an ETag race.</summary>
    internal const int MaxUpdateAttempts = 16;

    /// <summary>The key of a leaf's marker row.</summary>
    [GrainKeyBuilder]
    internal static string LeafKey(Guid leafId) => leafId.ToString("N", CultureInfo.InvariantCulture);

    /// <summary>The key of a shard root's marker row.</summary>
    [GrainKeyBuilder]
    internal static string ShardKey(string treeId, int shardIndex) =>
        string.Concat(treeId, "~", shardIndex.ToString(CultureInfo.InvariantCulture));

    /// <summary>Reads a leaf's marker; an absent row reads as uninitialised, with nothing open.</summary>
    internal static Task<GrainState<LeafHandOffState>> ReadLeafAsync(IGrainStorage storage, Guid leafId) =>
        ReadAsync<LeafHandOffState>(storage, LeafGrainType, LeafKey(leafId));

    /// <summary>Reads a shard root's marker; an absent row reads as uninitialised, with nothing open.</summary>
    internal static Task<GrainState<ShardHandOffState>> ReadShardAsync(IGrainStorage storage, string treeId, int shardIndex) =>
        ReadAsync<ShardHandOffState>(storage, ShardGrainType, ShardKey(treeId, shardIndex));

    /// <summary>
    /// Applies <paramref name="mutate"/> to a leaf's marker and writes it when it
    /// reports a change, conditionally on the ETag read; returns the marker as stored.
    /// </summary>
    internal static Task<LeafHandOffState> UpdateLeafAsync(IGrainStorage storage, Guid leafId, Func<LeafHandOffState, bool> mutate) =>
        UpdateAsync(storage, LeafGrainType, LeafKey(leafId), mutate);

    /// <summary>
    /// Applies <paramref name="mutate"/> to a shard root's marker and writes it when it
    /// reports a change, conditionally on the ETag read; returns the marker as stored.
    /// </summary>
    internal static Task<ShardHandOffState> UpdateShardAsync(IGrainStorage storage, string treeId, int shardIndex, Func<ShardHandOffState, bool> mutate) =>
        UpdateAsync(storage, ShardGrainType, ShardKey(treeId, shardIndex), mutate);

    /// <summary>
    /// As <see cref="UpdateShardAsync"/>, but never creates the row: returns
    /// <see langword="null"/> when the shard root's marker row is absent (#5705).
    /// </summary>
    internal static async Task<ShardHandOffState?> UpdateExistingShardAsync(
        IGrainStorage storage, string treeId, int shardIndex, Func<ShardHandOffState, bool> mutate)
    {
        ArgumentNullException.ThrowIfNull(mutate);
        var key = ShardKey(treeId, shardIndex);
        for (var attempt = 1; ; attempt++)
        {
            var row = await ReadAsync<ShardHandOffState>(storage, ShardGrainType, key).ConfigureAwait(true);
            if (!row.RecordExists)
            {
                return null;
            }

            if (!mutate(row.State))
            {
                return row.State;
            }

            try
            {
                await storage.WriteStateAsync(ShardGrainType, RowId(ShardGrainType, key), row).ConfigureAwait(true);
                return row.State;
            }
            catch (InconsistentStateException) when (attempt < MaxUpdateAttempts)
            {
                // Another writer updated (or deleted) the marker since the read; re-read.
            }
        }
    }

    /// <summary>Deletes a leaf's marker row, if any; called when the leaf is cleared.</summary>
    internal static async Task ClearLeafAsync(IGrainStorage storage, Guid leafId)
    {
        for (var attempt = 1; ; attempt++)
        {
            var row = await ReadLeafAsync(storage, leafId).ConfigureAwait(true);
            if (!row.RecordExists)
            {
                return;
            }

            try
            {
                await storage.ClearStateAsync(LeafGrainType, RowId(LeafGrainType, LeafKey(leafId)), row).ConfigureAwait(true);
                return;
            }
            catch (InconsistentStateException) when (attempt < MaxUpdateAttempts)
            {
            }
        }
    }

    private static GrainId RowId(string grainType, string key) =>
        GrainId.Create(GrainType.Create(grainType), IdSpan.Create(key));

    private static async Task<T> UpdateAsync<T>(IGrainStorage storage, string grainType, string key, Func<T, bool> mutate)
        where T : new()
    {
        ArgumentNullException.ThrowIfNull(mutate);
        for (var attempt = 1; ; attempt++)
        {
            var row = await ReadAsync<T>(storage, grainType, key).ConfigureAwait(true);
            if (!mutate(row.State))
            {
                return row.State;
            }

            try
            {
                await storage.WriteStateAsync(grainType, RowId(grainType, key), row).ConfigureAwait(true);
                return row.State;
            }
            catch (InconsistentStateException) when (attempt < MaxUpdateAttempts)
            {
                // Another writer updated the marker since the read; re-apply to what it wrote.
            }
        }
    }

    private static async Task<GrainState<T>> ReadAsync<T>(IGrainStorage storage, string grainType, string key)
        where T : new()
    {
        ArgumentNullException.ThrowIfNull(storage);
        var row = new GrainState<T>(new T());
        await storage.ReadStateAsync(grainType, RowId(grainType, key), row).ConfigureAwait(true);
        row.State ??= new T();
        return row;
    }
}
