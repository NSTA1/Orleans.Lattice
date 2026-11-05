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
    /// As <see cref="UpdateLeafAsync"/>, but returns the row as written - with the ETag the
    /// write left on it - or <see langword="null"/> when <paramref name="mutate"/> changed
    /// nothing, so no write was made (#5837, E1).
    /// </summary>
    internal static async Task<GrainState<LeafHandOffState>?> UpdateLeafRowAsync(IGrainStorage storage, Guid leafId, Func<LeafHandOffState, bool> mutate)
    {
        ArgumentNullException.ThrowIfNull(mutate);
        var key = LeafKey(leafId);
        for (var attempt = 1; ; attempt++)
        {
            var row = await ReadAsync<LeafHandOffState>(storage, LeafGrainType, key).ConfigureAwait(true);
            if (!mutate(row.State))
            {
                return null;
            }

            try
            {
                await storage.WriteStateAsync(LeafGrainType, RowId(LeafGrainType, key), row).ConfigureAwait(true);
                return row;
            }
            catch (InconsistentStateException) when (attempt < MaxUpdateAttempts)
            {
                // Another writer updated (or deleted) the marker since the read; re-read.
            }
        }
    }

    /// <summary>
    /// Applies <paramref name="mutate"/> to a copy of <paramref name="written"/> - a row
    /// this activation itself wrote - and writes it conditionally on that row's ETag, with
    /// no read (#5837, E1-E3). Any ETag miss, a write by anyone else in between included,
    /// takes the full path instead: <see cref="UpdateLeafAsync"/> re-reads the row and
    /// re-applies <paramref name="mutate"/>, which decides afresh whether there is anything
    /// left to change. Returns the marker as stored.
    /// </summary>
    internal static async Task<LeafHandOffState> UpdateLeafFromAsync(
        IGrainStorage storage, Guid leafId, GrainState<LeafHandOffState> written, Func<LeafHandOffState, bool> mutate)
    {
        ArgumentNullException.ThrowIfNull(written);
        ArgumentNullException.ThrowIfNull(mutate);
        var copy = new GrainState<LeafHandOffState>(Copy(written.State)) { ETag = written.ETag, RecordExists = written.RecordExists };
        if (!mutate(copy.State))
        {
            return copy.State;
        }

        try
        {
            await storage.WriteStateAsync(LeafGrainType, RowId(LeafGrainType, LeafKey(leafId)), copy).ConfigureAwait(true);
            return copy.State;
        }
        catch (InconsistentStateException)
        {
            return await UpdateLeafAsync(storage, leafId, mutate).ConfigureAwait(true);
        }
    }

    /// <summary>A deep copy of a leaf marker, so a mutation of it never touches the row it came from (#5837, E3).</summary>
    internal static LeafHandOffState Copy(LeafHandOffState marker)
    {
        ArgumentNullException.ThrowIfNull(marker);
        var copy = new LeafHandOffState
        {
            Initialised = marker.Initialised,
            CompactedHeads = (long[])marker.CompactedHeads.Clone(),
            // RED-PROOF M20: Sequence = marker.Sequence,
            SiblingLinkedGeneration = marker.SiblingLinkedGeneration,
            SiblingLinkedShard = marker.SiblingLinkedShard,
        };
        foreach (var (token, range) in marker.Open)
        {
            copy.Open[token] = new HandOffMarkerRange { Token = range.Token, LowInclusive = range.LowInclusive, HighExclusive = range.HighExclusive };
        }

        foreach (var close in marker.Closes)
        {
            copy.Closes.Add(new HandOffClose
            {
                Token = close.Token,
                LowInclusive = close.LowInclusive,
                HighExclusive = close.HighExclusive,
                Heads = (long[])close.Heads.Clone(),
                Sequence = close.Sequence,
                DeferredShard = close.DeferredShard,
                DeferredGeneration = close.DeferredGeneration,
                DonorLeaf = close.DonorLeaf,
                DonorToken = close.DonorToken,
                AdmittedShard = close.AdmittedShard,
                AdmittedGeneration = close.AdmittedGeneration,
                LinkedGeneration = close.LinkedGeneration,
                CarryResolvedAt = (bool[])close.CarryResolvedAt.Clone(),
                LinkedShard = close.LinkedShard,
            });
        }

        return copy;
    }

    /// <summary>
    /// Applies <paramref name="mutate"/> to a shard root's marker and writes it when it
    /// reports a change, conditionally on the ETag read; returns the marker as stored.
    /// </summary>
    internal static Task<ShardHandOffState> UpdateShardAsync(IGrainStorage storage, string treeId, int shardIndex, Func<ShardHandOffState, bool> mutate) =>
        UpdateAsync(storage, ShardGrainType, ShardKey(treeId, shardIndex), mutate);

    /// <summary>
    /// Writes a shard root's marker conditionally on the ETag <paramref name="row"/>
    /// carries - a create when it carries none - and leaves the new ETag on it. A
    /// conflict, a deleted row included, throws <see cref="InconsistentStateException"/>.
    /// </summary>
    internal static Task WriteShardAsync(IGrainStorage storage, string treeId, int shardIndex, GrainState<ShardHandOffState> row) =>
        storage.WriteStateAsync(ShardGrainType, RowId(ShardGrainType, ShardKey(treeId, shardIndex)), row);

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
