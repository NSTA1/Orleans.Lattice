using System.Globalization;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The durable rows of the WAL append fence (epic #5605), each under its own
/// reserved grain type in the <c>_lattice_</c> namespace and written straight
/// through the tree's grain storage provider, as
/// <see cref="GrainStorageFencingProbe"/> writes its probe row. No existing state
/// type gains a field, so a build that predates the fence rewrites its own rows
/// without dropping anything the fence relies on, and never reads these.
/// </summary>
/// <remarks>
/// Every key is composed by a <see cref="GrainKeyBuilderAttribute"/> method with
/// <c>~</c> separators, never <c>/</c>, so it is safe as an Azure Table
/// partition and row key. A composed key is unambiguous because its suffix has a
/// fixed shape (a decimal partition, then a 32-digit leaf id or a one-letter
/// initiator tag and its index), which is parsed from the end.
/// </remarks>
internal static class WalAppendFenceRows
{
    /// <summary>Reserved grain type and state name of a leaf's epoch row.</summary>
    internal const string LeafEpochGrainType = "_lattice_leaf-append-epoch";

    /// <summary>Reserved grain type and state name of a partition's epoch entry for one leaf.</summary>
    internal const string EntryGrainType = "_lattice_wal-append-epoch";

    /// <summary>Reserved grain type and state name of a partition's retired-epoch floor.</summary>
    internal const string FloorGrainType = "_lattice_wal-append-epoch-floor";

    /// <summary>Reserved grain type and state name of a partition's routing-generation entry for one initiator.</summary>
    internal const string GenerationGrainType = "_lattice_wal-routing-generation";

    /// <summary>The key of a leaf's epoch row.</summary>
    [GrainKeyBuilder]
    internal static string LeafEpochKey(Guid leafId) => leafId.ToString("N", CultureInfo.InvariantCulture);

    /// <summary>The key of a WAL partition's retired-epoch floor row.</summary>
    [GrainKeyBuilder]
    internal static string FloorKey(string treeId, int partition) =>
        string.Concat(treeId, "~", partition.ToString(CultureInfo.InvariantCulture));

    /// <summary>The key of a WAL partition's epoch entry row for one leaf.</summary>
    [GrainKeyBuilder]
    internal static string EntryKey(string treeId, int partition, Guid leafId) =>
        string.Concat(FloorKey(treeId, partition), "~", leafId.ToString("N", CultureInfo.InvariantCulture));

    /// <summary>The key of a WAL partition's routing-generation row for one initiator.</summary>
    [GrainKeyBuilder]
    internal static string GenerationKey(string treeId, int partition, WalRoutingInitiatorKind kind, int index)
    {
        var tag = kind switch
        {
            WalRoutingInitiatorKind.ShardRoot => "s",
            WalRoutingInitiatorKind.TreeRangeDelete => "t",
            _ => "n",
        };
        return string.Concat(FloorKey(treeId, partition), "~", tag, index.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>The grain id a row of <paramref name="grainType"/> under <paramref name="key"/> is stored at.</summary>
    internal static GrainId RowId(string grainType, string key) =>
        GrainId.Create(GrainType.Create(grainType), IdSpan.Create(key));

    /// <summary>Reads a leaf's epoch row; an absent row reads as epoch 0 with no ETag.</summary>
    internal static Task<GrainState<LeafAppendEpochState>> ReadLeafEpochAsync(IGrainStorage storage, Guid leafId) =>
        ReadAsync<LeafAppendEpochState>(storage, LeafEpochGrainType, LeafEpochKey(leafId));

    /// <summary>Reads a partition's epoch entry row for one leaf.</summary>
    internal static Task<GrainState<WalAppendEpochEntryState>> ReadEntryAsync(IGrainStorage storage, string treeId, int partition, Guid leafId) =>
        ReadAsync<WalAppendEpochEntryState>(storage, EntryGrainType, EntryKey(treeId, partition, leafId));

    /// <summary>Reads a partition's retired-epoch floor row.</summary>
    internal static Task<GrainState<WalAppendEpochFloorState>> ReadFloorAsync(IGrainStorage storage, string treeId, int partition) =>
        ReadAsync<WalAppendEpochFloorState>(storage, FloorGrainType, FloorKey(treeId, partition));

    /// <summary>Reads a partition's routing-generation row for one initiator.</summary>
    internal static Task<GrainState<WalRoutingGenerationState>> ReadGenerationAsync(
        IGrainStorage storage, string treeId, int partition, WalRoutingInitiator initiator) =>
        ReadAsync<WalRoutingGenerationState>(
            storage, GenerationGrainType, GenerationKey(treeId, partition, initiator.Kind, initiator.Index));

    /// <summary>
    /// Writes <paramref name="row"/> conditionally on the ETag it carries: a row
    /// read absent is created only if it is still absent. A conflict throws
    /// <see cref="InconsistentStateException"/>, and <paramref name="row"/> carries
    /// the new ETag on success.
    /// </summary>
    internal static Task WriteAsync<T>(IGrainStorage storage, string grainType, string key, GrainState<T> row) =>
        storage.WriteStateAsync(grainType, RowId(grainType, key), row);

    /// <summary>Deletes <paramref name="row"/> conditionally on the ETag it carries.</summary>
    internal static Task ClearAsync<T>(IGrainStorage storage, string grainType, string key, GrainState<T> row) =>
        storage.ClearStateAsync(grainType, RowId(grainType, key), row);

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
