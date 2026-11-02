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
/// fixed shape (a decimal partition, then a 32-digit leaf id, a one-letter
/// initiator tag and its index, or a directory bucket and page), which is parsed
/// from the end.
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

    /// <summary>Reserved grain type and state name of a page of a partition's append-epoch directory (#5634).</summary>
    internal const string DirectoryPageGrainType = "_lattice_wal-append-epoch-dir";

    /// <summary>Reserved grain type and state name of a tree's append-fence sweep claim (#5634).</summary>
    internal const string SweepGrainType = "_lattice_wal-append-fence-sweep";

    /// <summary>Reserved grain type and state name of a tree id's routing-generation floor (#5668).</summary>
    internal const string RoutingFloorGrainType = "_lattice_tree-routing-floor";

    /// <summary>
    /// The <c>rowEpoch</c> a retirement passes when it has not read the leaf's epoch
    /// row: the partition reads it, and does nothing when the leaf has none.
    /// </summary>
    internal const long RowEpochUnread = -1;

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
    internal static string GenerationKey(string treeId, int partition, WalRoutingInitiatorKind kind, int index) =>
        string.Concat(FloorKey(treeId, partition), "~", InitiatorTag(kind, index));

    /// <summary>
    /// An initiator's tag: a one-letter kind and its index. It is the suffix of the
    /// initiator's generation rows, and its key in the tree id's routing floors.
    /// </summary>
    internal static string InitiatorTag(WalRoutingInitiatorKind kind, int index)
    {
        var tag = kind switch
        {
            WalRoutingInitiatorKind.ShardRoot => "s",
            WalRoutingInitiatorKind.TreeRangeDelete => "t",
            _ => "n",
        };
        return string.Concat(tag, index.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>An initiator's tag; see <see cref="InitiatorTag(WalRoutingInitiatorKind, int)"/>.</summary>
    internal static string InitiatorTag(WalRoutingInitiator initiator) => InitiatorTag(initiator.Kind, initiator.Index);

    /// <summary>The routing floor <paramref name="floors"/> holds for <paramref name="initiator"/>; 0 when it names none.</summary>
    internal static long FloorOf(TreeRoutingFloorState floors, WalRoutingInitiator initiator) =>
        floors.Floors.TryGetValue(InitiatorTag(initiator), out var floor) ? floor : 0;

    /// <summary>The key of one page of a WAL partition's append-epoch directory bucket.</summary>
    [GrainKeyBuilder]
    internal static string DirectoryPageKey(string treeId, int partition, int bucket, int page) =>
        string.Concat(
            FloorKey(treeId, partition),
            "~d",
            bucket.ToString(CultureInfo.InvariantCulture),
            "~",
            page.ToString(CultureInfo.InvariantCulture));

    /// <summary>The key of a tree's append-fence sweep claim row.</summary>
    [GrainKeyBuilder]
    internal static string SweepKey(string treeId) => treeId;

    /// <summary>The key of a tree id's routing-generation floor row.</summary>
    [GrainKeyBuilder]
    internal static string RoutingFloorKey(string treeId) => treeId;

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

    /// <summary>Reads one page of a partition's append-epoch directory bucket.</summary>
    internal static Task<GrainState<WalAppendEpochDirectoryPageState>> ReadDirectoryPageAsync(
        IGrainStorage storage, string treeId, int partition, int bucket, int page) =>
        ReadAsync<WalAppendEpochDirectoryPageState>(storage, DirectoryPageGrainType, DirectoryPageKey(treeId, partition, bucket, page));

    /// <summary>Reads a tree's append-fence sweep claim row.</summary>
    internal static Task<GrainState<WalAppendFenceSweepState>> ReadSweepAsync(IGrainStorage storage, string treeId) =>
        ReadAsync<WalAppendFenceSweepState>(storage, SweepGrainType, SweepKey(treeId));

    /// <summary>Reads a tree id's routing-generation floor row; an absent row reads as floor 0.</summary>
    internal static Task<GrainState<TreeRoutingFloorState>> ReadRoutingFloorAsync(IGrainStorage storage, string treeId) =>
        ReadAsync<TreeRoutingFloorState>(storage, RoutingFloorGrainType, RoutingFloorKey(treeId));

    /// <summary>
    /// Raises a tree id's durable routing floors to at least <paramref name="atLeast"/>,
    /// per initiator, in one conditional max-merge write, re-read on an ETag conflict,
    /// and returns the floors as stored. A floor never moves down, and floors already
    /// covering every value cost one read.
    /// </summary>
    internal static async Task<TreeRoutingFloorState> RaiseRoutingFloorsAsync(
        IGrainStorage storage, string treeId, IReadOnlyDictionary<WalRoutingInitiator, long> atLeast)
    {
        ArgumentNullException.ThrowIfNull(atLeast);
        var key = RoutingFloorKey(treeId);
        for (var attempt = 1; ; attempt++)
        {
            var row = await ReadRoutingFloorAsync(storage, treeId).ConfigureAwait(true);
            var changed = false;
            foreach (var (initiator, value) in atLeast)
            {
                var tag = InitiatorTag(initiator);
                if (value > 0 && (!row.State.Floors.TryGetValue(tag, out var current) || current < value))
                {
                    row.State.Floors[tag] = value;
                    changed = true;
                }
            }

            if (!changed)
            {
                return row.State;
            }

            try
            {
                await WriteAsync(storage, RoutingFloorGrainType, key, row).ConfigureAwait(true);
                return row.State;
            }
            catch (InconsistentStateException) when (attempt < WalShardGrain.MaxFenceRowAttempts)
            {
                // Another retirement raised it since the read; max-merge with what it wrote.
            }
        }
    }

    /// <summary>
    /// Raises <paramref name="initiator"/>'s routing floor under <paramref name="treeId"/>
    /// to at least <paramref name="atLeast"/>, and returns that initiator's floor as stored.
    /// </summary>
    internal static async Task<long> RaiseRoutingFloorAsync(IGrainStorage storage, string treeId, WalRoutingInitiator initiator, long atLeast) =>
        FloorOf(
            await RaiseRoutingFloorsAsync(storage, treeId, new Dictionary<WalRoutingInitiator, long> { [initiator] = atLeast }).ConfigureAwait(true),
            initiator);

    /// <summary>Reads <paramref name="initiator"/>'s routing floor under <paramref name="treeId"/>; 0 when none is stored.</summary>
    internal static async Task<long> ReadRoutingFloorAsync(IGrainStorage storage, string treeId, WalRoutingInitiator initiator) =>
        FloorOf((await ReadRoutingFloorAsync(storage, treeId).ConfigureAwait(true)).State, initiator);
    /// <summary>Reads the row of <paramref name="grainType"/> under <paramref name="key"/>; an absent row reads as a new state.</summary>
    internal static Task<GrainState<T>> ReadRowAsync<T>(IGrainStorage storage, string grainType, string key)
        where T : new() =>
        ReadAsync<T>(storage, grainType, key);

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
