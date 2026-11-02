namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A tree id's durable routing-generation floor (epic #5605, #5668). A purge that
/// retires the tree's per-tree fence rows (each shard root's hand-off marker, each
/// partition's routing-generation entries, and the range-delete fence's state) first
/// raises this floor to at least every routing generation it is about to delete,
/// and no removal ever lowers it. A WAL partition refuses a routing-generation claim
/// at or below the floor, and a re-created incarnation starts its generations above
/// it, so no generation of a purged incarnation recurs under the same tree id.
/// </summary>
/// <remarks>
/// Stored under a reserved grain type keyed by tree id
/// (<c>WalAppendFenceRows.RoutingFloorKey</c>), so it outlives every row it covers:
/// it is the one row a purge leaves per purged tree id. Binary persisted; every
/// member is a public property of a concrete type so the JSON grain-storage
/// serializer round trips it too. An absent row reads as floor 0.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.TreeRoutingFloorState)]
internal sealed class TreeRoutingFloorState : ILatticeBinaryPersistedState
{
    /// <summary>The highest routing generation any purge of this tree id has retired.</summary>
    [Id(0)]
    public long Floor { get; set; }
}
