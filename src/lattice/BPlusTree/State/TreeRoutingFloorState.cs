namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A tree id's durable routing-generation floors, one per initiator (epic #5605,
/// #5668, #5705). Before a purge deletes a tree's per-tree fence rows (each shard
/// root's hand-off marker, each partition's routing-generation entries, and the
/// range-delete fence's state), it raises every initiator's floor to at least every
/// generation it is about to delete. A shard root that re-seeds a purged root and finds
/// a marker that predates it (one a 10.2.0 purge left) raises its own initiator's floor
/// over it. No removal ever lowers a floor.
/// </summary>
/// <remarks>
/// <para>
/// A WAL partition refuses a routing-generation claim at or below its initiator's
/// floor, and a re-created incarnation seeds each first generation above it, so no
/// generation of a purged incarnation recurs under the same tree id. The floors are
/// per initiator because a re-seed's catch-up runs one shard at a time while the
/// re-created incarnation is live. A single tree-wide floor raised by a later shard's
/// catch-up could cover a generation another shard already holds live.
/// </para>
/// <para>
/// Stored under a reserved grain type keyed by tree id
/// (<c>WalAppendFenceRows.RoutingFloorKey</c>), so it outlives every row it covers:
/// it is the one row a purge leaves per purged tree id. Binary persisted; every
/// member is a public property of a concrete type so the JSON grain-storage
/// serializer round trips it too. An absent row, or an initiator it does not name,
/// reads as floor 0.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.TreeRoutingFloorState)]
internal sealed class TreeRoutingFloorState : ILatticeBinaryPersistedState
{
    /// <summary>
    /// The highest routing generation retired for each initiator, keyed by its tag
    /// (<c>WalAppendFenceRows.InitiatorTag</c>).
    /// </summary>
    [Id(0)]
    public Dictionary<string, long> Floors { get; set; } = new(StringComparer.Ordinal);
}
