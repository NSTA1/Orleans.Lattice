namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A WAL partition's durable retired-epoch floor (epic #5605). Retiring a leaf's
/// epoch entry raises this floor to at least the entry's epoch before the entry
/// row is deleted, and no removal ever lowers it. A claim or a stamped append at
/// or below the floor is refused, so a surviving duplicate of a retired leaf can
/// never outrank the leaf re-created under the same id.
/// </summary>
/// <remarks>
/// Stored under a reserved grain type keyed by WAL partition
/// (<c>WalAppendFenceRows.FloorKey</c>). Binary persisted; every member is a
/// public property of a concrete type so the JSON grain-storage serializer round
/// trips it too. An absent row reads as floor 0.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalAppendEpochFloorState)]
internal sealed class WalAppendEpochFloorState : ILatticeBinaryPersistedState
{
    /// <summary>The highest epoch any retirement at this partition has retired.</summary>
    [Id(0)]
    public long Floor { get; set; }
}
