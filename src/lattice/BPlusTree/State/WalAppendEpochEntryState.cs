namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A WAL partition's durable epoch entry for one leaf (epic #5605): the highest
/// epoch a claim of that leaf has written at the partition. Written only as a
/// conditional max-merge, so it never moves down, and deleted only by the leaf's
/// retirement after the partition's retired floor has been raised to at least
/// its value.
/// </summary>
/// <remarks>
/// Stored under a reserved grain type keyed by WAL partition and leaf
/// (<c>WalAppendFenceRows.EntryKey</c>), never inside
/// <c>WalShardFloorState</c>. Binary persisted; every member is a public property
/// of a concrete type so the JSON grain-storage serializer round trips it too.
/// An absent row reads as epoch 0.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalAppendEpochEntryState)]
internal sealed class WalAppendEpochEntryState : ILatticeBinaryPersistedState
{
    /// <summary>The highest epoch claimed for the leaf at this partition.</summary>
    [Id(0)]
    public long Epoch { get; set; }
}
