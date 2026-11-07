namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A WAL partition's durable routing-generation table (epic #5605, #5987): one row per
/// partition with a slot per shard-level initiator (a shard root, or a tree's
/// range-delete fence). Each slot has the leaf epoch entry's semantics: raised only by a
/// conditional (ETag) read-modify-write that max-merges that slot and leaves every other
/// slot as read, and reloaded into memory by one read of the row, so a WAL shard restart
/// or move never loses a generation a close relied on.
/// </summary>
/// <remarks>
/// <para>
/// One row per partition, rather than one per partition and initiator, because the
/// partition's WAL shard grain is the one grain that admits against every slot: its cold
/// load reads the whole table once, so a new tree pays one read per partition rather than
/// one per partition and initiator.
/// </para>
/// <para>
/// Stored under a reserved grain type keyed by WAL partition
/// (<c>WalAppendFenceRows.GenerationsKey</c>). Binary persisted; every member is a public
/// property of a concrete type so the JSON grain-storage serializer round trips it too.
/// An absent row, or an initiator it does not name, reads as generation 0. The slot
/// count is bounded by the tree's initiators (its shard roots and its range-delete
/// fence), and every raise rewrites the whole row; raises are rare (an arm or a close).
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalRoutingGenerationsState)]
internal sealed class WalRoutingGenerationsState : ILatticeBinaryPersistedState
{
    /// <summary>
    /// The highest generation each initiator has re-claimed at this partition, keyed by
    /// its tag (<c>WalAppendFenceRows.InitiatorTag</c>).
    /// </summary>
    [Id(0)]
    public Dictionary<string, long> Generations { get; set; } = new(StringComparer.Ordinal);
}
