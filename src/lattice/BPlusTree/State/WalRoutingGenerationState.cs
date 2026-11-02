namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A WAL partition's durable routing-generation entry for one shard-level
/// initiator (epic #5605): a shard root, or a tree's range-delete fence. It has
/// the leaf epoch entry's semantics: written only as a conditional max-merge by
/// the initiator's re-claim, and reloaded into memory by a re-presented re-claim,
/// so a WAL shard restart or move never loses a generation a close relied on.
/// </summary>
/// <remarks>
/// Stored under a reserved grain type keyed by WAL partition and initiator
/// (<c>WalAppendFenceRows.GenerationKey</c>). Binary persisted; every member is a
/// public property of a concrete type so the JSON grain-storage serializer round
/// trips it too. An absent row reads as generation 0.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalRoutingGenerationState)]
internal sealed class WalRoutingGenerationState : ILatticeBinaryPersistedState
{
    /// <summary>The highest generation the initiator has re-claimed at this partition.</summary>
    [Id(0)]
    public long Generation { get; set; }
}
