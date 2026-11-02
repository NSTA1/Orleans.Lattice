namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A leaf's durable append epoch (epic #5605). Written only by an activation's
/// epoch claim, as a strict conditional write on the ETag the claim read, so a
/// conflict on it always means another activation of the leaf claimed since.
/// Stored in its own row under a reserved grain type
/// (<c>WalAppendFenceRows.LeafEpochGrainType</c>), never inside
/// <see cref="LeafNodeState"/>, so a build that predates it rewrites the leaf row
/// without touching it.
/// </summary>
/// <remarks>
/// Written through the Orleans binary serializer
/// (<see cref="ILatticeBinaryPersistedState"/>); every member is a public
/// property of a concrete type so the JSON grain-storage serializer also round
/// trips it when a host bypasses Lattice's serializer (#5579). An absent row reads
/// as epoch 0: the leaf has never claimed.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LeafAppendEpochState)]
internal sealed class LeafAppendEpochState : ILatticeBinaryPersistedState
{
    /// <summary>The highest epoch any activation of the leaf has claimed.</summary>
    [Id(0)]
    public long Epoch { get; set; }
}
