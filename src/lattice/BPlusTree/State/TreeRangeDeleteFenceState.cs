namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A tree's range-delete fence (epic #5605, member 2; design note section 2): the
/// tree's range-delete generation, which stamps every shard's walk of a fenced range
/// delete, and the open intents. Owned by <c>TreeRangeDeleteFenceGrain</c>, its only
/// writer.
/// </summary>
/// <remarks>
/// A new grain type's state, not a field on any existing state. Binary persisted;
/// every member is a public property of a concrete type so the JSON grain-storage
/// serializer round trips it too (#5579).
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.TreeRangeDeleteFenceState)]
internal sealed class TreeRangeDeleteFenceState : ILatticeBinaryPersistedState
{
    /// <summary>
    /// The tree's current range-delete generation: the one an open intent hands out.
    /// 0 before the first intent. A close raises it before it re-claims it.
    /// </summary>
    [Id(0)]
    public long Generation { get; set; }

    /// <summary>
    /// The highest generation claimed at every WAL partition of the tree. An intent
    /// is handed a generation only once it is claimed everywhere, so its walks never
    /// meet a partition that has no entry for it.
    /// </summary>
    [Id(1)]
    public long ClaimedGeneration { get; set; }

    /// <summary>The open intents, by intent id.</summary>
    [Id(2)]
    public Dictionary<Guid, TreeRangeDeleteIntent> Intents { get; set; } = new();
}
