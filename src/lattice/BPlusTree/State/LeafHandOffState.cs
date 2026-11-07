namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// A leaf's hand-off marker (epic #5605, member 2; design note section 2): the open
/// leaf-level hand-offs onto it (a split onto its sibling, a reclaim onto the
/// survivor) and the recent closes with their fence heads. The leaf holds the live
/// advance while any is open, and re-arms only after replaying to a close's heads.
/// </summary>
/// <remarks>
/// <para>
/// Stored in its own row under the reserved grain type <c>_lattice_leaf-handoff</c>
/// (<c>HandOffMarkerRows</c>), never inside <see cref="LeafNodeState"/>. Written by
/// hand-off initiators and by the leaf's birth, each conditionally on the ETag it
/// read. A leaf-level hand-off covers the whole owner, so its ranges are unbounded.
/// </para>
/// <para>
/// <b>Absence never means closed.</b> A missing row, or one whose
/// <see cref="Initialised"/> is <see langword="false"/>, holds the leaf. A leaf born
/// under the gate writes the flag at its birth; an existing leaf takes its shard's
/// flag once the shard's is set and the leaf has no split in flight.
/// </para>
/// <para>
/// Binary persisted; every member is a public property of a concrete type, so the
/// JSON grain-storage serializer round trips it too (#5579).
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LeafHandOffState)]
internal sealed class LeafHandOffState : ILatticeBinaryPersistedState, Orleans.Lattice.BPlusTree.Grains.IHandOffMarker
{
    /// <summary>Set once the leaf's marker state is known: at its birth under the gate, or taken from its shard.</summary>
    [Id(0)]
    public bool Initialised { get; set; }

    /// <summary>The open hand-offs, by token.</summary>
    [Id(1)]
    public Dictionary<string, HandOffMarkerRange> Open { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The most recent closes, oldest first; at most <c>HandOffMarkerCore.MaxRetainedCloses</c>.</summary>
    [Id(2)]
    public List<HandOffClose> Closes { get; set; } = [];

    /// <summary>The per-partition maximum of the heads of every close evicted from <see cref="Closes"/>.</summary>
    [Id(3)]
    public long[] CompactedHeads { get; set; } = [];

    /// <summary>Raised on every open and close.</summary>
    [Id(4)]
    public long Sequence { get; set; }

    /// <summary>
    /// The routing generation the shard root raised for the link batch that linked this
    /// marker's leaf as a split sibling, written only after that raise is durable; 0 while
    /// unwitnessed (a5e2e2ef's B2 and G1 on #5866). Held at marker level so a witness that
    /// lands before the donor's close - still open, re-driven or recovered - is never lost:
    /// a donor close written later copies it, and while it is set no donor bound on the
    /// marker is pending. A sibling's marker belongs to one leaf, born once.
    /// </summary>
    [Id(5)]
    public long SiblingLinkedGeneration { get; set; }

    /// <summary>The shard root whose raise <see cref="SiblingLinkedGeneration"/> witnesses (A1); meaningful only while that is above 0.</summary>
    [Id(6)]
    public int SiblingLinkedShard { get; set; }
}
