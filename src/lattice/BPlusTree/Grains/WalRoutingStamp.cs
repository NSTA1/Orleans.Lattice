namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The routing part of a stamped WAL append (epic #5605, member 2): the shard-level
/// initiator a write was admitted under and that initiator's generation at
/// admission. It is captured where the write is admitted (a shard root's phase
/// check, or the tree's range-delete intent) and travels to the leaf as an explicit
/// argument of a stamped leaf method, never through an ambient carrier, so a write
/// that reaches the leaf through an existing method carries none and is appended
/// unstamped (design note section 2, S3). The leaf combines it with its own epoch
/// into the <see cref="WalAppendStamp"/> it appends under.
/// </summary>
/// <param name="Initiator">The shard-level initiator; <see cref="WalRoutingInitiator.None"/> when there is none.</param>
/// <param name="Generation">The initiator's generation at admission; at least 1 when <paramref name="Initiator"/> is set.</param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalRoutingStamp)]
internal readonly record struct WalRoutingStamp(
    [property: Id(0)] WalRoutingInitiator Initiator,
    [property: Id(1)] long Generation)
{
    /// <summary>
    /// The receiving leaf's count of hand-off closes when it admitted the write, plus
    /// one; 0 when not captured (#5664 (a)). Set only by the leaf itself, at the entry of
    /// a stamped write method and before its span admission, and reset there for every
    /// call: a value received from another grain is never used. A write dispatched after
    /// a close it was admitted before is self-fenced and re-admitted, so a donor's write
    /// admitted before its split narrowed is never appended above the close's heads.
    /// </summary>
    [Id(2)]
    public long AdmissionMark { get; init; }

    /// <summary>No routing stamp.</summary>
    public static WalRoutingStamp None => default;

    /// <summary><see langword="true"/> when the stamp names an initiator.</summary>
    public bool IsSet => Initiator.IsSet && Initiator.Kind != WalRoutingInitiatorKind.ForceUnstamped;

    /// <summary>
    /// A stamp that forces the leaf's append through the existing, unstamped WAL
    /// method, reported to the leaf itself (S3): for a write that reached the leaf
    /// through an existing, unstamped leaf method.
    /// </summary>
    public static WalRoutingStamp Unstamped => new(new WalRoutingInitiator(WalRoutingInitiatorKind.ForceUnstamped, 0), 0);

    /// <summary><see langword="true"/> for <see cref="Unstamped"/>.</summary>
    public bool ForcesUnstamped => Initiator.Kind == WalRoutingInitiatorKind.ForceUnstamped;

    /// <summary>The tree range-delete generation <paramref name="generation"/>.</summary>
    public static WalRoutingStamp ForTreeRangeDelete(long generation)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generation);
        return new(WalRoutingInitiator.TreeRangeDelete, generation);
    }

    /// <summary>Shard root <paramref name="shardIndex"/>'s routing generation <paramref name="generation"/>.</summary>
    public static WalRoutingStamp ForShardRoot(int shardIndex, long generation)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(generation);
        return new(WalRoutingInitiator.ShardRoot(shardIndex), generation);
    }

    /// <summary><paramref name="leafStamp"/> with this routing stamp's initiator and generation.</summary>
    public WalAppendStamp Apply(WalAppendStamp leafStamp) =>
        IsSet ? leafStamp with { Initiator = Initiator, Generation = Generation } : leafStamp;
}
