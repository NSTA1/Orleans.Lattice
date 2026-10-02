namespace Orleans.Lattice;

/// <summary>
/// What an on-demand probe of a tree's durable WAL materialiser pins found
/// holding the tree's WAL floor (issue #4195): the pin with the lowest usable
/// offset, the leaf behind it, and that leaf's persisted checkpoint and
/// classification. Internal: the public face is the tree-admin facade's
/// <c>TreeWalReclamationReport</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The holder is chosen on the offset axis, exactly as the WAL GC floor is.</b>
/// A pin reporting offset <c>-1</c> constrains no offset floor, so the holder is
/// the pin with the lowest offset <c>&gt;= 0</c>. Only when no pin reports a
/// usable offset is a <c>-1</c> pin reported instead (the ordinally first), so a
/// tree whose pins are all still at the sentinel names one of them rather than
/// nothing.
/// </para>
/// <para>
/// <b>The wedge.</b> A holder with a usable offset whose persisted checkpoint is
/// still the <c>-1</c> sentinel (<see cref="WalGcBlockingPinState.NeverCheckpointed"/>)
/// is the stranded pin of issue #3258/#4191: the durable pin store merges
/// monotonic-max and cannot be lowered, and the GC refuses to drive a leaf with no
/// proven checkpoint, so the floor never moves. The same state at offset
/// <c>-1</c> is the benign sentinel that clears when the leaf checkpoints
/// (issue #4198). <see cref="IsWedged"/> keys on the pair, never on the state alone.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalFloorHolderProbeReport)]
[Immutable]
internal readonly record struct WalFloorHolderProbeReport
{
    /// <summary>The physical tree id probed.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>Whether the durable pin store answered. When <see langword="false"/> nothing else in the report is a measurement.</summary>
    [Id(1)] public bool PinStoreReadable { get; init; }

    /// <summary>How many materialiser pins the store holds for the tree.</summary>
    [Id(2)] public int PinCount { get; init; }

    /// <summary>How many of those pins report the <c>-1</c> "no offset yet" sentinel.</summary>
    [Id(3)] public int PinsWithoutOffset { get; init; }

    /// <summary>The holder's materialiser consumer id, or <see langword="null"/> when the tree holds no pin.</summary>
    [Id(4)] public string? ConsumerId { get; init; }

    /// <summary>The leaf that published the holder's pin, or <see langword="null"/> when the consumer id did not parse or there is no holder.</summary>
    [Id(5)] public string? LeafId { get; init; }

    /// <summary>The WAL partition the holder's pin belongs to.</summary>
    [Id(6)] public int Partition { get; init; }

    /// <summary>The holder's durable pin offset; <c>-1</c> when it reports none.</summary>
    [Id(7)] public long PinOffset { get; init; }

    /// <summary>The leaf's persisted checkpoint for the partition (<c>-1</c> when never checkpointed), or <see langword="null"/> when none was read.</summary>
    [Id(8)] public long? PersistedCheckpoint { get; init; }

    /// <summary>The holder's classification. Meaningful only when <see cref="ConsumerId"/> is set.</summary>
    [Id(9)] public WalGcBlockingPinState State { get; init; }

    /// <summary>
    /// Whether the floor is wedged: the holder carries a usable offset above a
    /// persisted checkpoint that is still the <c>-1</c> sentinel. Derived, so it
    /// is never stale against the fields it reads.
    /// </summary>
    public bool IsWedged =>
        ConsumerId is not null
        && PinOffset >= 0
        && State == WalGcBlockingPinState.NeverCheckpointed;
}
