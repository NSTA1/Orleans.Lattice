namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Which durable pin holds a tree's write-ahead-log floor, and whether that pin
/// has wedged the tree's WAL reclamation. Returned by
/// <see cref="ILatticeWalReclamation.GetWalReclamationAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// Every leaf publishes a durable materialiser pin, and the WAL is trimmed only
/// below the lowest usable pin offset. A tree whose WAL holds steady can therefore
/// be idle, or be held by a pin that can never move; their trim and storage
/// figures read the same. This report names the pin, so the two can be told
/// apart.
/// </para>
/// <para>
/// <b>The wedge is keyed on the holder, never on growth.</b> A wedged tree need not
/// be growing, so a growth signal reads healthy on it. <see cref="IsWedged"/> is
/// true exactly when the holder carries a usable offset above a persisted
/// checkpoint that is still <c>-1</c>: the durable pin store cannot be lowered and
/// the WAL GC will not drive a leaf with no proven checkpoint, so the condition
/// does not clear on its own. A holder at offset <c>-1</c> in the same state is
/// the benign sentinel and clears once the leaf checkpoints.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalReclamationReport)]
[Immutable]
public sealed record TreeWalReclamationReport
{
    /// <summary>The tree this report describes, as the caller named it.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// Whether the durable pin store answered. When <see langword="false"/> the
    /// remaining fields are not measurements and <see cref="IsWedged"/> is
    /// <see langword="false"/> because nothing was established, not because the
    /// tree is healthy.
    /// </summary>
    [Id(1)] public bool PinStoreReadable { get; init; }

    /// <summary>How many materialiser pins the tree holds.</summary>
    [Id(2)] public int PinCount { get; init; }

    /// <summary>How many of those pins report no offset yet (<c>-1</c>) and so hold no offset floor.</summary>
    [Id(3)] public int PinsWithoutOffset { get; init; }

    /// <summary>
    /// The pin holding the floor: the one with the lowest usable offset, or, when
    /// no pin reports a usable offset, a pin at <c>-1</c>. <see langword="null"/>
    /// when the tree holds no pin.
    /// </summary>
    [Id(4)] public TreeWalFloorHolder? FloorHolder { get; init; }

    /// <summary>
    /// Whether reclamation is wedged: the floor holder carries a usable offset
    /// above a persisted checkpoint of <c>-1</c>
    /// (<see cref="TreeWalFloorHolderState.NeverCheckpointed"/>). Does not clear on
    /// its own.
    /// </summary>
    public bool IsWedged => FloorHolder is { PinOffset: >= 0, State: TreeWalFloorHolderState.NeverCheckpointed };
}
