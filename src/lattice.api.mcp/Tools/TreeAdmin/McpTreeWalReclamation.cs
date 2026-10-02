namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of <c>lattice_treeadmin_wal_reclamation</c>
/// (#4237): which durable pin holds a tree's write-ahead-log floor, and whether it
/// has wedged the tree's WAL reclamation. A projection of the facade's
/// <see cref="Orleans.Lattice.Api.TreeAdmin.TreeWalReclamationReport"/> that carries
/// its computed wedge verdict as a field, so a caller never has to re-derive it.
/// </summary>
internal sealed record McpTreeWalReclamation
{
    /// <summary>The tree this report describes, as the caller named it.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// Whether the durable pin store answered. When <see langword="false"/> the other
    /// fields are not measurements and <see cref="IsWedged"/> is <see langword="false"/>
    /// because nothing was established, not because the tree is healthy.
    /// </summary>
    public bool PinStoreReadable { get; init; }

    /// <summary>How many materialiser pins the tree holds.</summary>
    public int PinCount { get; init; }

    /// <summary>How many of those pins report no offset yet (<c>-1</c>) and so hold no offset floor.</summary>
    public int PinsWithoutOffset { get; init; }

    /// <summary>
    /// Whether reclamation is wedged: the floor holder carries a usable offset above a
    /// persisted checkpoint of <c>-1</c>. Keyed on the holder, never on WAL growth, and
    /// does not clear on its own.
    /// </summary>
    public bool IsWedged { get; init; }

    /// <summary>The pin holding the floor, or <see langword="null"/> when the tree holds no pin.</summary>
    public McpTreeWalFloorHolder? FloorHolder { get; init; }
}
