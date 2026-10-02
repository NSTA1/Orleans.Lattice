namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The durable write-ahead-log materialiser pin holding a tree's WAL floor, and the
/// leaf behind it, as reported by <c>lattice_treeadmin_wal_reclamation</c> (#4237).
/// A projection of <see cref="Orleans.Lattice.Api.TreeAdmin.TreeWalFloorHolder"/>.
/// </summary>
internal sealed record McpTreeWalFloorHolder
{
    /// <summary>The materialiser consumer id of the pin.</summary>
    public required string ConsumerId { get; init; }

    /// <summary>The leaf that published the pin, or <see langword="null"/> when its consumer id could not be parsed back to a leaf.</summary>
    public string? LeafId { get; init; }

    /// <summary>The WAL partition the pin belongs to.</summary>
    public int Partition { get; init; }

    /// <summary>The pin's durable offset, or <c>-1</c> when the pin reports none and so holds no offset floor.</summary>
    public long PinOffset { get; init; }

    /// <summary>The leaf's persisted checkpoint for the partition (<c>-1</c> when it has never checkpointed), or <see langword="null"/> when none was read.</summary>
    public long? PersistedCheckpoint { get; init; }

    /// <summary>
    /// The leaf's durable state, by name: <c>CheckpointedUncovered</c>, <c>NeverCheckpointed</c>,
    /// <c>NoDurableState</c>, <c>Unreadable</c>, <c>Orphaned</c> or <c>CheckpointedCoverageUnknown</c>.
    /// Read it beside <see cref="PinOffset"/>: <c>NeverCheckpointed</c> at <c>-1</c> is the benign
    /// sentinel, and at an offset of <c>0</c> or more it is a stranded pin.
    /// </summary>
    public required string State { get; init; }

    /// <summary>Whether the pin carries a usable offset and so holds the tree's offset floor.</summary>
    public bool HoldsOffsetFloor { get; init; }
}
