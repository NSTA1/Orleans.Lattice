namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The durable write-ahead-log materialiser pin holding a tree's WAL floor, and
/// the leaf behind it.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalFloorHolder)]
[Immutable]
public sealed record TreeWalFloorHolder
{
    /// <summary>The materialiser consumer id of the pin.</summary>
    [Id(0)] public required string ConsumerId { get; init; }

    /// <summary>The leaf that published the pin, or <see langword="null"/> when its consumer id could not be parsed back to a leaf.</summary>
    [Id(1)] public string? LeafId { get; init; }

    /// <summary>The WAL partition the pin belongs to.</summary>
    [Id(2)] public int Partition { get; init; }

    /// <summary>The pin's durable offset, or <c>-1</c> when the pin reports none and so holds no offset floor.</summary>
    [Id(3)] public long PinOffset { get; init; }

    /// <summary>The leaf's persisted checkpoint for the partition (<c>-1</c> when it has never checkpointed), or <see langword="null"/> when none was read.</summary>
    [Id(4)] public long? PersistedCheckpoint { get; init; }

    /// <summary>The leaf's durable state.</summary>
    [Id(5)] public TreeWalFloorHolderState State { get; init; }

    /// <summary>Whether the pin carries a usable offset and so holds the tree's offset floor; <see langword="false"/> for a pin at <c>-1</c>.</summary>
    public bool HoldsOffsetFloor => PinOffset >= 0;
}
