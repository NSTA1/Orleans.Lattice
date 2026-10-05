namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Raised by <see cref="IWalShardGrain"/> when it refuses a freshly authored
/// local write whose HLC stamp is below the partition's clock floor (issue
/// #4586). The partition has published that floor to its replication shippers
/// with the promise that every later offset carries a stamp at or above it, so
/// it cannot accept the write; no offset was assigned and nothing was applied.
/// The producer merges its clock with <see cref="Floor"/> and re-stamps, or -
/// when the stamp is fixed by contract - reports a typed refusal.
/// <para>
/// Derives directly from <see cref="Exception"/>, so a same-silo deep copy of a
/// grain result needs no registered copier.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalStampBelowFloorException)]
internal sealed class WalStampBelowFloorException : Exception
{
    /// <summary>The tree whose partition refused the write. Empty on the convenience constructors.</summary>
    [Id(0)]
    public string TreeId { get; }

    /// <summary>The WAL partition that refused the write.</summary>
    [Id(1)]
    public int Partition { get; }

    /// <summary>The refused stamp.</summary>
    [Id(2)]
    public HybridLogicalClock Timestamp { get; }

    /// <summary>The partition's clock floor the stamp fell below.</summary>
    [Id(3)]
    public HybridLogicalClock Floor { get; }

    /// <summary>Initialises a new instance with a default message.</summary>
    public WalStampBelowFloorException()
        : base("The WAL partition refused a write stamped below its clock floor.")
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance with <paramref name="message"/>.</summary>
    /// <param name="message">Diagnostic message.</param>
    public WalStampBelowFloorException(string message)
        : base(message)
    {
        TreeId = string.Empty;
    }

    /// <summary>Initialises a new instance for a refusal on <paramref name="treeId"/>/<paramref name="partition"/>.</summary>
    /// <param name="treeId">The tree id.</param>
    /// <param name="partition">The WAL partition.</param>
    /// <param name="timestamp">The refused stamp.</param>
    /// <param name="floor">The partition's floor.</param>
    public WalStampBelowFloorException(string treeId, int partition, HybridLogicalClock timestamp, HybridLogicalClock floor)
        : base($"WAL partition {treeId}/{partition} refused a write stamped {timestamp} below its clock floor {floor}.")
    {
        TreeId = treeId;
        Partition = partition;
        Timestamp = timestamp;
        Floor = floor;
    }
}
