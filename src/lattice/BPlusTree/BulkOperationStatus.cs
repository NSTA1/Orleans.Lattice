namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A bulk load's status on one shard root, read by a caller whose load call outran
/// the response timeout (#5812).
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.BulkOperationStatus)]
internal enum BulkOperationStatus : byte
{
    /// <summary>The load is neither running nor the shard's last completed bulk operation: it never started, was dropped, or faulted.</summary>
    NotRunning = 0,

    /// <summary>The load is running on the shard root now.</summary>
    Running = 1,

    /// <summary>The load is the shard's last completed bulk operation.</summary>
    Completed = 2,
}
