namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Thrown when a bulk-load chunk contains keys that are not in strictly ascending
/// order. Bulk-load grafts entries onto the right edge of each shard without
/// splits, so a globally ascending key order (both within a chunk and across the
/// whole stream) is required for a correct tree; an out-of-order chunk is rejected
/// before any entry is applied, leaving no partial data behind. A transport
/// binding surfaces this as a distinct, typed <c>InvalidOrder</c> outcome that
/// identifies the offending key.
/// </summary>
public sealed class BulkLoadOrderException : Exception
{
    /// <summary>
    /// Initialises the exception for the first out-of-order key.
    /// </summary>
    /// <param name="treeId">The tree the rejected chunk targeted.</param>
    /// <param name="chunkIndex">The zero-based index of the rejected chunk.</param>
    /// <param name="offendingKey">The first key found out of ascending order.</param>
    /// <param name="precedingKey">The key that immediately preceded <paramref name="offendingKey"/>.</param>
    public BulkLoadOrderException(string treeId, long chunkIndex, string offendingKey, string precedingKey)
        : base($"Bulk-load chunk {chunkIndex} for tree '{treeId}' is not in strictly ascending key order: "
            + $"key '{offendingKey}' does not sort after the preceding key '{precedingKey}'. "
            + "Bulk-load entries must be globally ascending; no data was applied.")
    {
        TreeId = treeId;
        ChunkIndex = chunkIndex;
        OffendingKey = offendingKey;
        PrecedingKey = precedingKey;
    }

    /// <summary>The tree the rejected chunk targeted.</summary>
    public string TreeId { get; }

    /// <summary>The zero-based index of the rejected chunk.</summary>
    public long ChunkIndex { get; }

    /// <summary>The first key found out of ascending order.</summary>
    public string OffendingKey { get; }

    /// <summary>The key that immediately preceded <see cref="OffendingKey"/>.</summary>
    public string PrecedingKey { get; }
}
