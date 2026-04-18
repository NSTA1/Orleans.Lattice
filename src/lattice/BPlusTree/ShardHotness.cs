using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Volatile hotness counters for a single shard. Tracks the number of read and
/// write operations processed since the grain was activated (or counters were
/// last reset). Counters are in-memory only — they reset on grain deactivation
/// and are never persisted. Used by split coordinators to detect hot shards.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardHotness)]
[Immutable]
[EditorBrowsable(EditorBrowsableState.Never)]
public readonly record struct ShardHotness
{
    /// <summary>Number of read operations (Get, Exists, GetMany, key/entry scans) since activation.</summary>
    [Id(0)] public long Reads { get; init; }

    /// <summary>Number of write operations (Set, Delete, SetMany, Merge, BulkLoad) since activation.</summary>
    [Id(1)] public long Writes { get; init; }

    /// <summary>
    /// Wall-clock duration since the counters were started (grain activation time).
    /// Divide <see cref="Reads"/> or <see cref="Writes"/> by this to obtain a rate.
    /// </summary>
    [Id(2)] public TimeSpan Window { get; init; }
}
