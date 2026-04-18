using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TreeMergeGrain"/>.
/// Tracks the progress of an in-flight merge operation so that it can be
/// resumed after a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeMergeState)]
internal sealed class TreeMergeState
{
    /// <summary>Whether a merge operation is currently in progress.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>The next source shard index to process (0-based).</summary>
    [Id(1)] public int NextShardIndex { get; set; }

    /// <summary>
    /// Number of consecutive failures for the current shard.
    /// Reset to 0 when the shard advances.
    /// </summary>
    [Id(2)] public int ShardRetries { get; set; }

    /// <summary>The source tree ID to merge from.</summary>
    [Id(3)] public string? SourceTreeId { get; set; }

    /// <summary>
    /// The total number of source shards. Captured at the start of the merge
    /// so that it is consistent even if options change mid-operation.
    /// </summary>
    [Id(4)] public int SourceShardCount { get; set; }

    /// <summary>Whether the merge has fully completed.</summary>
    [Id(5)] public bool Complete { get; set; }
}
