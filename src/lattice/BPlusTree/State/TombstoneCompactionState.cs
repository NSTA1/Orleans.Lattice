namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TombstoneCompactionGrain"/>.
/// Tracks the progress of an in-flight compaction pass so that it can be
/// resumed after a silo restart.
/// </summary>
[GenerateSerializer]
internal sealed class TombstoneCompactionState
{
    /// <summary>Whether a compaction pass is currently in progress.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>The next shard index to compact (0-based).</summary>
    [Id(1)] public int NextShardIndex { get; set; }

    /// <summary>
    /// Number of consecutive failures for the current shard.
    /// Reset to 0 when the shard succeeds or is skipped.
    /// </summary>
    [Id(2)] public int ShardRetries { get; set; }
}
