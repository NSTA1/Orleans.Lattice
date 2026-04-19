using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TombstoneCompactionGrain"/>.
/// Tracks the progress of an in-flight compaction pass so that it can be
/// resumed after a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TombstoneCompactionState)]
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

    /// <summary>
    /// The list of physical shard indices to compact, captured at the start of
    /// the current pass by resolving the tree's shard map. This is persisted
    /// so that the pass is resumable after a silo restart even if the shard
    /// map changes mid-pass (e.g. due to an adaptive split). When empty, the
    /// grain falls back to resolving the shard map on-demand.
    /// </summary>
    [Id(3)] public int[] PhysicalShardIndices { get; set; } = [];

    /// <summary>
    /// The resolved physical tree id (alias target) for the current pass.
    /// Persisted so that mid-pass alias rebinds don't mis-route subsequent
    /// ticks. <c>null</c> when no pass is in flight.
    /// </summary>
    [Id(4)] public string? PhysicalTreeId { get; set; }
}
