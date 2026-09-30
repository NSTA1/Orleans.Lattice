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

    /// <summary>
    /// The resolved physical tree id of the source, captured at merge start.
    /// Persisted so mid-merge alias rebinds don't mis-route subsequent ticks.
    /// </summary>
    [Id(6)] public string? SourcePhysicalTreeId { get; set; }

    /// <summary>
    /// The resolved physical tree id of the target (this grain's tree),
    /// captured at merge start.
    /// </summary>
    [Id(7)] public string? TargetPhysicalTreeId { get; set; }

    /// <summary>
    /// The list of distinct physical shard indices to drain from the source,
    /// captured at merge start from the source tree's current shard map.
    /// When empty, the grain resolves them on-demand (for forward compatibility
    /// with state persisted before this field existed).
    /// </summary>
    [Id(8)] public int[] SourcePhysicalShards { get; set; } = [];

    /// <summary>
    /// Resume position for the bounded drain of the shard at
    /// <see cref="NextShardIndex"/>: the key the next pass re-descends onto, or
    /// <see langword="null"/> to start at that shard's leftmost leaf. Cleared
    /// whenever the shard cursor advances, so each shard gets a fresh sweep.
    /// <para>
    /// A <b>key</b>, never a leaf grain id. Orleans grains are virtual, so a
    /// leaf id persisted across a pass boundary can activate an empty grain
    /// whose sibling pointer is null and end the resumed walk early, silently
    /// leaving the rest of the shard un-merged; a key always re-descends onto
    /// whichever leaf now owns it (issue 1973).
    /// </para>
    /// <para>
    /// Legacy persisted state decodes the missing slot to <see langword="null"/>,
    /// which is the correct semantic default.
    /// </para>
    /// </summary>
    [Id(9)] public string? DrainCursorKey { get; set; }

    /// <summary>
    /// Position in <see cref="SourcePhysicalShards"/> where the most recently
    /// resolved generation of source shards begins. Before completing, the merge
    /// checks that every shard from here on is still in the source's routing
    /// map; one that is not was retired by an online shard consolidation, which
    /// releases its storage, so its slots are re-drained from their current
    /// owners by appending the source's current shards as a new generation.
    /// State persisted before the field existed deserializes to <c>0</c>, which
    /// checks the whole list - the correct reading of a single generation.
    /// </summary>
    [Id(10)] public int SourceGenerationStart { get; set; }
}
