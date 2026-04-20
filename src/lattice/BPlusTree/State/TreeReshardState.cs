namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for the <c>TreeReshardGrain</c> coordinator. Tracks the
/// lifecycle of a single online reshard: the target physical shard count,
/// the current phase, and an operation ID for idempotent retries. Once
/// <see cref="InProgress"/> is <c>false</c> the grain deactivates and the
/// state is retained only to back <c>IsCompleteAsync</c>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeReshardState)]
internal sealed class TreeReshardState
{
    /// <summary>Whether a reshard is currently in progress for this tree.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>Whether the most recent reshard completed successfully.</summary>
    [Id(1)] public bool Complete { get; set; }

    /// <summary>Unique operation ID for the current / most-recent reshard.</summary>
    [Id(2)] public string? OperationId { get; set; }

    /// <summary>Current phase of the reshard state machine.</summary>
    [Id(3)] public ReshardPhase Phase { get; set; }

    /// <summary>
    /// Target number of distinct physical shards. Reshard terminates once
    /// the persisted <see cref="ShardMap"/> contains at least this many
    /// distinct physical shard indices.
    /// </summary>
    [Id(4)] public int TargetShardCount { get; set; }
}
