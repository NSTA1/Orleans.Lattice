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
    /// Target number of distinct physical shards. A grow terminates once the
    /// persisted <see cref="ShardMap"/> contains at least this many distinct
    /// physical shard indices; a shrink (<see cref="Shrinking"/>) once it
    /// contains at most this many and every fold it started has finished.
    /// </summary>
    [Id(4)] public int TargetShardCount { get; set; }

    /// <summary>
    /// Whether the in-flight reshard reduces the shard count - folding shards
    /// together through online shard consolidation - rather than growing it
    /// through shard splits. Fixed when the reshard starts, so a grow that
    /// briefly overshoots its target can never turn into a shrink. State
    /// persisted before the field existed deserializes to <c>false</c>, the grow
    /// every older reshard was.
    /// </summary>
    [Id(5)] public bool Shrinking { get; set; }

    /// <summary>
    /// Donor shard indices of the folds a shrink has started and not yet seen
    /// finish. Recorded before each fold is started, so the set can over-count
    /// (the next reconcile drops a fold that is not running) but never
    /// under-count; it bounds concurrency and holds the shrink open until every
    /// fold - including the retirement of its donor's storage - has finished.
    /// </summary>
    [Id(6)] public List<int> ConsolidationDonorShardIndices { get; set; } = [];

    /// <summary>
    /// The number of distinct physical shards the tree's <see cref="ShardMap"/>
    /// named when the current or most recent reshard started, so its progress can
    /// be measured from where it began rather than from zero. Legacy persisted
    /// state decodes the missing slot to <c>0</c>, which means "not recorded".
    /// </summary>
    [Id(7)] public int StartShardCount { get; set; }
}
