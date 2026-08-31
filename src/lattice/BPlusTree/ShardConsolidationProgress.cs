namespace Orleans.Lattice.BPlusTree;

using Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Point-in-time progress report for an online shard consolidation, returned
/// by <see cref="ITreeShardConsolidationGrain.GetProgressAsync"/>.
/// <para>
/// This is the observation half of the driver seam a healing orchestrator
/// uses: start the operation, poll this report to decide whether it is
/// advancing and whether to admit another concurrent consolidation, and
/// cancel it when the healing budget is spent. Every field is derived from
/// the coordinator's persisted state, so the report survives a silo restart
/// and is stable across reactivation.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardConsolidationProgress)]
[Immutable]
internal readonly record struct ShardConsolidationProgress
{
    /// <summary>Whether a consolidation is currently in flight for this donor shard.</summary>
    [Id(0)] public bool InProgress { get; init; }

    /// <summary>Whether the most recent consolidation ran to completion.</summary>
    [Id(1)] public bool Complete { get; init; }

    /// <summary>
    /// Whether the most recent consolidation ended because a cancel was
    /// honoured at a pre-swap boundary rather than because it completed.
    /// </summary>
    [Id(2)] public bool Cancelled { get; init; }

    /// <summary>Current phase of the consolidation state machine.</summary>
    [Id(3)] public ShardConsolidationPhase Phase { get; init; }

    /// <summary>Physical shard index being retired.</summary>
    [Id(4)] public int DonorShardIndex { get; init; }

    /// <summary>Physical shard index absorbing the donor's virtual slots.</summary>
    [Id(5)] public int SurvivorShardIndex { get; init; }

    /// <summary>Number of virtual slots this operation folds onto the survivor.</summary>
    [Id(6)] public int SlotsToFold { get; init; }

    /// <summary>Entries forwarded from the donor to the survivor so far.</summary>
    [Id(7)] public long EntriesDrained { get; init; }

    /// <summary>Donor leaves visited by the drain across all passes so far.</summary>
    [Id(8)] public long LeavesScanned { get; init; }

    /// <summary>
    /// Unique operation id of the current or most-recent consolidation, or
    /// <see langword="null"/> when none has ever run on this donor.
    /// </summary>
    [Id(9)] public string? OperationId { get; init; }

    /// <summary>UTC ticks at which the current or most-recent operation started.</summary>
    [Id(10)] public long StartedAtTicks { get; init; }

    /// <summary>UTC ticks of the most recent persisted phase or drain advance.</summary>
    [Id(11)] public long UpdatedAtTicks { get; init; }

    /// <summary>
    /// Whether a cancel has been requested but not yet honoured. A cancel is
    /// only actionable at a boundary strictly before
    /// <see cref="ShardConsolidationPhase.Swap"/>; past that point the flag
    /// stays set but the operation deliberately runs to completion.
    /// </summary>
    [Id(12)] public bool CancelRequested { get; init; }
}
