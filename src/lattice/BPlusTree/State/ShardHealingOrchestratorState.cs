using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persisted state for <see cref="Grains.ShardHealingOrchestratorGrain"/>.
/// <para>
/// Healing is a steady-state observer rather than a one-shot migration, so
/// its scheduling state must survive a silo restart: an orchestrator that
/// forgot which folds it had started would re-plan against a map those folds
/// were still mutating, and one that forgot its post-split stand-off would
/// resume consolidating immediately after a restart that happened to follow a
/// split. Both facts are therefore durable, and both are small - the in-flight
/// set is bounded by <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/>.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardHealingOrchestratorState)]
internal sealed class ShardHealingOrchestratorState
{
    /// <summary>
    /// Donor physical shard indices whose consolidation this orchestrator has
    /// started and not yet observed finish. Each entry addresses a
    /// <see cref="ITreeShardConsolidationGrain"/> keyed
    /// <c>{treeId}/{donorShardIndex}</c>, so the set is exactly what a
    /// reactivated orchestrator needs in order to resume polling without
    /// interrogating every shard in a thousand-shard tree.
    /// </summary>
    [Id(0)] public List<int> InFlightDonorShardIndices { get; set; } = [];

    /// <summary>
    /// UTC instant before which no consolidation may be admitted, set when a
    /// sweep observes an adaptive split in flight on the tree. This is the
    /// time-domain half of the hysteresis with the splitter; the skew dead
    /// band is the other half.
    /// </summary>
    [Id(1)] public DateTime? CooldownUntilUtc { get; set; }

    /// <summary>
    /// The decision the most recent sweep reached, so a reactivated
    /// orchestrator reports its last real observation rather than
    /// <see cref="ShardHealingDecision.NotObserved"/>.
    /// </summary>
    [Id(2)] public ShardHealingDecision LastDecision { get; set; }

    /// <summary>UTC ticks at which the most recent sweep observed the tree, or zero when none has.</summary>
    [Id(3)] public long LastObservedAtTicks { get; set; }

    /// <summary>Physical shards above the configured base at the most recent sweep.</summary>
    [Id(4)] public int LastBacklog { get; set; }
}
