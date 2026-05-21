
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for periodic tombstone compaction across all leaves in
/// a single tree. One activation exists per tree, keyed by <c>{treeId}</c>.
/// Registers a single grain reminder whose period matches
/// <see cref="LatticeOptions.TombstoneGracePeriod"/>.
/// </summary>
[Alias(TypeAliases.ITombstoneCompactionGrain)]
internal interface ITombstoneCompactionGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ensures the compaction reminder is registered. Called once by
    /// <see cref="ILattice"/> on the first write to a tree. Idempotent.
    /// </summary>
    Task EnsureReminderAsync();

    /// <summary>
    /// Runs a complete compaction pass synchronously - walks every shard's
    /// leaf chain and compacts tombstones older than the configured grace
    /// period. Intended for manual triggers and integration testing;
    /// the periodic reminder uses a timer-based approach instead.
    /// </summary>
    Task RunCompactionPassAsync();

    /// <summary>
    /// Schedules an out-of-cycle compaction pass for a single shard.
    /// Honours <see cref="LatticeOptions.CompactionTriggerCooldown"/> when
    /// <paramref name="triggerKind"/> is <c>"ratio"</c> or <c>"size"</c>:
    /// if a pass for the same shard ran inside the cooldown window the
    /// request is silently dropped. <c>"operator"</c> requests bypass the
    /// cooldown and proceed unconditionally. If a pass is already in
    /// flight on this grain (regular reminder or otherwise) the request
    /// is dropped because the in-flight pass will visit every shard.
    /// Returns <c>true</c> when the request was honoured (a pass was
    /// scheduled), <c>false</c> when it was dropped.
    /// </summary>
    /// <param name="shardIndex">Physical shard index to compact.</param>
    /// <param name="triggerKind">
    /// Diagnostic label tagged onto pass-level telemetry. One of
    /// <c>"ratio"</c>, <c>"size"</c>, or <c>"operator"</c>.
    /// </param>
    Task<bool> RequestCompactionAsync(int shardIndex, string triggerKind);

    /// <summary>
    /// Unregisters all compaction reminders and deactivates the grain.
    /// Called when the tree is deleted and compaction is no longer needed.
    /// </summary>
    Task UnregisterReminderAsync();
}
