using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for periodic tombstone compaction across all leaves in
/// a single tree. One activation exists per tree, keyed by <c>{treeId}</c>.
/// Registers a single grain reminder whose period matches
/// <see cref="LatticeOptions.TombstoneGracePeriod"/>.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface ITombstoneCompactionGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ensures the compaction reminder is registered. Called once by
    /// <see cref="ILattice"/> on the first write to a tree. Idempotent.
    /// </summary>
    Task EnsureReminderAsync();

    /// <summary>
    /// Runs a complete compaction pass synchronously — walks every shard's
    /// leaf chain and compacts tombstones older than the configured grace
    /// period. Intended for manual triggers and integration testing;
    /// the periodic reminder uses a timer-based approach instead.
    /// </summary>
    Task RunCompactionPassAsync();

    /// <summary>
    /// Unregisters all compaction reminders and deactivates the grain.
    /// Called when the tree is deleted and compaction is no longer needed.
    /// </summary>
    Task UnregisterReminderAsync();
}
