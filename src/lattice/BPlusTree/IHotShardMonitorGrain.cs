
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Per-tree autonomic monitor that periodically samples each physical shard's
/// hotness counters (<see cref="IShardRootGrain.GetHotnessAsync"/>) and
/// triggers an online adaptive split on any shard whose observed
/// operations-per-second exceeds <see cref="LatticeOptions.HotShardOpsPerSecondThreshold"/>.
/// <para>
/// Activation is started lazily by <c>LatticeGrain</c> on the first write to
/// a tree; the grain registers a reminder so it survives silo restarts and
/// continues monitoring without explicit re-activation.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
[Alias(TypeAliases.IHotShardMonitorGrain)]
internal interface IHotShardMonitorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ensures the monitor is active for this tree. Idempotent — repeated
    /// calls are no-ops once the monitor is running.
    /// </summary>
    Task EnsureRunningAsync();

    /// <summary>
    /// Synchronously runs one sampling pass: polls every physical shard's
    /// hotness, applies suppression rules, and triggers a split on the
    /// hottest eligible shard if any. Used by tests to drive the monitor
    /// deterministically.
    /// </summary>
    Task RunSamplingPassAsync();

    /// <summary>
    /// Stops the monitor and unregisters its reminder. Used by tree deletion
    /// and tests. Idempotent.
    /// </summary>
    Task StopAsync();
}
