
using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Per-tree autonomic monitor that periodically samples each physical shard's
/// hotness counters (<see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetHotnessAsync"/>) and
/// triggers an online adaptive split on any shard whose observed
/// operations-per-second exceeds <see cref="LatticeOptions.HotShardOpsPerSecondThreshold"/>.
/// <para>
/// Activation is started when the tree's <c>LatticeGrain</c> activates, and
/// re-attempted on every write so that an arming which lost the race with
/// reminder-service startup recovers; the grain registers a reminder so it
/// survives silo restarts and continues monitoring without explicit
/// re-activation.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
[Alias(TypeAliases.IHotShardMonitorGrain)]
internal interface IHotShardMonitorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ensures the monitor is active for this tree. Idempotent - repeated
    /// calls are no-ops once the monitor is running.
    /// </summary>
    /// <remarks>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> to
    /// break an activation-time deadlock (#2218). A sampling pass
    /// (<see cref="RunSamplingPassAsync"/>) calls the tree's status verbs
    /// (<c>ILattice.Is*CompleteAsync</c>); on a quiet, idle-collected tree that
    /// call must birth a fresh <c>LatticeGrain</c> activation whose
    /// <c>OnActivateAsync</c> arms this monitor by awaiting this method. Without
    /// interleaving, the monitor - busy inside the sampling turn and not
    /// reentrant - cannot admit the arming call, and the activation cannot
    /// complete, so both expire at the 30s response deadline.
    /// <para>
    /// Interleaving is safe because the implementation runs
    /// <c>if (_running) return; _running = true;</c> with no await between the
    /// guard and the set, so single-threaded turn semantics let at most one
    /// caller ever execute the body. A call admitted while a sampling pass holds
    /// the turn observes <c>_running == true</c> - a live pass implies a live
    /// timer, which implies the set already ran - and returns synchronously,
    /// touching nothing. The attribute therefore adds no concurrent mutation; it
    /// only lets the no-op be admitted while the pass holds the turn.
    /// </para>
    /// </remarks>
    [AlwaysInterleave]
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
