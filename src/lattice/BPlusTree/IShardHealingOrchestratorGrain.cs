namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Per-tree steady-state orchestrator that heals an over-split tree: it
/// observes the tree's shape and load on a cadence and consolidates a pair of
/// physical shards whenever the tree is measurably over-split and its load is
/// uniform enough that folding cannot recreate a hot spot.
/// <para>
/// <b>An observer, not a migration.</b> A one-shot startup pass cannot know
/// when a tree became over-split, cannot resume after a crash mid-heal, and
/// cannot react when a future ingest shatters a tree again. A continuous
/// observer handles damage already persisted on an existing volume and any
/// recurrence with the same code, and inherits resumability and idempotence
/// from the consolidation coordinator it drives.
/// </para>
/// <para>
/// <b>Bounded and polite.</b> At most one fold is admitted per sweep, at most
/// <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/> run at once,
/// and healing yields entirely while the tree's median shard rate is at or
/// above <see cref="LatticeOptions.ShardHealingBackpressureOpsPerSecond"/>, so
/// repairing a thousand-shard tree stays invisible to a user issuing queries.
/// </para>
/// <para>
/// <b>Default-on with a kill switch.</b> Healing runs unless
/// <see cref="LatticeOptions.ShardHealingEnabled"/> is <c>false</c>, so a
/// deployment whose trees are already shattered repairs itself with no
/// operator action, and an operator who hits trouble disables healing
/// specifically rather than reverting the image.
/// </para>
/// <para>
/// Key format: <c>{treeId}</c>.
/// </para>
/// </summary>
[Alias(TypeAliases.IShardHealingOrchestratorGrain)]
internal interface IShardHealingOrchestratorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Ensures the orchestrator is observing this tree, registering its
    /// keepalive reminder so it survives silo restarts. Idempotent - repeated
    /// calls are no-ops once it is running, and the call is a no-op when
    /// <see cref="LatticeOptions.ShardHealingEnabled"/> is <c>false</c>.
    /// </summary>
    Task EnsureRunningAsync();

    /// <summary>
    /// Runs one healing sweep synchronously: observes every physical shard's
    /// rate and split status, decides with
    /// <see cref="ShardHealingDecisionCore"/>, drives any in-flight fold
    /// forward by one bounded pass, and admits at most one new fold. Used by
    /// the sweep timer and by tests, which drive it directly so healing is
    /// assertable without waiting on wall-clock timers.
    /// </summary>
    Task RunHealingPassAsync();

    /// <summary>
    /// Returns the most recent sweep's <see cref="ShardHealingReport"/>,
    /// derived from persisted state so it is stable across reactivation. Safe
    /// to poll.
    /// </summary>
    Task<ShardHealingReport> GetHealingReportAsync();

    /// <summary>
    /// Stops the orchestrator and unregisters its reminder. Used by tree
    /// deletion and tests. Idempotent.
    /// <para>
    /// Stopping never tears a fold: an in-flight consolidation is left to its
    /// own coordinator, which is resumable and idempotent, so the tree is
    /// consistent whether the fold finishes or is later abandoned at a
    /// pre-swap boundary.
    /// </para>
    /// </summary>
    Task StopAsync();
}
