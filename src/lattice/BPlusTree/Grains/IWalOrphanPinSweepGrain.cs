using Orleans.Concurrency;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cluster-wide coordinator for one tree's WAL GC orphan-pin sweep, keyed by
/// <c>{treeId}</c> (issue #5097).
/// <para>
/// Every silo runs <see cref="LatticeWalGcScheduler"/>, and each one used to
/// sweep a blocked tree's durable materialiser pins itself, reading every
/// pinned leaf's persisted state on every silo. This grain runs that sweep at
/// most once per <see cref="LatticeWalGcScheduler.OrphanSweepInterval"/> for the
/// whole cluster and hands every silo the same outcome.
/// </para>
/// </summary>
[Alias(TypeAliases.IWalOrphanPinSweepGrain)]
internal interface IWalOrphanPinSweepGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records demand for a sweep of this tree and returns the most recent
    /// completed outcome, or <see langword="null"/> when no sweep has completed
    /// on this activation yet. Never waits for a sweep: the sweep runs on the
    /// grain's own phase timer, so a caller sees its demand served on a later call.
    /// </summary>
    /// <param name="classifyFloorHolders">
    /// Whether the caller needs the floor holders classified. Demand is
    /// coalesced, so one classifying request makes the next sweep classify.
    /// </param>
    [AlwaysInterleave]
    Task<WalOrphanPinSweepOutcome?> RequestSweepAsync(bool classifyFloorHolders);
}
