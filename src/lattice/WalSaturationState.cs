namespace Orleans.Lattice;

/// <summary>
/// Three-state classification of a tree's write-ahead-log saturation
/// regime, surfaced by <see cref="IWalSaturationSignal"/> and pushed to
/// every registered <see cref="IWalSaturationObserver"/> on each
/// transition. Callers driving a producer into <see cref="ILattice"/>
/// should slow down on <see cref="Throttled"/> and pause new appends on
/// <see cref="Saturated"/>; <see cref="Healthy"/> means new appends will
/// admit without waiting.
/// <para>
/// The signal is computed from two internal regimes:
/// </para>
/// <list type="bullet">
///   <item><description>The per-(tree, partition) admission-semaphore
///   depth against <see cref="LatticeOptions.WalMaxPendingBatches"/>
///   (the writer-side admission gate). Sustained near-cap depth raises
///   the state to <see cref="Throttled"/>; a saturated semaphore with a
///   non-empty wait queue raises it to <see cref="Saturated"/>.</description></item>
///   <item><description>The recent rate of
///   <c>orleans.lattice.wal.append_dispatch.timeouts</c> trips against
///   <see cref="LatticeOptions.WalAppendDispatchTimeout"/>. Crossing
///   <see cref="LatticeOptions.WalSaturationDispatchTimeoutThreshold"/>
///   per sample window raises the state to <see cref="Saturated"/>
///   regardless of admission depth.</description></item>
/// </list>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalSaturationState)]
public enum WalSaturationState
{
    /// <summary>
    /// Admission-semaphore depth is well under the
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> cap and recent
    /// dispatch-timeout trips are below
    /// <see cref="LatticeOptions.WalSaturationDispatchTimeoutThreshold"/>.
    /// New appends will admit without waiting. The default state for a
    /// silo that has never seen a tree's WAL, and the steady-state
    /// regime for a healthy host.
    /// </summary>
    Healthy = 0,

    /// <summary>
    /// Admission-semaphore depth is at or above
    /// <see cref="LatticeOptions.WalSaturationThrottledRatio"/> of the
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> cap and recent
    /// admission waits have been non-trivial, but the cap has not been
    /// exceeded and no dispatch-timeout threshold has been crossed.
    /// Callers should slow down their offered rate but may continue
    /// dispatching - new appends will land, possibly after a brief
    /// admission wait.
    /// </summary>
    Throttled = 1,

    /// <summary>
    /// The admission semaphore is at the
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> cap with a
    /// non-empty wait queue, and / or the recent rate of
    /// <c>orleans.lattice.wal.append_dispatch.timeouts</c> trips has
    /// crossed
    /// <see cref="LatticeOptions.WalSaturationDispatchTimeoutThreshold"/>.
    /// Callers should pause new appends until the state returns to
    /// <see cref="Healthy"/> - continuing to dispatch will fault parked
    /// callers with <see cref="TimeoutException"/> rather than
    /// improving throughput.
    /// </summary>
    Saturated = 2,
}
