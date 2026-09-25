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
/// The signal is computed each sample window from several inputs, each
/// named by a <see cref="WalSaturationCause"/> on the transition it drives:
/// </para>
/// <list type="bullet">
///   <item><description>Acute inputs raise the state to
///   <see cref="Saturated"/>: recent
///   <c>orleans.lattice.wal.append_dispatch.timeouts</c> trips crossing
///   <see cref="LatticeOptions.WalSaturationDispatchTimeoutThreshold"/>,
///   WAL storage-provider failures crossing
///   <see cref="LatticeOptions.WalSaturationProviderFailureRateThreshold"/>
///   (when set), and flush latency held at or above
///   <see cref="LatticeOptions.WalSaturationFlushLatencyThreshold"/> for
///   the configured consecutive windows (when set).</description></item>
///   <item><description>Back-off inputs raise it only to
///   <see cref="Throttled"/>: the per-(tree, partition) admission-semaphore
///   depth reaching <see cref="LatticeOptions.WalSaturationThrottledRatio"/>
///   of <see cref="LatticeOptions.WalMaxPendingBatches"/>, and (when set) a
///   sustained materialiser drain lag or durable materialiser-pin write
///   latency.</description></item>
///   <item><description>An admission semaphore at its cap with callers
///   parked on it reads <see cref="Throttled"/> under the default
///   <see cref="LatticeOptions.WalSaturationAcuteOnly"/> = <see langword="true"/>,
///   and <see cref="Saturated"/> only when that option is disabled.</description></item>
/// </list>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalSaturationState)]
public enum WalSaturationState
{
    /// <summary>
    /// No input is at its Throttled or Saturated threshold: admission-semaphore
    /// depth is below <see cref="LatticeOptions.WalSaturationThrottledRatio"/>
    /// of the <see cref="LatticeOptions.WalMaxPendingBatches"/> cap and no acute
    /// threshold has been crossed. New appends will admit without waiting. The
    /// default state for a silo that has never seen a tree's WAL, and the
    /// steady-state regime for a healthy host.
    /// </summary>
    Healthy = 0,

    /// <summary>
    /// A back-off input is at its threshold - admission-semaphore depth at or
    /// above <see cref="LatticeOptions.WalSaturationThrottledRatio"/> of the
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> cap (including a
    /// semaphore at its cap under the default
    /// <see cref="LatticeOptions.WalSaturationAcuteOnly"/>), or a sustained
    /// materialiser drain lag or pin latency when those inputs are enabled - and
    /// no acute threshold has been crossed. Callers should slow down their
    /// offered rate but may continue dispatching - new appends will land,
    /// possibly after a brief admission wait.
    /// </summary>
    Throttled = 1,

    /// <summary>
    /// An acute input has crossed its threshold: recent
    /// <c>orleans.lattice.wal.append_dispatch.timeouts</c> trips at or above
    /// <see cref="LatticeOptions.WalSaturationDispatchTimeoutThreshold"/>,
    /// provider failures or sustained flush latency at their thresholds (when
    /// enabled), or - only when <see cref="LatticeOptions.WalSaturationAcuteOnly"/>
    /// is disabled - an admission semaphore at its cap with callers parked on
    /// it. Callers should pause new appends until the state returns to
    /// <see cref="Healthy"/> - continuing to dispatch will fault parked
    /// callers with <see cref="TimeoutException"/> rather than
    /// improving throughput.
    /// </summary>
    Saturated = 2,
}
