namespace Orleans.Lattice;

/// <summary>
/// Which of the sampler's inputs drove a
/// <see cref="WalSaturationStateChange"/>. Several independent inputs map to the
/// same <see cref="WalSaturationState"/>, so the state alone does not say what a
/// host should look at: two different conditions both raise
/// <see cref="WalSaturationState.Throttled"/>, and four both raise
/// <see cref="WalSaturationState.Saturated"/>. This discriminator names the one
/// the sampler attributed the transition to, so an observer can route an alert
/// at the subsystem actually under pressure.
/// <para>
/// Attribution is best-effort and single-valued. When more than one input
/// crossed in the same sample window the sampler reports the one evaluated
/// first, in the order this enum declares; the transition itself is unaffected.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalSaturationCause)]
public enum WalSaturationCause
{
    /// <summary>
    /// No single input was attributed. The value carried on every transition
    /// back to <see cref="WalSaturationState.Healthy"/>, and on any transition
    /// published by a host predating cause attribution.
    /// </summary>
    None = 0,

    /// <summary>
    /// Recent <c>orleans.lattice.wal.append_dispatch.timeouts</c> trips crossed
    /// <see cref="LatticeOptions.WalSaturationDispatchTimeoutThreshold"/> in one
    /// sample window.
    /// </summary>
    DispatchTimeouts = 1,

    /// <summary>
    /// Recent WAL storage-provider failures crossed
    /// <see cref="LatticeOptions.WalSaturationProviderFailureThreshold"/> in one
    /// sample window.
    /// </summary>
    ProviderFailures = 2,

    /// <summary>
    /// WAL flush latency stayed at or above
    /// <see cref="LatticeOptions.WalSaturationFlushLatencyThreshold"/> for
    /// <see cref="LatticeOptions.WalSaturationFlushLatencySampleWindows"/>
    /// consecutive windows.
    /// </summary>
    FlushLatency = 3,

    /// <summary>
    /// The per-(tree, partition) admission semaphore was at the
    /// <see cref="LatticeOptions.WalMaxPendingBatches"/> cap with callers parked
    /// on it, or its depth reached
    /// <see cref="LatticeOptions.WalSaturationThrottledRatio"/> of that cap.
    /// </summary>
    AdmissionDepth = 4,

    /// <summary>
    /// The in-memory materialiser drain lag stayed at or above
    /// <see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/> for
    /// <see cref="LatticeOptions.WalSaturationMaterialiserLagSampleWindows"/>
    /// consecutive windows.
    /// </summary>
    MaterialiserDrainLag = 5,

    /// <summary>
    /// Durable materialiser-pin writes stayed at or above
    /// <see cref="LatticeOptions.WalSaturationMaterialiserPinLatencyThreshold"/>
    /// for
    /// <see cref="LatticeOptions.WalSaturationMaterialiserPinLatencySampleWindows"/>
    /// consecutive windows.
    /// <para>
    /// This is the only input that observes the <b>durable</b> WAL retention
    /// floor rather than in-memory progress. A stalled pin store leaves the
    /// floor pinned even while every in-memory cursor keeps advancing, so
    /// without this input the signal reads healthy while the WAL grows without
    /// bound (issue #2015).
    /// </para>
    /// </summary>
    MaterialiserPinLatency = 6,
}
