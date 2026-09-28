namespace Orleans.Lattice;

/// <summary>
/// Which of the sampler's inputs drove a
/// <see cref="WalSaturationStateChange"/>. Several independent inputs map to the
/// same <see cref="WalSaturationState"/>, so the state alone does not say what a
/// host should look at: <see cref="AdmissionDepth"/>,
/// <see cref="MaterialiserDrainLag"/> and <see cref="MaterialiserPinLatency"/>
/// all raise <see cref="WalSaturationState.Throttled"/>, while
/// <see cref="DispatchTimeouts"/>, <see cref="ProviderFailures"/> and
/// <see cref="FlushLatency"/> raise <see cref="WalSaturationState.Saturated"/>,
/// as does <see cref="AdmissionDepth"/> when
/// <see cref="LatticeOptions.WalSaturationAcuteOnly"/> is disabled. This
/// discriminator names the one the sampler attributed the transition to, so an
/// observer can route an alert at the subsystem actually under pressure.
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
    /// back to <see cref="WalSaturationState.Healthy"/>, on a transition to
    /// <see cref="WalSaturationState.Throttled"/> that only the
    /// <see cref="LatticeOptions.WalSaturationRecoveryWindow"/> hold produced,
    /// and on any transition published by a host predating cause attribution.
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
    /// <see cref="LatticeOptions.WalSaturationProviderFailureRateThreshold"/> in one
    /// sample window.
    /// </summary>
    ProviderFailures = 2,

    /// <summary>
    /// At least one WAL flush met or exceeded
    /// <see cref="LatticeOptions.WalSaturationFlushLatencyThreshold"/> in each of
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
    /// The materialiser drain lag - how far the WAL head has run ahead of the
    /// slowest fresh consumer cursor in the tree's in-memory WAL cursor
    /// registry, across leaf materialisers and tree-wide tailers such as view
    /// maintainers, WAL subscribers and replication shippers - stayed above
    /// <see cref="LatticeOptions.WalSaturationMaterialiserLagThreshold"/> for
    /// <see cref="LatticeOptions.WalSaturationMaterialiserLagSampleWindows"/>
    /// consecutive windows.
    /// </summary>
    MaterialiserDrainLag = 5,

    /// <summary>
    /// At least one durable materialiser-pin write faulted, or met or exceeded
    /// <see cref="LatticeOptions.WalSaturationMaterialiserPinLatencyThreshold"/>,
    /// in each of
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
