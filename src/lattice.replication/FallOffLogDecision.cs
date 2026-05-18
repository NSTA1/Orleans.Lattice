using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Result of a single
/// <see cref="ILatticeFallOffLogDetector.CheckAndTriggerAsync"/>
/// call. Diagnostic only; the detector's durable side effect is the
/// optional auto-bootstrap kickoff and the
/// <see cref="LatticeReplicationMetrics.PeerFellOffLog"/> metric
/// increment.
/// </summary>
/// <param name="FellOffLog">
/// <see langword="true"/> when the receiver's per-origin
/// high-water-mark for <c>(treeName, sourceClusterId)</c> was strictly
/// less than the sender's oldest available WAL entry HLC at the time
/// of the check, indicating the receiver has fallen off the sender's
/// log and cannot resume incremental replication without a fresh
/// snapshot.
/// </param>
/// <param name="LocalHighWaterMark">
/// The receiver's per-origin high-water-mark observed at decision
/// time. Equal to <see cref="HybridLogicalClock.Zero"/> when the
/// receiver has never applied an entry from
/// <c>sourceClusterId</c>.
/// </param>
/// <param name="BootstrapTriggered">
/// <see langword="true"/> when the detector invoked
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> as a
/// result of this check. Always <see langword="false"/> when
/// <see cref="FellOffLog"/> is <see langword="false"/>; also
/// <see langword="false"/> when
/// <see cref="LatticeReplicationOptions.AutoBootstrapOnFallOffLog"/>
/// is disabled (the metric increment still fires, but the operator
/// is expected to drive
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/>
/// manually).
/// </param>
/// <param name="Suppressed">
/// <see langword="true"/> when the detector observed a fall-off
/// condition but the bootstrap coordinator was already in flight from
/// the same <c>sourceClusterId</c> (one of
/// <see cref="LatticeBootstrapState.RequestingSnapshot"/>,
/// <see cref="LatticeBootstrapState.ApplyingSnapshot"/>, or
/// <see cref="LatticeBootstrapState.IncrementalHandoff"/>). In that
/// case the detector treats this probe as a no-op for alerting
/// purposes: <see cref="LatticeReplicationMetrics.PeerFellOffLog"/>
/// is not incremented, the warning log is downgraded to debug
/// verbosity, and
/// <see cref="LatticeReplicationMetrics.PeerFellOffLogSuppressed"/>
/// is incremented instead so operators can distinguish "detector did
/// not fire" from "detector fired and the coordinator was already
/// handling it". Always <see langword="false"/> when
/// <see cref="FellOffLog"/> is <see langword="false"/>.
/// <see cref="BootstrapTriggered"/> is reported as
/// <see langword="true"/> alongside <see cref="Suppressed"/> because
/// the coordinator's in-progress drain is, semantically, the
/// triggered bootstrap.
/// </param>
public readonly record struct FallOffLogDecision(
    bool FellOffLog,
    HybridLogicalClock LocalHighWaterMark,
    bool BootstrapTriggered,
    bool Suppressed);