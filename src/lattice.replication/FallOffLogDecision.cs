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
public readonly record struct FallOffLogDecision(
    bool FellOffLog,
    HybridLogicalClock LocalHighWaterMark,
    bool BootstrapTriggered);