using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side fall-off-the-log detector. The inbound apply path
/// (or the transport layer that owns the apply path) calls
/// <see cref="CheckAndTriggerAsync"/> when it has acquired the
/// sender's oldest available WAL entry HLC for a given tree. The
/// detector compares that HLC against the receiver's per-origin
/// high-water-mark; when the local HWM is strictly older the receiver
/// has fallen off the sender's log and cannot resume incremental
/// replication without a fresh snapshot. The detector then emits the
/// <see cref="LatticeReplicationMetrics.PeerFellOffLog"/> metric and,
/// when
/// <see cref="LatticeReplicationOptions.AutoBootstrapOnFallOffLog"/>
/// is enabled (the default), kicks off
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> for the
/// affected tree.
/// <para>
/// The sender's oldest-available HLC is supplied by the caller rather
/// than fetched by the detector itself: the detector lives on the
/// receiver side and has no inherent path to the remote WAL. A
/// future transport revision will plumb the sender's oldest HLC
/// through the batch envelope so each inbound apply naturally
/// populates the parameter; until then, callers can use
/// <see cref="ILatticeWalIntrospection"/> in a co-located test
/// fixture to introspect the sender's WAL directly.
/// </para>
/// <para>
/// The coordinator's idempotency contract handles concurrent
/// detection cleanly: when a bootstrap is already in flight from the
/// same source cluster, the kickoff is a no-op; from a different
/// source cluster, the kickoff throws (and the exception propagates
/// out of <see cref="CheckAndTriggerAsync"/> verbatim); when the
/// bootstrap is in a terminal state
/// (<see cref="LatticeBootstrapState.LiveIncremental"/> or
/// <see cref="LatticeBootstrapState.Failed"/>), the kickoff starts a
/// fresh cycle.
/// </para>
/// </summary>
public interface ILatticeFallOffLogDetector
{
    /// <summary>
    /// Runs the fall-off-the-log check for
    /// <paramref name="treeName"/> against the sender identified by
    /// <paramref name="sourceClusterId"/>, and (when configured)
    /// triggers
    /// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> on
    /// detection. Idempotent — re-issuing the call with the same
    /// arguments while a bootstrap is in flight is a no-op at the
    /// coordinator and observable here as
    /// <see cref="FallOffLogDecision.BootstrapTriggered"/> on every
    /// call.
    /// </summary>
    /// <param name="treeName">
    /// Logical tree id. Must be non-null and non-empty.
    /// </param>
    /// <param name="sourceClusterId">
    /// Origin cluster id of the lagging sender. Must be non-null and
    /// non-empty. Stamped onto every snapshot entry the bootstrap
    /// coordinator subsequently applies, so the per-origin
    /// high-water-mark dedupe recognises the snapshot/incremental
    /// boundary.
    /// </param>
    /// <param name="senderOldestAvailableHlc">
    /// The sender's oldest still-available WAL entry HLC. The
    /// detector compares this value against the receiver's per-origin
    /// high-water-mark; lag is detected when the local HWM is
    /// strictly less than this value.
    /// </param>
    /// <param name="cancellationToken">Cancellation token observed at every grain hop.</param>
    /// <returns>
    /// A <see cref="FallOffLogDecision"/> describing the outcome.
    /// </returns>
    Task<FallOffLogDecision> CheckAndTriggerAsync(
        string treeName,
        string sourceClusterId,
        HybridLogicalClock senderOldestAvailableHlc,
        CancellationToken cancellationToken = default);
}