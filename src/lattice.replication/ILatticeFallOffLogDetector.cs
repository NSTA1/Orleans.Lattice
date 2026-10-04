using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver-side fall-off-the-log detector. The maintenance path calls
/// <see cref="CheckAndTriggerAsync"/> with the oldest retained local
/// WAL entry that was authored by a given origin. The detector compares
/// that local reading against the receiver's per-origin high-water-mark;
/// when the local HWM is strictly older the receiver has fallen off its
/// own local log for that origin and cannot safely use that local stream
/// without a fresh snapshot. The detector then emits the
/// <see cref="LatticeReplicationMetrics.PeerFellOffLog"/> metric and,
/// when
/// <see cref="LatticeReplicationOptions.AutoBootstrapOnFallOffLog"/>
/// is enabled (the default), kicks off
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> for the
/// affected tree.
/// <para>
/// This is not the cross-cluster source-WAL trim detector. A sender that
/// trims records before its shipper reads them observes the sequence gap
/// in <c>IWalShardGrain.ReadShippingAsync</c>,
/// records <see cref="ReplicationBatch.ReseedAfterEpoch"/>, and asks the
/// receiver to re-seed on subsequent pushes. A transport that does not
/// carry that batch field cannot deliver the request; the sender then
/// keeps saga records withheld and the link remains stalled until an
/// operator re-seeds or the transport is fixed.
/// </para>
/// <para>
/// The coordinator's idempotency contract handles concurrent
/// detection cleanly: when a bootstrap is already in flight from the
/// same source cluster, the detector consults
/// <see cref="ILatticeBootstrapCoordinator.GetStatusAsync"/> first
/// and suppresses the probe - the kickoff is skipped, the
/// <see cref="LatticeReplicationMetrics.PeerFellOffLog"/> counter is
/// not bumped, the warning is downgraded to debug, and
/// <see cref="LatticeReplicationMetrics.PeerFellOffLogSuppressed"/>
/// is incremented instead so operators can still observe the
/// absorbed probes. From a different source cluster, the kickoff
/// throws (and the exception propagates out of
/// <see cref="CheckAndTriggerAsync"/> verbatim); when the bootstrap
/// is in a terminal state
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
    /// detection. Idempotent - re-issuing the call with the same
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
    /// The oldest retained WAL entry HLC the caller is comparing
    /// against. The built-in maintenance caller supplies the receiver's
    /// local oldest entry for <paramref name="sourceClusterId"/>, not the
    /// remote sender's WAL floor. Lag is detected when the receiver's
    /// local HWM is strictly less than this value.
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