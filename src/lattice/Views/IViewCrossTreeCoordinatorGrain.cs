
namespace Orleans.Lattice.Views;

/// <summary>
/// View-side coordinator for a cross-tree atomic write's joint visibility flip.
/// One activation per cross-tree <c>operationId</c> (this grain's string key),
/// the analogue of the receiver-side <see cref="Orleans.Lattice.BPlusTree.ILatticeCrossTreeReceiverGrain"/>
/// but flipping <b>view</b> trees rather than source trees.
/// <para>
/// <b>Why this exists.</b> When a source-side cross-tree atomic write commits
/// across multiple source trees, the materialised views derived from those
/// source trees must publish jointly, all-or-nothing, at the joint-write
/// boundary. This does not provide a shared multi-view snapshot cursor:
/// snapshot cursors are single-tree, and independent reads or opens can straddle
/// a valid joint commit. It adds no multi-source join-view or view-DAG contract.
/// Each participating
/// view is an ordinary <see cref="ILattice"/> tree maintained by its own
/// (distinct) maintainer grain, so no bespoke barrier is needed - this
/// coordinator rendezvouses the per-view slices and reuses the existing
/// cross-tree atomic-write primitive
/// (<see cref="LatticeCrossTreeAtomicWriteExtensions.SetManyAtomicAsync"/>) to
/// flip every participating view tree in a single cross-tree saga.
/// </para>
/// <para>
/// <b>Wait set.</b> The wait set is the set of view names whose source tree is
/// one of the cross-tree batch's participants <i>and</i> that has a configured
/// view (the <c>participants ∩ present</c> handling that mirrors the receiver's
/// partial-replication case). It is frozen on the first registration; later
/// registrations must carry an identical wait set or are rejected.
/// </para>
/// <para>
/// <b>Deadlock-freedom.</b> The only grain
/// <see cref="RegisterReadyAsync"/> calls into is the cross-tree atomic-write
/// coordinator (<see cref="Orleans.Lattice.BPlusTree.ILatticeCrossTreeTxGrain"/>),
/// which writes the view trees - never back into a view maintainer. Each
/// maintainer resolves its own active view tree id locally and passes it in its
/// registration, so the coordinator has everything it needs to issue the joint
/// flip without a call back into the registering grain.
/// </para>
/// </summary>
[Alias(TypeAliases.IViewCrossTreeCoordinatorGrain)]
internal interface IViewCrossTreeCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the durably admitted protocol without registration or mutation:
    /// null before admission or after cleanup, zero for a populated released
    /// operation, and the current version for a current operation. A failed
    /// first admission write does not report an in-memory marker as durable.
    /// Call before minting a new hold so legacy progress does not depend on the
    /// cluster's new-protocol admission gate. Unsupported versions are rejected.
    /// </summary>
    Task<int?> GetRepairProtocolVersionAsync();

    /// <summary>
    /// Records one participating view's ready slice. The first call freezes the
    /// wait set; later calls must carry an identical wait set. Idempotent and
    /// durable: the coordinator persists its state before returning, and once the
    /// wait set completes it issues the joint cross-tree flip (idempotently keyed
    /// so a redelivery re-attaches rather than double-applying) and persists the
    /// decision. The caller must persist its original readiness and source/view
    /// physical-identity hold before submission and explicitly identify the
    /// current protocol in <see cref="ViewCrossTreeReadiness.RepairProtocolVersion"/>.
    /// Returns a <see cref="ViewCrossTreeDecision"/> whose
    /// <see cref="ViewCrossTreeDecision.Applied"/> is <c>true</c> once the joint
    /// flip has durably committed. When
    /// <see cref="ViewCrossTreeDecision.RequiresRepair"/> is true, the caller
    /// retains its staged batch and checkpoint/physical-identity holds through
    /// mandatory fresh repair and durable retirement. Persisted released
    /// operations instead complete under their original contract: they return
    /// Applied without RequiresRepair, retire normally and expire automatically,
    /// never certifying a fresh repair that their original callers did not owe.
    /// Returns <c>false</c> while the wait set is incomplete: the caller keeps its
    /// batch staged and re-registers on a later drain. A readiness timeout may
    /// degrade only before a durable joint intent has been decided. A current
    /// operation also waits for explicit current-protocol readiness from every
    /// participant; an older caller cannot supply a durable repair hold.
    /// </summary>
    Task<ViewCrossTreeDecision> RegisterReadyAsync(ViewCrossTreeReadiness readiness);

    /// <summary>
    /// Registers a freshly recomputed slice for the current mandatory joint repair round
    /// following the original committed publication. Never reuses the original
    /// decision's payload or operation id. Every original participant must join,
    /// including empty slices, before retiring its staged source batch. A caller
    /// must retain its source checkpoint and fence all ordinary/local publication
    /// until this round commits. Physical rebind, rebuild and eviction remain
    /// fenced until durable checkpoint retirement.
    /// The slice must include admitted newer source replacements, not just the
    /// original prepares or an unverified copy of current view rows.
    /// A committed retry only confirms the durable result and never republishes
    /// a frozen slice over a subsequently advanced peer.
    /// Unlike the original readiness timeout, a repair cannot degrade to
    /// per-tree publication: it retains checkpoint holds while a peer is
    /// unavailable. Eventual repair requires compatible peer recovery and fair
    /// maintenance. Legacy operations cannot enter or certify this round.
    /// </summary>
    Task<ViewCrossTreeDecision> RegisterRepairReadyAsync(ViewCrossTreeReadiness readiness);

    /// <summary>
    /// Returns this participant's frozen repair contribution, or <c>null</c>
    /// before its first submission. Resubmit any returned contribution through
    /// <see cref="RegisterRepairReadyAsync"/>: a failed persistence attempt may
    /// leave frozen in-memory intent that registration must durably rewrite.
    /// Reactivation must resume the exact physical target and payload. Publication
    /// remains fenced until repair commits; physical identity remains pinned until
    /// durable checkpoint retirement.
    /// </summary>
    Task<ViewCrossTreeReadiness?> GetRepairReadinessAsync(string viewName);

    /// <summary>
    /// Records that a participant has durably retired the repaired source batch
    /// from its checkpoint. Call only after checkpoint persistence, retaining a
    /// durable pending acknowledgement until this call succeeds. Retention
    /// cleanup is permitted only after every original participant acknowledges.
    /// </summary>
    Task AcknowledgeRepairAsync(string viewName);

    /// <summary>
    /// Records that a participant view has timed out waiting for the joint flip
    /// and is degrading to per-tree-slice atomicity. Terminally degrades the
    /// operation so the coordinator never issues the joint flip, unless a joint
    /// intent has already been decided - in which case it resumes that frozen
    /// publication and returns
    /// an applied decision. Current-protocol callers retain their staged batch
    /// and enter fresh repair when RequiresRepair is true; legacy operations
    /// finish under their original contract. Neither issues a per-tree fallback
    /// after durable joint intent. Otherwise returns
    /// <see cref="ViewCrossTreeDecision.Degraded"/>. Idempotent and durable.
    /// </summary>
    Task<ViewCrossTreeDecision> RegisterDegradedAsync(string viewName);
}
