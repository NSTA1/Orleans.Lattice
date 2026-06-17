using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// View-side coordinator for a cross-tree atomic write's joint visibility flip.
/// One activation per cross-tree <c>operationId</c> (this grain's string key),
/// the analogue of the receiver-side <see cref="Orleans.Lattice.BPlusTree.ILatticeCrossTreeReceiverGrain"/>
/// but flipping <b>view</b> trees rather than source trees.
/// <para>
/// <b>Why this exists.</b> When a source-side cross-tree atomic write commits
/// across multiple source trees, the materialised views derived from those
/// source trees must flip jointly, all-or-nothing: a reader must never observe
/// one participating view's slice of the cross-tree batch committed while
/// another participating view's slice is still pre-commit. Each participating
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
[Alias(ReplicationTypeAliases.IViewCrossTreeCoordinatorGrain)]
internal interface IViewCrossTreeCoordinatorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records one participating view's ready slice. The first call freezes the
    /// wait set; later calls must carry an identical wait set. Idempotent and
    /// durable: the coordinator persists its state before returning, and once the
    /// wait set completes it issues the joint cross-tree flip (idempotently keyed
    /// so a redelivery re-attaches rather than double-applying) and persists the
    /// decision. Returns a <see cref="ViewCrossTreeDecision"/> whose
    /// <see cref="ViewCrossTreeDecision.Applied"/> is <c>true</c> once the joint
    /// flip has durably committed (the caller then applies its own retraction
    /// deletes and clears its staged batch), or <c>false</c> while the wait set is
    /// still incomplete (the caller keeps its batch staged and re-registers on a
    /// later drain, or degrades once its readiness timeout elapses).
    /// </summary>
    Task<ViewCrossTreeDecision> RegisterReadyAsync(ViewCrossTreeReadiness readiness);
}
