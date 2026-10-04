using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote tests for <see cref="SagaCopyBindingModel"/>: an atomic-write
/// saga's binding to a physical copy across an alias swap, decided by the real
/// <c>SagaCopyBinding</c> core. Tagged <c>[Category("Coyote")]</c>; see the "Coyote
/// concurrency tier" section of <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class SagaCopyBindingCoyoteTests
{
    /// <summary>
    /// The shipping design, dispatching each batch in one routing-tier call: the
    /// saga decides with its whole batch on the bound copy and nothing off it,
    /// across a resize flip and across an undo. The mid-dispatch partial failure is
    /// pinned off here because it is the open defect #4454, characterised below.
    /// </summary>
    [Test]
    public void The_batch_is_decided_whole_on_the_bound_copy_across_a_swap(
        [Values(2, 3)] int keyCount,
        [Values] SagaCopyBindingSwap swap)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SagaCopyBindingModel(keyCount, swap, SagaCopyBindingGuard.None));
    }

    /// <summary>
    /// A routing tier that ignores the binding (#4358) places a re-bound saga's
    /// batch on the copy an undo discards, so the copy it decides on lacks it.
    /// </summary>
    [Test]
    public void A_router_that_ignores_the_binding_leaves_the_bound_copy_without_the_batch()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SagaCopyBindingModel(2, SagaCopyBindingSwap.UndoSwap, SagaCopyBindingGuard.RouterIgnoresBinding));
    }

    /// <summary>Specificity: that guard is caught only by the batch-on-bound-copy assertion.</summary>
    [Test]
    public void A_router_that_ignores_the_binding_is_caught_only_by_the_bound_copy_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SagaCopyBindingModel(
                2, SagaCopyBindingSwap.UndoSwap, SagaCopyBindingGuard.RouterIgnoresBinding,
                assertions: SagaCopyBindingAssertions.NoBucketOffTheBoundCopy));
    }

    /// <summary>
    /// A pre-decision check that ignores the mirror (#4369 before #4376) re-binds
    /// to the resized copy and strands the whole batch on the old one.
    /// </summary>
    [Test]
    public void A_pre_decision_check_that_ignores_the_mirror_strands_the_batch_on_the_old_copy()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SagaCopyBindingModel(2, SagaCopyBindingSwap.ResizeFlip, SagaCopyBindingGuard.PreDecisionIgnoresMirror));
    }

    /// <summary>Specificity: that guard is caught only by the no-bucket-off-the-bound-copy assertion.</summary>
    [Test]
    public void A_pre_decision_check_that_ignores_the_mirror_is_caught_only_by_the_off_copy_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SagaCopyBindingModel(
                2, SagaCopyBindingSwap.ResizeFlip, SagaCopyBindingGuard.PreDecisionIgnoresMirror,
                assertions: SagaCopyBindingAssertions.BatchOnBoundCopy));
    }

    /// <summary>
    /// A pre-decision check that stays bound whenever the tree moved lets a saga
    /// bound to the resized copy decide there after an undo swapped back, so its
    /// acknowledged commit is discarded with that copy (shard-ownership review
    /// #4435, finding F6).
    /// </summary>
    [Test]
    public void A_pre_decision_check_that_always_stays_bound_decides_on_a_copy_the_undo_discards()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SagaCopyBindingModel(2, SagaCopyBindingSwap.UndoSwap, SagaCopyBindingGuard.PreDecisionAlwaysStaysBound));
    }

    /// <summary>Specificity: that guard is caught only by the bound-copy-live assertion.</summary>
    [Test]
    public void A_pre_decision_check_that_always_stays_bound_is_caught_only_by_the_live_copy_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SagaCopyBindingModel(
                2, SagaCopyBindingSwap.UndoSwap, SagaCopyBindingGuard.PreDecisionAlwaysStaysBound,
                assertions: SagaCopyBindingAssertions.All & ~SagaCopyBindingAssertions.BoundCopyLive));
    }

    /// <summary>
    /// A refused dispatch that never re-binds the saga leaves it retrying the same
    /// refused copy for ever (shard-ownership review #4435, nit N4).
    /// </summary>
    [Test]
    public void A_refusal_that_never_rebinds_stops_the_saga_making_progress()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SagaCopyBindingModel(2, SagaCopyBindingSwap.UndoSwap, SagaCopyBindingGuard.RefusalNeverRebinds));
    }

    /// <summary>Specificity: that guard is caught only by the progress assertion.</summary>
    [Test]
    public void A_refusal_that_never_rebinds_is_caught_only_by_the_progress_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SagaCopyBindingModel(
                2, SagaCopyBindingSwap.UndoSwap, SagaCopyBindingGuard.RefusalNeverRebinds,
                assertions: SagaCopyBindingAssertions.All & ~SagaCopyBindingAssertions.RefusalMakesProgress));
    }
    /// <summary>
    /// Characterises the open defect #4454 against the shipping core: when a
    /// transient shard failure stops a dispatch part way and the tree then moves,
    /// the mid-dispatch re-bind (<c>SagaCopyBinding.RebindsAfterRefusal</c>) applies
    /// no mirror check and leaves the prepares already taken on the old copy. When
    /// #4454 is fixed this test must fail, and becomes the fixed-design assertion.
    /// </summary>
    [Test]
    public void Mid_dispatch_rebind_strands_partial_prepares_issue_4454()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SagaCopyBindingModel(2, SagaCopyBindingSwap.ResizeFlip, SagaCopyBindingGuard.None, partialDispatch: true));
    }
}
