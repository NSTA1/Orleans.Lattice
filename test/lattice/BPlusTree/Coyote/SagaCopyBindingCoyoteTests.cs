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
    /// across a resize flip and across an undo. A dispatch a transient failure
    /// stops part way is covered below.
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
    /// The shipping design, with a transient shard failure able to stop a dispatch
    /// part way and the tree then flipping to the resized copy: the rest of the
    /// batch still lands on the bound copy, which mirrors it, so nothing is left
    /// behind on a copy a resize undo re-exposes (#4454).
    /// </summary>
    [Test]
    public void A_dispatch_stopped_part_way_across_a_resize_flip_keeps_the_batch_on_the_bound_copy(
        [Values(2, 3)] int keyCount,
        [Values] SagaCopyBindingSwap swap)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SagaCopyBindingModel(keyCount, swap, SagaCopyBindingGuard.None, partialDispatch: true));
    }

    /// <summary>
    /// #4454 before its fix: the routing tier refuses the rest of a partly
    /// dispatched batch and the execute phase re-binds, both without asking where
    /// the bound copy mirrors, so the prepares already taken stay on the old copy.
    /// </summary>
    [Test]
    public void A_mid_dispatch_rebind_that_ignores_the_mirror_strands_partial_prepares()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new SagaCopyBindingModel(2, SagaCopyBindingSwap.ResizeFlip, SagaCopyBindingGuard.RebindIgnoresMirror, partialDispatch: true));
    }

    /// <summary>Specificity: that guard is caught only by the no-bucket-off-the-bound-copy assertion.</summary>
    [Test]
    public void A_mid_dispatch_rebind_that_ignores_the_mirror_is_caught_only_by_the_off_copy_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new SagaCopyBindingModel(
                2, SagaCopyBindingSwap.ResizeFlip, SagaCopyBindingGuard.RebindIgnoresMirror, partialDispatch: true,
                assertions: SagaCopyBindingAssertions.BatchOnBoundCopy));
    }
}
