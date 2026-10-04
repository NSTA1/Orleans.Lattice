using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote tests for <see cref="ResizeFenceModel"/>: an online resize's
/// fence-before-flip swap (#4362), the bound saga's admission through the fence
/// (#4369), and the lift after a failed flip, all decided by the real
/// <c>ResizeFence</c> core. Tagged <c>[Category("Coyote")]</c>; see the "Coyote
/// concurrency tier" section of <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// Each guard removes one fix and must be found. Each guard also has a
/// specificity test that disables exactly the assertion the guard targets and
/// requires no violation, so a guard cannot pass on another assertion's account.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class ResizeFenceCoyoteTests
{
    /// <summary>
    /// The shipping design: the bound batch lands on every shard of the old copy,
    /// and no router whose cached pair names the old copy is served by it once the
    /// resized copy has taken a write.
    /// </summary>
    [Test]
    public void Fence_before_flip_keeps_the_bound_batch_whole_and_never_serves_the_old_copy([Values(2, 3)] int shardCount)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new ResizeFenceModel(shardCount, ResizeFenceGuard.None));
    }

    /// <summary>
    /// Before #4376 the fence refused the saga bound to the old copy, so a batch
    /// dispatched across the fence landed on part of the old copy.
    /// </summary>
    [Test]
    public void A_fence_that_refuses_the_bound_saga_leaves_a_partial_batch_on_the_old_copy()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.BoundSagaRefusedByFence));
    }

    /// <summary>Specificity: the partial batch is caught only by the batch-whole assertion.</summary>
    [Test]
    public void A_fence_that_refuses_the_bound_saga_is_caught_only_by_the_batch_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.BoundSagaRefusedByFence, ResizeFenceAssertions.NoStaleReadAfterFlip));
    }

    /// <summary>
    /// Lifting the fence after a flip that failed in transport but landed leaves
    /// the old copy serving while the alias names the resized copy.
    /// </summary>
    [Test]
    public void Lifting_the_fence_after_a_flip_that_landed_serves_the_old_copy()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.LiftIgnoresRegistry));
    }

    /// <summary>Specificity: the stale read is caught only by the stale-read assertion.</summary>
    [Test]
    public void Lifting_the_fence_after_a_landed_flip_is_caught_only_by_the_stale_read_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.LiftIgnoresRegistry, ResizeFenceAssertions.BatchWholeOnOldCopy));
    }

    /// <summary>Flipping before fencing (#4362) serves the old copy after the resized copy took a write.</summary>
    [Test]
    public void Flipping_before_fencing_serves_the_old_copy()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.FlipBeforeFence));
    }

    /// <summary>Specificity: the flip-before-fence read is caught only by the stale-read assertion.</summary>
    [Test]
    public void Flipping_before_fencing_is_caught_only_by_the_stale_read_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.FlipBeforeFence, ResizeFenceAssertions.BatchWholeOnOldCopy));
    }

    /// <summary>
    /// A fence whose refusal arm admits a stale router's call serves the old copy
    /// after the resized copy took a write: the core's refusal is what the
    /// stale-read assertion checks (shard-ownership review #4435, finding F5).
    /// </summary>
    [Test]
    public void A_fence_that_admits_a_stale_routed_call_serves_the_old_copy()
    {
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.FenceAdmitsAStaleCall));
    }

    /// <summary>Specificity: the over-admitting fence is caught only by the stale-read assertion.</summary>
    [Test]
    public void A_fence_that_admits_a_stale_routed_call_is_caught_only_by_the_stale_read_assertion()
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new ResizeFenceModel(2, ResizeFenceGuard.FenceAdmitsAStaleCall, ResizeFenceAssertions.BatchWholeOnOldCopy));
    }
}
