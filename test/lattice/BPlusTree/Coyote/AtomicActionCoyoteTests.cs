using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the atomic-action saga's
/// step-sequencing and crash-resume core
/// (<see cref="Orleans.Lattice.BPlusTree.AtomicActionPlanCore"/>, issue #1609).
/// They are tagged <c>[Category("Coyote")]</c> so the fast dev loop and the
/// per-package deterministic CI step skip them; a dedicated CI step runs this
/// category. See the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class AtomicActionCoyoteTests
{
    /// <summary>
    /// The fix: the proven core compensates committed steps in strict reverse
    /// order, each exactly once, on every explored interleaving of crash-before-
    /// persist faults - and never commits a partial forward set nor settles the
    /// compensated terminal with an un-compensated step.
    /// </summary>
    [Test]
    public void Compensation_runs_in_reverse_order_exactly_once_on_any_order()
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new AtomicActionExecutionModel(useBrokenReverseOrder: false));
    }

    /// <summary>
    /// The guard: compensating in forward (lowest-index-first) order reintroduces
    /// the ordering regression, and Coyote must find a schedule whose compensation
    /// indices increase and trip the reverse-order assertion. This proves the model
    /// genuinely exercises the ordering rule, so the passing test above is
    /// meaningful rather than vacuous.
    /// </summary>
    [Test]
    public void Compensating_in_forward_order_is_caught()
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicActionExecutionModel(useBrokenReverseOrder: true));
    }
}
