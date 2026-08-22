using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the atomic-commit safety and
/// liveness <b>property catalogue</b> (level-C Phase 6, #1595). Each guard test
/// removes exactly one fix so Coyote re-finds the corresponding invariant
/// violation, proving the safety assertions in
/// <see cref="AtomicCommitInvariantModel"/> are non-vacuous. They are tagged
/// <c>[Category("Coyote")]</c> so the fast dev loop and the per-package
/// deterministic CI step skip them; a dedicated CI step runs this category. See
/// the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class AtomicCommitInvariantCoyoteTests
{
    /// <summary>
    /// The fix: driving a full single-saga lifecycle - the tree-wide decision, the
    /// per-leaf terminal broadcast, duplicate terminal re-deliveries classified by
    /// the write-once guard, and interleaved reader probes - through the production
    /// cores admits no schedule that violates strict isolation,
    /// visibility-matches-decision, linearized terminals, no-mixed-terminals,
    /// decision durability, monotonic visibility, or revision monotonicity, for
    /// both the commit and the abort outcome across a range of fan-out widths.
    /// </summary>
    [Test]
    public void Full_saga_lifecycle_upholds_every_catalogued_invariant(
        [Values(2, 3, 4)] int leafCount,
        [Values(AtomicCommitInvariantScenario.Commit, AtomicCommitInvariantScenario.Abort)]
        AtomicCommitInvariantScenario scenario)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new AtomicCommitInvariantModel(leafCount, scenario, AtomicCommitInvariantGuard.None));
    }

    /// <summary>
    /// The guard for strict isolation / visibility-matches-decision: surfacing an
    /// in-flight saga's prepared value at the read gate lets a reader observe a
    /// post-saga value with no committed decision, and Coyote must find it. This
    /// proves the strict-default assertion is load-bearing.
    /// </summary>
    [Test]
    public void Surfacing_in_flight_as_prepared_violates_strict_isolation(
        [Values(2, 3)] int leafCount,
        [Values(AtomicCommitInvariantScenario.Commit, AtomicCommitInvariantScenario.Abort)]
        AtomicCommitInvariantScenario scenario)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitInvariantModel(leafCount, scenario, AtomicCommitInvariantGuard.SurfaceInFlightAsPrepared));
    }

    /// <summary>
    /// The guard for linearized terminals: letting a leaf apply its terminal before
    /// the registry records the decision produces a terminal that precedes its
    /// decision, and Coyote must find it. This proves the decision-before-broadcast
    /// assertion is load-bearing.
    /// </summary>
    [Test]
    public void Broadcasting_before_the_decision_violates_terminal_linearization(
        [Values(2, 3)] int leafCount,
        [Values(AtomicCommitInvariantScenario.Commit, AtomicCommitInvariantScenario.Abort)]
        AtomicCommitInvariantScenario scenario)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitInvariantModel(leafCount, scenario, AtomicCommitInvariantGuard.BroadcastBeforeDecision));
    }

    /// <summary>
    /// The guard for no-mixed-terminals: deriving each leaf's terminal kind from an
    /// independent coin instead of the single recorded decision lets one leaf commit
    /// while a sibling aborts the same saga, and Coyote must find it. This proves the
    /// mixed-terminal assertion is load-bearing.
    /// </summary>
    [Test]
    public void Independent_per_leaf_terminals_violate_no_mixed_terminals(
        [Values(2, 3)] int leafCount,
        [Values(AtomicCommitInvariantScenario.Commit, AtomicCommitInvariantScenario.Abort)]
        AtomicCommitInvariantScenario scenario)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitInvariantModel(leafCount, scenario, AtomicCommitInvariantGuard.MixedBroadcast));
    }

    /// <summary>
    /// The guard for decision durability: letting a duplicate terminal delivery
    /// bypass the write-once classification and flip the recorded decision makes a
    /// committed decision an earlier read relied on become aborted, and Coyote must
    /// find it. This proves the write-once durability assertion is load-bearing as an
    /// interleaving property, beyond the serialized-registry unit tests.
    /// </summary>
    [Test]
    public void Flipping_a_recorded_decision_violates_decision_durability(
        [Values(2, 3)] int leafCount,
        [Values(AtomicCommitInvariantScenario.Commit, AtomicCommitInvariantScenario.Abort)]
        AtomicCommitInvariantScenario scenario)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitInvariantModel(leafCount, scenario, AtomicCommitInvariantGuard.FlipDecision));
    }

    /// <summary>
    /// The guard for revision monotonicity: an unpaired rollback that lowers the
    /// registry revision counter regresses the monotonic version the reader-side
    /// stability probe relies on, and Coyote must find it. This proves the
    /// revision-monotonic assertion is load-bearing.
    /// </summary>
    [Test]
    public void Lowering_the_revision_counter_violates_revision_monotonicity(
        [Values(2, 3)] int leafCount,
        [Values(AtomicCommitInvariantScenario.Commit, AtomicCommitInvariantScenario.Abort)]
        AtomicCommitInvariantScenario scenario)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitInvariantModel(leafCount, scenario, AtomicCommitInvariantGuard.DecrementRevision));
    }
}
