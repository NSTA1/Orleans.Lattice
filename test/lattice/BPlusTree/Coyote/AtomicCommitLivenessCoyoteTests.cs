using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote systematic-concurrency tests for the atomic-commit protocol's
/// <b>liveness</b> (progress under fault injection), the level-C Phase 4 model
/// (issue #1592, building on the Phase 1-3 safety models). They are tagged
/// <c>[Category("Coyote")]</c> so the fast dev loop and the per-package
/// deterministic CI step skip them; a dedicated CI step runs this category. See
/// the "Coyote concurrency tier" section of
/// <c>.github/instructions/testing.instructions.md</c>.
/// <para>
/// Each safety model asked "can a bad interleaving be certified?"; this fixture
/// asks "can the protocol get stuck?" under a bounded fault model (terminal drops,
/// duplicates, reorderings, and participant restarts). The fix drives the real
/// production cores (<see cref="Orleans.Lattice.BPlusTree.SagaCoordinatorCore"/>,
/// <see cref="Orleans.Lattice.BPlusTree.TxRegistryDecisionCore"/>,
/// <see cref="Orleans.Lattice.BPlusTree.MigrationTerminalCore"/>,
/// <see cref="Orleans.Lattice.BPlusTree.AtomicVisibilityGate"/>) and a
/// durable-registry backstop guarantees progress on every schedule; the guard
/// tests remove the backstop and prove Coyote re-finds the stalled schedule, so
/// the passing liveness tests are non-vacuous.
/// </para>
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class AtomicCommitLivenessCoyoteTests
{
    /// <summary>
    /// The fix, commit path: with all participants acking and the durable-registry
    /// backstop present, every leaf eventually reaches the commit terminal and the
    /// committed saga is visible on every owning leaf - for every interleaving of
    /// the fault-injected terminal broadcast (drops, duplicates, reorderings,
    /// restarts), and for a two-leaf saga and wider.
    /// </summary>
    [Test]
    public void All_acks_with_backstop_always_commit_and_become_visible_on_every_leaf([Values(2, 3, 4)] int leafCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new AtomicCommitLivenessModel(
                leafCount,
                AtomicCommitLivenessScenario.Commit,
                AtomicCommitLivenessMode.DurableBackstop));
    }

    /// <summary>
    /// The fix, abort path: with one participant nacking and the durable-registry
    /// backstop present, every leaf eventually releases its prepared bucket under
    /// every fault interleaving, so an aborted saga leaves no participant holding a
    /// prepared value.
    /// </summary>
    [Test]
    public void One_nack_with_backstop_always_releases_every_prepared_bucket([Values(2, 3, 4)] int leafCount)
    {
        CoyoteModelHarness.AssertNoInterleavingViolation(
            new AtomicCommitLivenessModel(
                leafCount,
                AtomicCommitLivenessScenario.Abort,
                AtomicCommitLivenessMode.DurableBackstop));
    }

    /// <summary>
    /// The guard for "a saga with all acks eventually commits on every participant"
    /// and "a committed saga is eventually visible on every owning leaf": removing
    /// the backstop lets a terminal lost to a drop or a restart go unrecovered, so a
    /// leaf never drains and - once the registry decision is garbage-collected - the
    /// committed saga is no longer visible there. Coyote must find the stall. This
    /// proves the backstop in the commit liveness test above is load-bearing.
    /// </summary>
    [Test]
    public void Commit_without_backstop_can_stall_a_leaf_and_lose_visibility([Values(2, 3)] int leafCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitLivenessModel(
                leafCount,
                AtomicCommitLivenessScenario.Commit,
                AtomicCommitLivenessMode.NoBackstop));
    }

    /// <summary>
    /// The guard for "an aborted saga leaves no participant holding a prepared
    /// value": removing the backstop lets an abort terminal lost to a drop or a
    /// restart go unrecovered, so a leaf keeps its prepared bucket forever. Coyote
    /// must find the schedule that leaves a bucket un-released. This proves the
    /// backstop in the abort liveness test above is load-bearing.
    /// </summary>
    [Test]
    public void Abort_without_backstop_can_leave_a_prepared_bucket_unreleased([Values(2, 3)] int leafCount)
    {
        CoyoteModelHarness.AssertInterleavingViolationFound(
            new AtomicCommitLivenessModel(
                leafCount,
                AtomicCommitLivenessScenario.Abort,
                AtomicCommitLivenessMode.NoBackstop));
    }
}
