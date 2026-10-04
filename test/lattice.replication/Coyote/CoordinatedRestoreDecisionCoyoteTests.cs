using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Replication.Tests.Coyote;

/// <summary>
/// Coyote tests for the coordinated restore's single global decision
/// (<see cref="CrossClusterSagaDecisionCore"/>, issue #4440). Opt-in:
/// <c>[Category("Coyote")]</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class CoordinatedRestoreDecisionCoyoteTests
{
    /// <summary>
    /// The production fold never leaves one cluster on its restored copy while
    /// another has compensated, whatever the votes, the coordinator's fate, or
    /// the delivery order.
    /// </summary>
    [Test]
    public void Production_fold_keeps_a_coordinated_restore_all_or_nothing() =>
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new CoordinatedRestoreDecisionModel(brokenFold: false));

    /// <summary>
    /// Guard: a fold that commits when any vote commits cuts one cluster over
    /// while another, whose build failed, has already compensated.
    /// </summary>
    [Test]
    public void Committing_on_any_vote_leaves_the_restore_mixed() =>
        CoyoteModelHarness.AssertViolationFoundInSomeExploredRun(new CoordinatedRestoreDecisionModel(brokenFold: true));
}
