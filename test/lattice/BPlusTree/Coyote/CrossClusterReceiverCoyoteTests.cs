using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Opt-in Coyote tests for the receiver half of cross-cluster atomic
/// visibility: <see cref="CrossClusterReceiverTallyModel"/> (the per-source-shard
/// terminal tally of a single-tree saga) and
/// <see cref="CrossTreeReceiverBarrierModel"/> (the cross-tree receiver barrier).
/// Each fixed-design test has a companion guard that removes exactly one fix
/// and must find a violation of one named property. The guards check the
/// property's tag in Coyote's bug report rather than accepting any violation,
/// so a guard cannot pass on a violation of a different property. The models
/// and their properties mirror <c>spec/atomic-commit/AtomicCommitCrossCluster.tla</c>.
/// </summary>
[TestFixture]
[Category("Coyote")]
public sealed class CrossClusterReceiverCoyoteTests
{
    private static void AssertGuardFinds(ICoyoteModel model, string propertyTag)
    {
        var result = CoyoteModelHarness.Explore(model);
        Assert.That(
            result.BugsFound,
            Is.GreaterThan(0),
            $"Expected Coyote to find a {propertyTag} violation, but no explored run violated anything. "
            + "The model may no longer exercise the race it is meant to catch.");
        Assert.That(
            result.BugReports,
            Has.Some.Contains(propertyTag),
            $"Coyote found a violation, but not of {propertyTag}: {string.Join(" | ", result.BugReports)}");
    }

    [TestCase(2, true, 2)]
    [TestCase(3, true, 2)]
    [TestCase(2, false, 2)]
    public void Single_tree_tally_keeps_a_replicated_saga_all_or_nothing_and_drains_it(int shards, bool committed, int duplicates)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossClusterReceiverTallyModel(shards, committed, duplicates, CrossClusterTallyGuard.None));
    }

    [Test]
    public void Unstamped_terminals_on_a_multi_shard_saga_split_the_receiver() =>
        AssertGuardFinds(
            new CrossClusterReceiverTallyModel(2, committed: true, duplicates: 0, CrossClusterTallyGuard.UnstampedTerminals),
            "[RAllOrNothing]");

    [Test]
    public void A_tally_final_on_its_first_terminal_splits_the_receiver() =>
        AssertGuardFinds(
            new CrossClusterReceiverTallyModel(2, committed: true, duplicates: 0, CrossClusterTallyGuard.MarkOnFirstTerminal),
            "[RAllOrNothing]");

    [Test]
    public void A_terminal_overtaking_its_prepare_splits_the_receiver() =>
        AssertGuardFinds(
            new CrossClusterReceiverTallyModel(2, committed: true, duplicates: 0, CrossClusterTallyGuard.TerminalMayOvertakePrepare),
            "[RAllOrNothing]");

    [Test]
    public void A_leaf_staging_a_prepare_that_trails_its_terminal_strands_it() =>
        AssertGuardFinds(
            new CrossClusterReceiverTallyModel(2, committed: true, duplicates: 2, CrossClusterTallyGuard.LatePrepareStaged),
            "[RNoStrandedPrepare]");

    [TestCase(2, true, 2)]
    [TestCase(3, true, 2)]
    [TestCase(2, false, 2)]
    public void Cross_tree_barrier_keeps_a_replicated_saga_all_or_nothing_across_trees(int trees, bool committed, int dialFaults)
    {
        CoyoteModelHarness.AssertNoViolationInAnyExploredRun(
            new CrossTreeReceiverBarrierModel(trees, committed, dialFaults, CrossTreeBarrierGuard.None));
    }

    [Test]
    public void A_barrier_deciding_on_its_first_arrival_splits_the_receiver() =>
        AssertGuardFinds(
            new CrossTreeReceiverBarrierModel(2, committed: true, dialFaults: 0, CrossTreeBarrierGuard.DecideOnFirstArrival),
            "[RAllOrNothing]");

    [Test]
    public void Notifying_the_barrier_before_registering_the_delegation_splits_the_receiver() =>
        AssertGuardFinds(
            new CrossTreeReceiverBarrierModel(2, committed: true, dialFaults: 0, CrossTreeBarrierGuard.NotifyBeforeRegister),
            "[RAllOrNothing]");

    [Test]
    public void An_undialled_delegation_read_as_in_flight_splits_the_receiver() =>
        AssertGuardFinds(
            new CrossTreeReceiverBarrierModel(2, committed: true, dialFaults: 2, CrossTreeBarrierGuard.UndialledDelegationReadsInFlight),
            "[RAllOrNothing]");
}
