using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the bounded emptiness probe that reshard
/// initiation runs before taking its empty-tree fast path.
/// <para>
/// The fast path only needs a boolean - "does this tree hold any live key?" -
/// but the count that answers it is a strongly-consistent whole-tree fan-out
/// that restarts whenever the shard map moves under it and abandons once
/// <see cref="LatticeOptions.MaxScanRetries"/> is exhausted. Reshard
/// initiation is precisely when that map is most likely to be churning, so an
/// unbounded probe could consume the caller's whole response budget and time
/// the reshard out before it had started - observed as a
/// <c>TimeoutException</c> on <c>ILattice.ReshardAsync</c> while a concurrent
/// write batch drove continuous adaptive splits.
/// </para>
/// <para>
/// Both inconclusive outcomes must be reported as "not empty" so initiation
/// proceeds down the normal coordinator path. That is the accurate reading,
/// not just the safe one: only concurrent split churn makes this probe slow
/// or unstable, and a churning tree necessarily holds keys.
/// </para>
/// </summary>
public partial class TreeReshardGrainTests
{
    // The normal (non-fast-path) coordinator branch is identifiable by the
    // state it persists: InProgress latched with the requested target. The
    // empty-tree fast path never sets these.
    private static void AssertTookNormalCoordinatorPath(
        Orleans.Lattice.Tests.Fakes.FakePersistentState<Orleans.Lattice.BPlusTree.State.TreeReshardState> state,
        int expectedTarget)
    {
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True,
                "an inconclusive emptiness probe must fall through to the coordinator path, not the empty-tree fast path");
            Assert.That(state.State.TargetShardCount, Is.EqualTo(expectedTarget));
        });
    }

    [Test]
    public async Task ReshardAsync_treats_a_probe_that_exceeds_its_budget_as_non_empty()
    {
        // Model the pathological case directly: a count that never returns
        // because the shard map keeps moving under it. Before the budget the
        // initiation turn awaited this forever and the caller timed out.
        var (grain, state, grainFactory, _) = CreateGrain(
            physicalShardCount: 2,
            reshardEmptyProbeBudget: TimeSpan.FromMilliseconds(50));

        var stalledLattice = Substitute.For<ILattice>();
        stalledLattice.CountAsync().Returns(new TaskCompletionSource<int>().Task);
        stalledLattice.IsResizeCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(stalledLattice);

        await grain.ReshardAsync(4);

        AssertTookNormalCoordinatorPath(state, 4);
    }

    [Test]
    public async Task ReshardAsync_treats_a_count_abandoned_under_topology_churn_as_non_empty()
    {
        // CountAsync throws exactly this once it exhausts MaxScanRetries
        // against a shard map that keeps changing. It must not fail the
        // reshard: the churn that caused it is itself proof of live keys.
        var (grain, state, grainFactory, _) = CreateGrain(physicalShardCount: 2);

        var churningLattice = Substitute.For<ILattice>();
        churningLattice.CountAsync().Returns(Task.FromException<int>(new InvalidOperationException(
            "CountAsync exceeded 3 retries while topology kept changing.")));
        churningLattice.IsResizeCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(churningLattice);

        await grain.ReshardAsync(4);

        AssertTookNormalCoordinatorPath(state, 4);
    }

    [Test]
    public async Task ReshardAsync_still_takes_the_empty_tree_fast_path_when_the_probe_answers_in_budget()
    {
        // The guard must not cost the fast path: a genuinely empty tree has
        // no split churn, so it answers immediately and still repins without
        // activating the coordinator.
        var (grain, state, grainFactory, _) = CreateGrain(
            physicalShardCount: 2,
            reshardEmptyProbeBudget: TimeSpan.FromMilliseconds(50));

        var emptyLattice = Substitute.For<ILattice>();
        emptyLattice.CountAsync().Returns(Task.FromResult(0));
        emptyLattice.IsResizeCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(emptyLattice);

        await grain.ReshardAsync(4);

        Assert.That(state.State.InProgress, Is.False,
            "the empty-tree fast path repins directly and must not latch a coordinator run");
    }

    [Test]
    public async Task ReshardAsync_probe_waits_without_a_budget_when_configured_infinite()
    {
        // Timeout.InfiniteTimeSpan restores the historical unbounded probe.
        // A prompt answer must still be honoured on that path.
        var (grain, state, grainFactory, _) = CreateGrain(
            physicalShardCount: 2,
            reshardEmptyProbeBudget: Timeout.InfiniteTimeSpan);

        var emptyLattice = Substitute.For<ILattice>();
        emptyLattice.CountAsync().Returns(Task.FromResult(0));
        emptyLattice.IsResizeCompleteAsync().Returns(true);
        grainFactory.GetGrain<ILattice>(TreeId).Returns(emptyLattice);

        await grain.ReshardAsync(4);

        Assert.That(state.State.InProgress, Is.False);
    }
}
