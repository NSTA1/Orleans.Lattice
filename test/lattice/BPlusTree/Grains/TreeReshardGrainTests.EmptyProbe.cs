using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the emptiness probe that reshard initiation runs
/// before taking its empty-tree fast path.
/// <para>
/// The fast path needs only a boolean - "does this tree hold any live key?" -
/// but it used to ask <c>ILattice.CountAsync</c>, a strongly-consistent
/// whole-tree fan-out that walks every leaf chain, discards its result whenever
/// the shard map moves under it, and retries until
/// <see cref="LatticeOptions.MaxScanRetries"/> is spent. Reshard initiation is
/// exactly when that map churns hardest, so the probe could burn the caller's
/// whole response budget and surface as a <c>TimeoutException</c> on
/// <c>ReshardAsync</c> before the reshard had even started.
/// </para>
/// <para>
/// It now OR-s a short-circuiting <see cref="IShardRootGrain.AnyAsync"/> across
/// the physical shards, which needs no stability loop: a split only ever
/// <em>moves</em> keys, so a key that exists is seen by at least one shard
/// wherever the split has got to, and seeing it twice still just means "a key
/// exists". The answer is deliberately one-sided - it may report non-empty
/// while keys migrate, but never empty while a key exists - and only "empty"
/// unlocks the fast path, so the sole consequential direction is the one that
/// cannot be wrong.
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
                "a tree that is not observably empty must fall through to the coordinator path, not the empty-tree fast path");
            Assert.That(state.State.TargetShardCount, Is.EqualTo(expectedTarget));
        });
    }

    private static void StubEveryShard(IGrainFactory grainFactory, Action<IShardRootGrain> configure)
    {
        var shard = Substitute.For<IShardRootGrain>();
        configure(shard);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shard);
    }

    [Test]
    public async Task ReshardAsync_probes_emptiness_without_counting_the_whole_tree()
    {
        // The headline behaviour: initiation must not drive ILattice.CountAsync,
        // whose stability-retry loop is what could outlast the caller's budget.
        var (grain, _, grainFactory, _) = CreateGrain(physicalShardCount: 2);
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        StubEveryShard(grainFactory, s => s.AnyAsync().Returns(Task.FromResult(false)));

        await grain.ReshardAsync(4);

        await lattice.DidNotReceive().CountAsync();
    }

    [Test]
    public async Task ReshardAsync_treats_a_probe_that_exceeds_its_budget_as_non_empty()
    {
        // A shard whose probe never returns must not pin initiation. Before the
        // budget the turn awaited this forever and the caller timed out.
        var (grain, state, grainFactory, _) = CreateGrain(
            physicalShardCount: 2,
            emptyTreeProbeBudget: TimeSpan.FromMilliseconds(50));

        StubEveryShard(grainFactory, s => s.AnyAsync().Returns(new TaskCompletionSource<bool>().Task));

        await grain.ReshardAsync(4);

        AssertTookNormalCoordinatorPath(state, 4);
    }

    [Test]
    public async Task ReshardAsync_treats_a_faulting_probe_as_non_empty()
    {
        // An inconclusive probe must never fail initiation outright.
        var (grain, state, grainFactory, _) = CreateGrain(physicalShardCount: 2);

        StubEveryShard(grainFactory, s => s.AnyAsync().Returns(
            Task.FromException<bool>(new InvalidOperationException("shard unavailable"))));

        await grain.ReshardAsync(4);

        AssertTookNormalCoordinatorPath(state, 4);
    }

    [Test]
    public async Task ReshardAsync_treats_a_single_non_empty_shard_as_non_empty()
    {
        // The OR must be genuine: one shard holding keys disqualifies the fast
        // path even when the other reports empty. A fast path taken here would
        // repin the shard map while live keys existed.
        var (grain, state, grainFactory, _) = CreateGrain(physicalShardCount: 2);

        var probes = 0;
        StubEveryShard(grainFactory, s => s.AnyAsync().Returns(
            _ => Task.FromResult(Interlocked.Increment(ref probes) > 1)));

        await grain.ReshardAsync(4);

        AssertTookNormalCoordinatorPath(state, 4);
    }

    [Test]
    public async Task ReshardAsync_takes_the_empty_tree_fast_path_when_every_shard_reports_empty()
    {
        // The guard must not cost the fast path: an empty tree has no split
        // churn, answers immediately, and still repins without a coordinator.
        var (grain, state, grainFactory, _) = CreateGrain(
            physicalShardCount: 2,
            emptyTreeProbeBudget: TimeSpan.FromMilliseconds(50));

        StubEveryShard(grainFactory, s => s.AnyAsync().Returns(Task.FromResult(false)));

        await grain.ReshardAsync(4);

        Assert.That(state.State.InProgress, Is.False,
            "the empty-tree fast path repins directly and must not latch a coordinator run");
    }

    [Test]
    public async Task ReshardAsync_probe_waits_without_a_budget_when_configured_infinite()
    {
        // Timeout.InfiniteTimeSpan restores an unbounded probe; a prompt answer
        // must still be honoured on that path.
        var (grain, state, grainFactory, _) = CreateGrain(
            physicalShardCount: 2,
            emptyTreeProbeBudget: Timeout.InfiniteTimeSpan);

        StubEveryShard(grainFactory, s => s.AnyAsync().Returns(Task.FromResult(false)));

        await grain.ReshardAsync(4);

        Assert.That(state.State.InProgress, Is.False);
    }
}
