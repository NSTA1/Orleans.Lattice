using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="TxRegistryHighWaterGrain"/>, the durable per-tree
/// saga decision registry shard high-water mark (issue #3501). The mark must only
/// ever grow, stay within the shard-count bounds, and never report a value it
/// failed to make durable.
/// </summary>
[TestFixture]
public class TxRegistryHighWaterGrainTests
{
    private static (TxRegistryHighWaterGrain Grain, FakePersistentState<TxRegistryHighWaterState> State) CreateGrain()
    {
        var state = new FakePersistentState<TxRegistryHighWaterState>();
        var grain = new TxRegistryHighWaterGrain(Substitute.For<IGrainContext>(), state);
        return (grain, state);
    }

    [Test]
    public async Task GetShardHighWaterAsync_is_zero_for_a_tree_no_shard_has_written()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.GetShardHighWaterAsync(), Is.Zero);
    }

    [Test]
    public async Task RaiseShardHighWaterAsync_persists_and_returns_the_raised_mark()
    {
        var (grain, state) = CreateGrain();

        var raised = await grain.RaiseShardHighWaterAsync(4);

        Assert.Multiple(() =>
        {
            Assert.That(raised, Is.EqualTo(4));
            Assert.That(state.State.ShardHighWater, Is.EqualTo(4));
            Assert.That(state.WriteCount, Is.EqualTo(1));
        });
        Assert.That(await grain.GetShardHighWaterAsync(), Is.EqualTo(4));
    }

    [Test]
    public async Task RaiseShardHighWaterAsync_to_a_lower_or_equal_mark_is_a_no_op()
    {
        var (grain, state) = CreateGrain();
        await grain.RaiseShardHighWaterAsync(6);

        var lower = await grain.RaiseShardHighWaterAsync(2);
        var equal = await grain.RaiseShardHighWaterAsync(6);

        Assert.Multiple(() =>
        {
            Assert.That(lower, Is.EqualTo(6), "A lower raise reports the current mark, not its argument.");
            Assert.That(equal, Is.EqualTo(6));
            Assert.That(state.WriteCount, Is.EqualTo(1), "A no-op raise must not write.");
        });
    }

    [TestCase(0, 1)]
    [TestCase(-5, 1)]
    [TestCase(10_000, LatticeOptions.MaxTxRegistryShardCount)]
    public async Task RaiseShardHighWaterAsync_clamps_to_the_shard_count_bounds(int requested, int expected)
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.RaiseShardHighWaterAsync(requested), Is.EqualTo(expected));
    }

    [Test]
    public async Task RaiseShardHighWaterAsync_rolls_back_when_the_write_fails()
    {
        var (grain, state) = CreateGrain();
        await grain.RaiseShardHighWaterAsync(2);
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RaiseShardHighWaterAsync(5));

        Assert.That(state.State.ShardHighWater, Is.EqualTo(2), "A mark that never became durable must not be reported.");
        Assert.That(await grain.GetShardHighWaterAsync(), Is.EqualTo(2));
        Assert.That(await grain.RaiseShardHighWaterAsync(5), Is.EqualTo(5), "The next raise retries the write.");
    }

    [Test]
    public void RaiseShardHighWaterAsync_deactivates_on_a_write_conflict_and_rethrows()
    {
        var context = Substitute.For<IGrainContext>();
        var state = new FakePersistentState<TxRegistryHighWaterState>();
        var grain = new TxRegistryHighWaterGrain(context, state);
        state.ThrowOnWrite = new Orleans.Storage.InconsistentStateException("etag mismatch");

        Assert.ThrowsAsync<Orleans.Storage.InconsistentStateException>(() => grain.RaiseShardHighWaterAsync(3));

        Assert.That(state.State.ShardHighWater, Is.Zero);
        context.ReceivedWithAnyArgs().Deactivate(default!);
    }

    [Test]
    public void RaiseShardHighWaterAsync_does_not_deactivate_on_a_non_conflict_failure()
    {
        var context = Substitute.For<IGrainContext>();
        var state = new FakePersistentState<TxRegistryHighWaterState>();
        var grain = new TxRegistryHighWaterGrain(context, state);
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.RaiseShardHighWaterAsync(3));

        context.DidNotReceiveWithAnyArgs().Deactivate(default!);
    }
}
