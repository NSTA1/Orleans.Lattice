using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for the durable per-tree write fence on
/// <see cref="ShardRootGrain"/> (issue #1173). Drives the engage / lift /
/// is-fenced API and the deadline-based self-lift directly against a
/// directly-instantiated grain, without a cluster. The "fenced write is
/// refused, read is not" end-to-end proof is a separate integration test.
/// </summary>
[TestFixture]
public class ShardRootGrainWriteFenceTests
{
    private const string TreeId = "wf-tree";
    private const string ShardKey = TreeId + "/0";

    private static (ShardRootGrain Grain, FakePersistentState<ShardRootState> State) CreateGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(),
            factory: factory);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return (grain, state);
    }

    private static long FutureDeadline() => DateTime.UtcNow.AddMinutes(5).Ticks;

    [Test]
    public async Task EngageWriteFence_marks_shard_fenced_and_persists()
    {
        var (grain, state) = CreateGrain();

        await grain.EngageWriteFenceAsync("saga-1", FutureDeadline());

        Assert.That(await grain.IsWriteFencedAsync(), Is.True);
        Assert.That(state.State.WriteFenceSagaId, Is.EqualTo("saga-1"));
        Assert.That(state.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task IsWriteFenced_false_on_fresh_shard()
    {
        var (grain, _) = CreateGrain();

        Assert.That(await grain.IsWriteFencedAsync(), Is.False);
    }

    [Test]
    public async Task LiftWriteFence_clears_the_matching_saga_fence()
    {
        var (grain, state) = CreateGrain();
        await grain.EngageWriteFenceAsync("saga-1", FutureDeadline());

        await grain.LiftWriteFenceAsync("saga-1");

        Assert.That(await grain.IsWriteFencedAsync(), Is.False);
        Assert.That(state.State.WriteFenceSagaId, Is.Null);
    }

    [Test]
    public async Task LiftWriteFence_for_a_different_saga_is_a_no_op()
    {
        var (grain, state) = CreateGrain();
        await grain.EngageWriteFenceAsync("saga-1", FutureDeadline());

        // A late terminal decision from a superseded saga must not clear a fence
        // a newer saga now owns.
        await grain.LiftWriteFenceAsync("saga-2");

        Assert.That(await grain.IsWriteFencedAsync(), Is.True);
        Assert.That(state.State.WriteFenceSagaId, Is.EqualTo("saga-1"));
    }

    [Test]
    public async Task EngageWriteFence_is_idempotent_for_same_saga_and_deadline()
    {
        var (grain, state) = CreateGrain();
        var deadline = FutureDeadline();

        await grain.EngageWriteFenceAsync("saga-1", deadline);
        var writesAfterFirst = state.WriteCount;
        await grain.EngageWriteFenceAsync("saga-1", deadline);

        // A re-engage with the identical deadline does not re-persist.
        Assert.That(state.WriteCount, Is.EqualTo(writesAfterFirst));
    }

    [Test]
    public void EngageWriteFence_for_a_second_unexpired_saga_is_rejected()
    {
        var (grain, _) = CreateGrain();

        Assert.That(async () =>
        {
            await grain.EngageWriteFenceAsync("saga-1", FutureDeadline());
            await grain.EngageWriteFenceAsync("saga-2", FutureDeadline());
        }, Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task EngageWriteFence_rejects_null_or_empty_saga()
    {
        var (grain, _) = CreateGrain();

        Assert.That(() => grain.EngageWriteFenceAsync(null!, FutureDeadline()),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(() => grain.EngageWriteFenceAsync(string.Empty, FutureDeadline()),
            Throws.InstanceOf<ArgumentException>());
        await Task.CompletedTask;
    }

    [Test]
    public async Task Write_fence_self_lifts_once_the_deadline_passes()
    {
        var (grain, state) = CreateGrain();

        // Engage with a deadline already in the past: the durable flag is set
        // but the hot-path gate treats an expired fence as lifted, so a
        // stranded coordinator never fences the tree forever.
        await grain.EngageWriteFenceAsync("saga-1", DateTime.UtcNow.AddSeconds(-1).Ticks);

        Assert.That(await grain.IsWriteFencedAsync(), Is.False);
        // The durable flag is still present (only the gate treats it as lifted);
        // an explicit lift or a fresh engage still cleans it up.
        Assert.That(state.State.WriteFenceSagaId, Is.EqualTo("saga-1"));
    }
}
