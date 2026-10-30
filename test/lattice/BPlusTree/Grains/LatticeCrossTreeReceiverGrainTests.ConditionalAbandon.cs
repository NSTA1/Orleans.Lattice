using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeCrossTreeReceiverGrainTests
{
    [Test]
    public void AbandonForOriginAsync_keeps_the_condition_and_mutation_in_a_serialized_turn()
    {
        var method = typeof(ILatticeCrossTreeReceiverGrain).GetMethod(nameof(ILatticeCrossTreeReceiverGrain.AbandonForOriginAsync))!;

        Assert.That(method.IsDefined(typeof(AlwaysInterleaveAttribute), inherit: true), Is.False);
        Assert.That(typeof(LatticeCrossTreeReceiverGrain).IsDefined(typeof(ReentrantAttribute), inherit: true), Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task AbandonForOriginAsync_keeps_another_origins_barrier(bool includeDecided)
    {
        var (grain, state) = CreateGrain();
        await grain.NotifyTerminalAsync(Terminal("orders", true, ["orders", "inventory"]));
        var writes = state.WriteCount;

        Assert.That(await grain.AbandonForOriginAsync("other", includeDecided), Is.False);
        Assert.That((await grain.GetStatusAsync()).Opened, Is.True);
        Assert.That(state.RecordExists, Is.True);
        Assert.That(state.WriteCount, Is.EqualTo(writes));
    }

    [Test]
    public async Task AbandonForOriginAsync_preserves_a_durable_decision_unless_readding()
    {
        var (grain, state) = CreateGrain();
        await grain.NotifyTerminalAsync(Terminal("orders", true, ["orders"]));

        Assert.That(await grain.AbandonForOriginAsync(Origin, includeDecided: false), Is.False);
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed));
        Assert.That(state.RecordExists, Is.True);
        Assert.That(await grain.AbandonForOriginAsync(Origin, includeDecided: true), Is.True);
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.InFlight));
        Assert.That(state.RecordExists, Is.False);
    }

    [Test]
    public async Task AbandonForOriginAsync_clears_the_whole_undecided_barrier_and_is_idempotent()
    {
        var (grain, state) = CreateGrain();
        await grain.NotifyTerminalAsync(Terminal("orders", true, ["orders", "inventory"]));

        Assert.That(await grain.AbandonForOriginAsync(Origin, false), Is.True);
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.InFlight));
        Assert.That(state.RecordExists, Is.False);
        Assert.That(await grain.AbandonForOriginAsync(Origin, false), Is.False);
    }

    [Test]
    public async Task AbandonForOriginAsync_treats_a_failed_deciding_write_as_undecided()
    {
        var (grain, state) = CreateGrain();
        await grain.NotifyTerminalAsync(Terminal("orders", true, ["orders", "inventory"]));
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");
        Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await grain.NotifyTerminalAsync(Terminal("inventory", true, ["orders", "inventory"])));
        Assert.That((await grain.GetStatusAsync()).Decided, Is.False);

        Assert.That(await grain.AbandonForOriginAsync(Origin, false), Is.True);
        Assert.That(state.RecordExists, Is.False);
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task AbandonForOriginAsync_propagates_failed_clear_and_can_retry()
    {
        var (grain, state) = CreateGrain();
        await grain.NotifyTerminalAsync(Terminal("orders", true, ["orders", "inventory"]));
        state.ThrowOnClear = new InvalidOperationException("storage unavailable");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.AbandonForOriginAsync(Origin, false));
        Assert.That(state.RecordExists, Is.True);
        Assert.That(await grain.AbandonForOriginAsync(Origin, false), Is.True);
        Assert.That(state.RecordExists, Is.False);
    }

    [TestCase(null)]
    [TestCase("")]
    public void AbandonForOriginAsync_requires_an_origin(string? origin)
    {
        var (grain, _) = CreateGrain();
        Assert.CatchAsync<ArgumentException>(async () => await grain.AbandonForOriginAsync(origin!, true));
    }
}
