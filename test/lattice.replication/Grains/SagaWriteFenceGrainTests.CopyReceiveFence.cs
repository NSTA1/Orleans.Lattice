using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Issue #4593: the fence closes the restored copies an engage names before the
/// caller's alias swap makes them routable, and opens them on every path that
/// resumes receiving - and on no other. One test per exit.
/// </summary>
public partial class SagaWriteFenceGrainTests
{
    private static SagaWriteFenceRequest RequestClosing(string tree, params string[] copies) =>
        Request(tree) with { ReceiveClosedCopies = copies.ToDictionary(static c => c, _ => tree) };

    [Test]
    public async Task Engage_records_then_closes_every_restored_copy_it_names()
    {
        var h = CreateGrain(["peer-a"]);

        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));

        Assert.That(h.State.State.ReceiveClosedCopies.Keys, Is.EqualTo(new[] { "orders-shadow" }));
        h.Factory.Received().GetGrain<ICopyReceiveFenceGrain>("orders-shadow");
        await h.CopyFence.Received(1).CloseAsync(SagaId, Arg.Any<long>());
        await h.CopyFence.DidNotReceive().OpenAsync(Arg.Any<string>());
    }

    [Test]
    public async Task Engage_closes_the_restored_copy_before_it_fences_writes()
    {
        var h = CreateGrain(["peer-a"]);
        var order = new List<string>();
        h.CopyFence.When(c => c.CloseAsync(SagaId, Arg.Any<long>())).Do(_ => order.Add("close"));
        h.Receive.When(r => r.PauseAsync(SagaId)).Do(_ => order.Add("pause-receive"));
        h.Shard.When(s => s.EngageWriteFenceAsync(SagaId, Arg.Any<long>())).Do(_ => order.Add("fence"));

        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));

        Assert.That(order.Take(3), Is.EqualTo(new[] { "pause-receive", "close", "fence" }),
            "receiving pauses first, so the close carries the pause epoch, and the close precedes the write fence and the swap");
    }

    [Test]
    public async Task Engage_without_restored_copies_closes_nothing()
    {
        var h = CreateGrain(["peer-a"]);

        await h.Grain.EngageAsync(Request("orders"));

        Assert.That(h.State.State.ReceiveClosedCopies, Is.Empty);
        await h.CopyFence.DidNotReceiveWithAnyArgs().CloseAsync(default!, default);
    }

    [Test]
    public async Task Terminal_lift_opens_the_restored_copies_and_forgets_them()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));

        await h.Grain.LiftAsync();

        await h.CopyFence.Received(1).OpenAsync(SagaId);
        Assert.That(h.State.State.ReceiveClosedCopies, Is.Empty);
    }

    [Test]
    public async Task Local_write_unblock_keeps_the_restored_copies_closed()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));

        await h.Grain.UnblockWritesAsync();

        await h.CopyFence.DidNotReceive().OpenAsync(Arg.Any<string>());
        Assert.That(h.State.State.ReceiveClosedCopies.Keys, Is.EqualTo(new[] { "orders-shadow" }));
    }

    [Test]
    public async Task Deadline_self_lift_keeps_the_restored_copies_closed_and_touches_them()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));
        h.State.State.FenceDeadlineTicks = DateTime.UtcNow.AddSeconds(-1).Ticks;
        h.Completion.Complete = false;

        var snap = await h.Grain.PollResumeAsync();

        Assert.That(snap.WritesUnblocked, Is.True);
        await h.CopyFence.DidNotReceive().OpenAsync(Arg.Any<string>());
        // A held copy is touched on every poll, so the closed-copy age gauge
        // keeps reporting it.
        await h.CopyFence.Received().GetStatusAsync();
    }

    [Test]
    public async Task Observed_global_completion_opens_the_restored_copies()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));
        await h.Grain.UnblockWritesAsync();
        h.Completion.Complete = true;

        var snap = await h.Grain.PollResumeAsync();

        Assert.That(snap.Phase, Is.EqualTo(SagaWriteFencePhase.Lifted));
        await h.CopyFence.Received(1).OpenAsync(SagaId);
        Assert.That(h.State.State.ReceiveClosedCopies, Is.Empty);
    }

    [Test]
    public async Task Re_engage_of_an_active_fence_keeps_the_copies_it_already_closed()
    {
        // A coordinator re-drive (or a retried commit after a failed swap)
        // re-engages; the copy the first engage closed must still be opened.
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));

        await h.Grain.EngageAsync(Request("orders"));
        await h.Grain.LiftAsync();

        Assert.That(h.State.State.ReceiveClosedCopies, Is.Empty);
        h.Factory.Received().GetGrain<ICopyReceiveFenceGrain>("orders-shadow");
        await h.CopyFence.Received(1).OpenAsync(SagaId);
    }

    [Test]
    public async Task A_close_that_fails_leaves_the_copy_recorded_so_the_lift_still_opens_it()
    {
        var h = CreateGrain(["peer-a"]);
        h.CopyFence.CloseAsync(SagaId, Arg.Any<long>()).Returns(Task.FromException(new InvalidOperationException("storage down")));

        Assert.ThrowsAsync<InvalidOperationException>(
            () => h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow")));
        Assert.That(h.State.State.ReceiveClosedCopies.Keys, Is.EqualTo(new[] { "orders-shadow" }));

        await h.Grain.LiftAsync();

        await h.CopyFence.Received(1).OpenAsync(SagaId);
    }

    [Test]
    public async Task Retention_expiry_forgets_the_restored_copies()
    {
        var h = CreateGrain(["peer-a"]);
        await h.Grain.EngageAsync(RequestClosing("orders", "orders-shadow"));
        await h.Grain.LiftAsync();
        h.State.State.ReceiveClosedCopies = new() { ["stale"] = "orders" };

        await h.Grain.ReceiveReminder(TtlReminder, default);

        Assert.That(h.State.State.ReceiveClosedCopies, Is.Empty);
    }

    [Test]
    public async Task Each_restored_copy_is_closed_with_its_own_trees_pause_epoch()
    {
        var h = CreateGrain(["peer-a"]);
        var ordersFence = Substitute.For<ITreeReceiveFenceGrain>();
        var indexFence = Substitute.For<ITreeReceiveFenceGrain>();
        ordersFence.PauseAsync(SagaId).Returns(7L);
        indexFence.PauseAsync(SagaId).Returns(2L);
        h.Factory.GetGrain<ITreeReceiveFenceGrain>("orders").Returns(ordersFence);
        h.Factory.GetGrain<ITreeReceiveFenceGrain>("orders-index").Returns(indexFence);
        var ordersCopy = Substitute.For<ICopyReceiveFenceGrain>();
        var indexCopy = Substitute.For<ICopyReceiveFenceGrain>();
        h.Factory.GetGrain<ICopyReceiveFenceGrain>("orders-shadow").Returns(ordersCopy);
        h.Factory.GetGrain<ICopyReceiveFenceGrain>("orders-index-shadow").Returns(indexCopy);

        await h.Grain.EngageAsync(Request("orders", "orders-index") with
        {
            ReceiveClosedCopies = new() { ["orders-shadow"] = "orders", ["orders-index-shadow"] = "orders-index" },
        });

        await ordersCopy.Received(1).CloseAsync(SagaId, 7);
        await indexCopy.Received(1).CloseAsync(SagaId, 2);
    }
}