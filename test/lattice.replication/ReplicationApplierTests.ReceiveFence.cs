using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Applier-level coverage for the durable inbound receive fence (issue #1173):
/// a fenced tree must surface an explicit <see cref="ApplyResult.Deferred"/>
/// signal - distinct from every other <see cref="ApplyResult.Applied"/><c> == false</c>
/// dedup / rejection outcome - so the receive paths can translate it into a
/// not-accepted, cursor-preserving ack that makes the sender re-ship the entry
/// once the fence lifts (rather than silently advancing its cursor past it).
/// </summary>
public partial class ReplicationApplierTests
{
    private sealed class ToggleReceiveGate : IReplicationReceiveGate
    {
        public bool Paused { get; set; }

        public ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default)
            => new(Paused);
    }

    private static (ReplicationApplier Applier, IReplicationApplyGrain Apply, ToggleReceiveGate Gate)
        CreateGatedApplier()
    {
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = HighWaterMarkTestGrains.Substitute();
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(HybridLogicalClock.Zero);
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());
        var gate = new ToggleReceiveGate { Paused = true };
        var applier = new ReplicationApplier(factory, Monitor(), receiveGate: gate, replicationContext: new AnyTreeLwwContext());
        return (applier, apply, gate);
    }

    [Test]
    public async Task ApplyAsync_flags_deferred_and_skips_apply_when_receive_fence_engaged()
    {
        var (applier, apply, _) = CreateGatedApplier();

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        Assert.That(result.Deferred, Is.True);
        Assert.That(result.Applied, Is.False);
        Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        await apply.DidNotReceiveWithAnyArgs()
            .ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyBatchAsync_flags_deferred_for_a_fenced_multi_entry_run()
    {
        var (applier, apply, _) = CreateGatedApplier();

        var result = await applier.ApplyBatchAsync(new[]
        {
            SetEntry("a", Hlc(10)),
            SetEntry("b", Hlc(20)),
        });

        Assert.That(result.Deferred, Is.True);
        Assert.That(result.Applied, Is.False);
        await apply.DidNotReceiveWithAnyArgs()
            .ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task ApplyAsync_does_not_flag_deferred_on_a_normal_apply()
    {
        var (applier, _, gate) = CreateGatedApplier();
        gate.Paused = false;

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        // A normal apply (fence clear) must never carry the deferred signal, so
        // the sender keeps advancing its cursor on the steady-state path.
        Assert.That(result.Deferred, Is.False);
        Assert.That(result.Applied, Is.True);
    }

    private static (LatticeReplicationDeadLetters Seam, IReplicationDeadLetterGrain Queue, IReplicationApplyGrain Apply, ToggleReceiveGate Gate)
        CreateGatedDeadLetterSeam(long entryId, WalRecord parkedEntry)
    {
        var (applier, apply, gate) = CreateGatedApplier();
        var queue = Substitute.For<IReplicationDeadLetterGrain>();
        queue.TryGetAsync(entryId, Arg.Any<CancellationToken>())
            .Returns(new DeadLetterEntry { EntryId = entryId, Entry = parkedEntry, FailureReason = "parked" });
        queue.RemoveReplayedAsync(entryId, Arg.Any<CancellationToken>()).Returns(true);
        var queueFactory = Substitute.For<IGrainFactory>();
        queueFactory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(queue);
        return (new LatticeReplicationDeadLetters(queueFactory, applier), queue, apply, gate);
    }

    [Test]
    public async Task DeadLetterReplayAsync_leaves_the_entry_parked_when_the_receive_fence_defers_it()
    {
        // Nothing re-ships a parked entry once the fence lifts, so a replay the
        // fence deferred must not remove it: removing it drops the write.
        var (seam, queue, apply, _) = CreateGatedDeadLetterSeam(7, SetEntry("k", Hlc(10)));

        var result = await seam.ReplayAsync(Tree, 7);

        Assert.That(result, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(result!.Value.Deferred, Is.True);
            Assert.That(result.Value.Applied, Is.False);
        });
        await queue.DidNotReceive().RemoveReplayedAsync(Arg.Any<long>(), Arg.Any<CancellationToken>());
        await apply.DidNotReceiveWithAnyArgs()
            .ApplySetAsync(default!, default!, default, default!, default, default);
    }

    [Test]
    public async Task DeadLetterReplayAsync_applies_and_removes_the_entry_once_the_receive_fence_lifts()
    {
        var (seam, queue, _, gate) = CreateGatedDeadLetterSeam(7, SetEntry("k", Hlc(10)));

        var deferred = await seam.ReplayAsync(Tree, 7);
        gate.Paused = false;
        var replayed = await seam.ReplayAsync(Tree, 7);

        Assert.Multiple(() =>
        {
            Assert.That(deferred!.Value.Deferred, Is.True);
            Assert.That(replayed!.Value.Deferred, Is.False);
            Assert.That(replayed.Value.Applied, Is.True);
        });
        await queue.Received(1).RemoveReplayedAsync(7, Arg.Any<CancellationToken>());
    }
}
