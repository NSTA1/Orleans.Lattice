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
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
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
}
