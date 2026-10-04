using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Applier-level coverage for the snapshot-capture decision gate (issue #4485):
/// while a capture holds a receiver tree's saga decision gate (or a backup set
/// holds its fence), the core refuses to record the replicated saga's decision
/// or delegation, and the applier must surface that as
/// <see cref="ApplyResult.Deferred"/> - a not-accepted, cursor-preserving ack the
/// sender re-ships - never as an apply failure or a silent acknowledgement.
/// </summary>
public partial class ReplicationApplierTests
{
    private static (ReplicationApplier Applier, IReplicationApplyGrain Apply) CreateApplierOverApply()
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
        var applier = new ReplicationApplier(factory, Monitor(), replicationContext: new AnyTreeLwwContext());
        return (applier, apply);
    }

    [TestCase((int)TxDecisionGateRefusal.DecisionGated)]
    [TestCase((int)TxDecisionGateRefusal.RegistrationFenced)]
    public async Task ApplyAsync_defers_an_entry_the_decision_gate_refused(int refusal)
    {
        var (applier, apply) = CreateApplierOverApply();
        apply.ApplySetAsync(default!, default!, default, default!, default, default)
            .ReturnsForAnyArgs(Task.FromException(new TxDecisionGateRefusedException(Tree, (TxDecisionGateRefusal)refusal, TimeSpan.FromSeconds(1))));

        var result = await applier.ApplyAsync(SetEntry("k", Hlc(10)));

        Assert.Multiple(() =>
        {
            Assert.That(result.Deferred, Is.True);
            Assert.That(result.Applied, Is.False);
        });
    }

    [Test]
    public async Task ApplyAsync_redelivers_a_deferred_entry_once_the_gate_releases()
    {
        var (applier, apply) = CreateApplierOverApply();
        apply.ApplySetAsync(default!, default!, default, default!, default, default)
            .ReturnsForAnyArgs(
                Task.FromException(new TxDecisionGateRefusedException(Tree, TxDecisionGateRefusal.DecisionGated, TimeSpan.FromSeconds(1))),
                Task.CompletedTask);
        var entry = SetEntry("k", Hlc(10));

        var deferred = await applier.ApplyAsync(entry);
        var redelivered = await applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(deferred.Deferred, Is.True);
            Assert.That(redelivered.Applied, Is.True, "the deferral must release the dedupe reservation so the re-ship applies");
        });
    }

    [Test]
    public void ApplyAsync_still_fails_on_a_lapsed_gate_signal()
    {
        var (applier, apply) = CreateApplierOverApply();
        apply.ApplySetAsync(default!, default!, default, default!, default, default)
            .ReturnsForAnyArgs(Task.FromException(new TxDecisionGateRefusedException(Tree, TxDecisionGateRefusal.GateLapsed, TimeSpan.Zero)));

        Assert.ThrowsAsync<TxDecisionGateRefusedException>(() => applier.ApplyAsync(SetEntry("k", Hlc(10))));
    }

    [Test]
    public async Task Causal_buffer_drain_leaves_a_gate_refused_entry_parked_instead_of_dead_lettering_it()
    {
        var h = CreateCausalHarness();
        await h.Applier.ApplyAsync(BlockedOnSiteC("k", 100));
        Assert.That(h.BufferState.State.Entries, Has.Count.EqualTo(1));
        h.Vc.Entries[OriginC] = Hlc(50);
        h.Apply.ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>())
            .Returns(
                Task.FromException(new TxDecisionGateRefusedException(Tree, TxDecisionGateRefusal.DecisionGated, TimeSpan.FromSeconds(1))),
                Task.CompletedTask);
        var grain = (CausalApplyBufferGrain)h.Factory.GetGrain<ICausalApplyBufferGrain>(Tree);

        var whileGated = await grain.DrainAsync();
        var afterRelease = await grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(whileGated, Is.EqualTo(1), "a gated entry stays parked for the next drain");
            Assert.That(afterRelease, Is.Zero, "the next drain applies it once the gate is released");
        });
        await h.Apply.Received(2).ApplySetAsync("k", Arg.Any<byte[]>(), Hlc(100), RemoteCluster, null, Arg.Any<long>());
    }
}
