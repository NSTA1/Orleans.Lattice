using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Liveness of the activation self-terminalise sweep for a cross-tree saga
/// (issue #4485). The sweep now reads the registry with terminal intent, so it
/// may only drain on a decision recorded on THIS tree. That must not strand the
/// bucket when the coordinator has decided but this tree's sub-saga crashed
/// before its local finalize: outside a capture's decision gate the
/// terminal-intent read still dials the coordinator, records the verdict locally
/// (invariant I1), and only then reports it, so the sweep drains. Under the gate
/// it reports InFlight and the bucket drains on the first activation after the
/// gate is released. These tests drive a real <see cref="TxRegistryGrain"/>
/// behind the leaf, so the recorded-before-drained ordering is the production
/// one rather than a stub's.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string CrossTreeCoordinatorKey = "xop-crashed-participant";

    private static TxRegistryGrain RealRegistryDelegatingTo(ILatticeCrossTreeTxGrain coordinator)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", ResumableTreeId));
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { TxDecisionRetention = TimeSpan.Zero });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeCrossTreeTxGrain>(CrossTreeCoordinatorKey).Returns(coordinator);
        return new TxRegistryGrain(
            context, factory, options, NullLogger<TxRegistryGrain>.Instance, new FakePersistentState<TxRegistryState>());
    }

    [Test]
    public async Task Cross_tree_prepare_whose_participant_crashed_before_finalize_drains_after_the_coordinator_decided()
    {
        // The sub-saga parked (registered its delegation), the coordinator
        // decided commit, and the participant crashed before recording its local
        // finalize: no local decision exists on this tree.
        var txId = Guid.NewGuid();
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        var registry = RealRegistryDelegatingTo(coordinator);
        await registry.RegisterExternalDecisionAuthorityAsync(txId, CrossTreeCoordinatorKey);
        Assert.That(await registry.GetRecordedStatusAsync(txId), Is.EqualTo(TxStatus.InFlight));

        var (grain, state) = BuildSelfTerminaliseLeafCore(txId, registry, out _, persistedCheckpoint: 0);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.PendingTransactionCount, Is.EqualTo(0), "the bucket must not be stranded");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(4L));
        });
        Assert.That(
            await registry.GetRecordedStatusAsync(txId),
            Is.EqualTo(TxStatus.Committed),
            "the coordinator's verdict is recorded on this tree before the leaf drains on it (I1)");
        Assert.That(System.Text.Encoding.UTF8.GetString((await grain.GetAsync("p2"))!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task Cross_tree_prepare_left_pending_under_a_capture_gate_drains_on_the_first_activation_after_release()
    {
        var txId = Guid.NewGuid();
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        var registry = RealRegistryDelegatingTo(coordinator);
        await registry.RegisterExternalDecisionAuthorityAsync(txId, CrossTreeCoordinatorKey);
        var gate = Guid.NewGuid();
        await registry.AcquireCaptureGateAsync(gate, TxRegistryCaptureGateMode.Gate, TimeSpan.FromMinutes(1));

        var (gated, _) = BuildSelfTerminaliseLeafCore(txId, registry, out _, persistedCheckpoint: 0);
        await LeafActivationHarness.ActivateAsync(gated, CancellationToken.None);
        Assert.That(gated.PendingTransactionCount, Is.EqualTo(1), "under the gate the sweep must not drain on an unrecorded verdict");
        Assert.That(await registry.GetRecordedStatusAsync(txId), Is.EqualTo(TxStatus.InFlight));

        Assert.That(await registry.ReleaseCaptureGateAsync(gate), Is.True);
        var (recovered, _) = BuildSelfTerminaliseLeafCore(txId, registry, out _, persistedCheckpoint: 0);
        await LeafActivationHarness.ActivateAsync(recovered, CancellationToken.None);

        Assert.That(recovered.PendingTransactionCount, Is.EqualTo(0));
        Assert.That(await registry.GetRecordedStatusAsync(txId), Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task Cross_tree_prepare_stays_resident_while_its_coordinator_is_still_preparing()
    {
        var txId = Guid.NewGuid();
        var coordinator = Substitute.For<ILatticeCrossTreeTxGrain>();
        coordinator.GetDecisionAsync().Returns(TxStatus.InFlight);
        var registry = RealRegistryDelegatingTo(coordinator);
        await registry.RegisterExternalDecisionAuthorityAsync(txId, CrossTreeCoordinatorKey);

        var (grain, _) = BuildSelfTerminaliseLeafCore(txId, registry, out _, persistedCheckpoint: 0);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(grain.PendingTransactionCount, Is.EqualTo(1));
    }
}
