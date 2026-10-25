using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class AtomicWriteGrainTests
{
    [TestCase(true, WalSaturationState.Healthy)]
    [TestCase(true, WalSaturationState.Throttled)]
    [TestCase(true, WalSaturationState.Saturated)]
    [TestCase(false, WalSaturationState.Healthy)]
    [TestCase(false, WalSaturationState.Throttled)]
    [TestCase(false, WalSaturationState.Saturated)]
    public async Task ReceiveReminder_terminal_resumption_never_waits_for_pressure(
        bool commit, WalSaturationState pressure)
    {
        await AssertTerminalResumptionAsync(commit, pressure);
    }

    private static async Task AssertTerminalResumptionAsync(bool commit, WalSaturationState pressure)
    {
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(pressure);
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromCanceled(new CancellationToken(true)));
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.TreeId = TreeId;
        state.State.BoundPhysicalTreeId = TreeId;
        state.State.TransactionId = Guid.NewGuid();
        state.State.Entries = MakeEntries(("terminal-resume", [1]));
        state.State.NextIndex = state.State.Entries.Count;
        state.State.Phase = commit ? AtomicWritePhase.Execute : AtomicWritePhase.Compensate;
        state.State.FailureMessage = commit ? null : "persisted abort awaiting terminal delivery";
        var (grain, _, lattice, shard) = CreateGrainWithSignal(signal, state,
            options: new LatticeOptions { WalPartitions = 2, TxRegistryShardCount = 1 });
        state.State.RoutingEpoch = (await lattice.GetRoutingAsync()).Map.Version;
        state.State.WalPartitions = [WalPartitionHash.Compute("terminal-resume", 2)];
        var transactionId = state.State.TransactionId;

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Completed),
            "[ReliefNeverGated] terminal resumption must complete even while Saturated");
        Assert.That(lattice.ReceivedCalls().Any(call =>
            call.GetMethodInfo().Name == nameof(ILattice.SetManyAsync)), Is.False,
            "Terminal resumption must not dispatch new data");
        var terminals = shard.ReceivedCalls().Where(call =>
            call.GetMethodInfo().Name == nameof(IShardRootGrain.AppendTxTerminalAsync)).ToArray();
        Assert.That(terminals, Is.Not.Empty,
            "Resumption must deliver actual shard terminals rather than only flipping the saga phase");
        Assert.That(terminals.All(call => (Guid)call.GetArguments()[0]! == transactionId
            && (bool)call.GetArguments()[1]! == commit), Is.True,
            "Resumption must retain the persisted transaction and its commit/abort outcome");
        Assert.That(signal.ReceivedCalls().Any(call =>
            call.GetMethodInfo().Name == nameof(IWalSaturationSignal.WaitForHealthyAsync)), Is.False,
            "[ReliefNeverGated] terminal delivery must not join the signal wait queue");
    }
}
