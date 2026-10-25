using Microsoft.Extensions.Hosting;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task ExecuteAsync_quiesce_uses_dispatch_budget_before_shutdown_watchdog()
    {
        using var stopping = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        lifetime.ApplicationStopping.Returns(stopping.Token);
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.Delay(Timeout.InfiniteTimeSpan, (CancellationToken)call[1]));
        var (grain, state, lattice, _) = CreateGrainWithSignal(signal,
            options: new LatticeOptions
            {
                WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
                TxRegistryShardCount = 1,
            }, lifetime: lifetime);

        Assert.ThrowsAsync<LatticeSaturatedException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("bounded-quiesce", [1]))),
            "[SagaQuiesceWinsDispatch] dispatch-budget expiry must refuse before the shutdown watchdog");

        Assert.Multiple(() =>
        {
            Assert.That(stopping.IsCancellationRequested, Is.False,
                "The real dispatch-budget timer, not the cleanup watchdog, must finish the wait");
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Execute));
            Assert.That(state.State.NextIndex, Is.Zero);
        });
        await signal.Received(1).WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await lattice.DidNotReceive().SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>());
    }

    [TestCase(30000, 50, 50)]
    [TestCase(30000, 60000, 30000)]
    [TestCase(30000, 30000, 30000)]
    [TestCase(30000, 0, 0)]
    [TestCase(0, 30000, 0)]
    [TestCase(-1, 50, 50)]
    [TestCase(30000, -1, 30000)]
    [TestCase(-1, -1, -1)]
    public void Effective_quiesce_budget_respects_dispatch_and_infinite_sentinels(
        int quiesceMilliseconds, int dispatchMilliseconds, int expectedMilliseconds)
    {
        Assert.That(AtomicWriteGrain.EffectiveQuiesceBudget(
                TimeSpan.FromMilliseconds(quiesceMilliseconds),
                TimeSpan.FromMilliseconds(dispatchMilliseconds)),
            Is.EqualTo(TimeSpan.FromMilliseconds(expectedMilliseconds)));
    }
}
