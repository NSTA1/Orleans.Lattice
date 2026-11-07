using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Issue #5981 (W12 on #5605): a consumer that withdrew its offset claim
// (IWalCursorRegistry.ClearCheckpointAsync) - a resident leaf the live advance moved - reads as
// no claim: counted as unclaimed, never as lag and never as incomplete history. A legacy
// consumer that never reported offsets is still incomplete history.
public partial class WalSaturationSamplerDrainLagTests
{
    [Test]
    public async Task A_withdrawn_checkpoint_claim_is_unclaimed_not_lag_or_incomplete_history()
    {
        var registry = new InMemoryWalCursorRegistry();
        var observedAt = DateTimeOffset.UtcNow;
        var consumer = LeafConsumerId("resident");
        await registry.ReportCheckpointAsync(_treeId, consumer, At(observedAt), 0, 0, CancellationToken.None);
        await registry.ClearCheckpointAsync(_treeId, consumer, 0, CancellationToken.None);
        await registry.ReportCheckpointAsync(_treeId, LeafConsumerId("behind"), At(observedAt), 0, 0, CancellationToken.None);
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = observedAt.UtcTicks;
        WalCommitLogWriter.RecordAppend(_treeId, 0, 1, observedAt.UtcTicks - TimeSpan.FromHours(1).Ticks);
        var logger = new CapturingLogger();
        var signal = NewSignal();
        var sampler = CreateRegistrySampler(registry, observedAt, signal, logger);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
                "control: the claimed consumer an hour behind trips the drain-lag input");
            Assert.That(logger.Lines.Select(l => l.Message), Has.None.Contain("incomplete local append history"),
                "a withdrawn claim was read as unknown history and warned (#5981)");
            Assert.That(logger.Lines.Where(l => l.Fields.ContainsKey("LaggingConsumers")).Select(l => l.Fields["LaggingConsumers"]),
                Has.All.EqualTo(1), "the withdrawn consumer was counted as lagging (#5981)");
            Assert.That(logger.Lines.Where(l => l.Fields.ContainsKey("UnclaimedCheckpoints")).Select(l => l.Fields["UnclaimedCheckpoints"]),
                Has.All.EqualTo(1), "the withdrawn claim must stay observable as unclaimed");
        });
    }
}
