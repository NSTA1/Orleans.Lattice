using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Issue #5969 (W11 on #5605): a negative checkpoint offset is "no claim" - a leaf born with no
// checkpoint at a partition because its donor had no replay frontier there (#5757, #5769). It is
// neither lag from the partition's first append, which counted every born leaf of a freshly
// built tree as lagging, nor incomplete append history, which would warn on every fresh resize.
public partial class WalSaturationSamplerDrainLagTests
{
    [Test]
    public async Task An_unclaimed_checkpoint_is_neither_lag_nor_incomplete_history()
    {
        var registry = new InMemoryWalCursorRegistry();
        var observedAt = DateTimeOffset.UtcNow;
        await registry.ReportCheckpointAsync(_treeId, LeafConsumerId("born"), At(observedAt), 0, -1, CancellationToken.None);
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = observedAt.UtcTicks;
        WalCommitLogWriter.RecordAppend(_treeId, 0, 0, observedAt.UtcTicks - TimeSpan.FromHours(1).Ticks);
        WalCommitLogWriter.RecordAppend(_treeId, 0, 1, observedAt.UtcTicks - TimeSpan.FromHours(1).Ticks);
        var signal = NewSignal();
        var logger = new CapturingLogger();
        var sampler = CreateRegistrySampler(registry, observedAt, signal, logger);
        var sink = new List<(double Value, string? Tree)>();
        using var listener = ListenForDrainLag(sink);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
                "a born leaf with no checkpoint claim was read as an hour behind the partition's first append");
            Assert.That(sink.Where(m => m.Tree == _treeId).Where(m => m.Value > 0), Is.Empty,
                "an unclaimed checkpoint must record no drain lag");
            Assert.That(logger.Lines, Is.Empty,
                "an unclaimed checkpoint is not incomplete history and must not warn on every fresh resize");
        });
    }

    [Test]
    public async Task An_unclaimed_consumer_hides_no_claimed_consumers_lag_and_the_holder_log_counts_it_as_unclaimed()
    {
        var registry = new InMemoryWalCursorRegistry();
        var observedAt = DateTimeOffset.UtcNow;
        await registry.ReportCheckpointAsync(_treeId, LeafConsumerId("born"), At(observedAt), 0, -1, CancellationToken.None);
        await registry.ReportCheckpointAsync(_treeId, LeafConsumerId("behind"), At(observedAt), 0, 0, CancellationToken.None);
        WalCommitLogWriter._walHeadWallClockTicks[_treeId] = observedAt.UtcTicks;
        WalCommitLogWriter.RecordAppend(_treeId, 0, 0, observedAt.UtcTicks - TimeSpan.FromHours(1).Ticks);
        WalCommitLogWriter.RecordAppend(_treeId, 0, 1, observedAt.UtcTicks - TimeSpan.FromHours(1).Ticks);
        var signal = NewSignal();
        var logger = new CapturingLogger();
        var sampler = CreateRegistrySampler(registry, observedAt, signal, logger);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
                "the claimed consumer an hour behind must still trip the drain-lag input");
            Assert.That(logger.Lines, Has.Count.EqualTo(1), "one holder line: only the claimed consumer is a holder");
            Assert.That(logger.Lines[0].Fields["ConsumerId"], Is.EqualTo(LeafConsumerId("behind")));
            Assert.That(logger.Lines[0].Fields["LaggingConsumers"], Is.EqualTo(1),
                "the unclaimed consumer was counted as lagging");
            Assert.That(logger.Lines[0].Fields.TryGetValue("UnclaimedConsumers", out var unclaimed) ? unclaimed : null,
                Is.EqualTo(1), "the holder log must keep the unclaimed population observable");
        });
    }
}
