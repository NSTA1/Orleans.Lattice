using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class WalSaturationSamplerDrainLagTests
{
    [Test]
    public async Task Restart_without_a_local_append_reports_unknown_instead_of_zero()
    {
        var registry = new InMemoryWalCursorRegistry();
        var now = DateTimeOffset.UtcNow;
        await registry.ReportCheckpointAsync(_treeId, LeafConsumerId("no-local-append"),
            At(now - TimeSpan.FromHours(6)), 0, 42);
        var logger = new CapturingLogger();
        var sampler = CreateRegistrySampler(registry, now, NewSignal(), logger);
        var sink = new List<(double Value, string? Tree)>();
        using var listener = ListenForDrainLag(sink);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(sink.Where(m => m.Tree == _treeId), Is.Empty);
        Assert.That(logger.Lines.Single().Message, Does.Contain("incomplete local append history"));
    }

    [TestCase(-1)]
    [TestCase(42)]
    public void Empty_append_history_has_unknown_lag_even_with_a_durable_checkpoint(long checkpoint)
    {
        var history = new WalAppendTimeline();
        Assert.That(history.GetLagTicks(checkpoint, 1000, out var incomplete), Is.Null);
        Assert.That(incomplete, Is.True);
    }

    [Test]
    public void Checkpoint_beyond_the_observed_head_has_unknown_lag()
    {
        var history = new WalAppendTimeline();
        history.Record(10, 100);
        Assert.That(history.GetLagTicks(42, 1000, out var incomplete), Is.Null);
        Assert.That(incomplete, Is.True);
    }

    [Test]
    public async Task Recent_unread_offset_with_ancient_HLC_does_not_throttle()
    {
        var registry = new InMemoryWalCursorRegistry();
        var now = DateTimeOffset.UtcNow;
        var oldStamp = At(now - TimeSpan.FromHours(6));
        await registry.ReportCheckpointAsync(_treeId, LeafConsumerId("reap"), oldStamp, 0, 40);
        WalCommitLogWriter.RecordAppend(_treeId, 0, 41, now.UtcTicks - TimeSpan.FromSeconds(1).Ticks);
        var signal = NewSignal();
        var sampler = CreateRegistrySampler(registry, now, signal, NullLogger<WalSaturationSampler>.Instance);
        var sink = new List<(double Value, string? Tree)>();
        using var listener = ListenForDrainLag(sink);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));
        Assert.That(sink.Where(m => m.Tree == _treeId).Select(m => m.Value), Is.EqualTo(new[] { 1000d }));
    }

    [Test]
    public async Task Restart_gap_reports_lower_bound_without_using_old_HLC()
    {
        var registry = new InMemoryWalCursorRegistry();
        var now = DateTimeOffset.UtcNow;
        await registry.ReportCheckpointAsync(_treeId, LeafConsumerId("restarted"), At(now - TimeSpan.FromHours(6)), 0, 4);
        WalCommitLogWriter.RecordAppend(_treeId, 0, 100, now.UtcTicks - TimeSpan.FromSeconds(1).Ticks);
        var logger = new CapturingLogger();
        var signal = NewSignal();
        var sampler = CreateRegistrySampler(registry, now, signal, logger);
        var sink = new List<(double Value, string? Tree)>();
        using var listener = ListenForDrainLag(sink);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));
        Assert.That(sink.Where(m => m.Tree == _treeId).Select(m => m.Value), Is.EqualTo(new[] { 1000d }));
        Assert.That(logger.Lines.Single().Message, Does.Contain("incomplete local append history"));
    }

    [Test]
    public async Task Legacy_HLC_only_report_warns_and_does_not_emit_a_false_zero()
    {
        var registry = new InMemoryWalCursorRegistry();
        var now = DateTimeOffset.UtcNow;
        await registry.ReportCursorAsync(_treeId, "legacy-view", At(now - TimeSpan.FromHours(6)));
        WalCommitLogWriter.RecordAppend(_treeId, 0, 1, now.UtcTicks);
        var logger = new CapturingLogger();
        var sampler = CreateRegistrySampler(registry, now, NewSignal(), logger);
        var sink = new List<(double Value, string? Tree)>();
        using var listener = ListenForDrainLag(sink);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(sink.Where(m => m.Tree == _treeId), Is.Empty);
        Assert.That(logger.Lines.Single().Message, Does.Contain("unknown ages are not recorded as zero"));
    }

    [Test]
    public void Append_history_is_bounded_and_missing_offsets_have_explicit_lower_bounds()
    {
        var history = new WalAppendTimeline();
        for (var offset = 0; offset <= WalAppendTimeline.Capacity; offset++)
            history.Record(offset, offset + 1);

        var lag = history.GetLagTicks(-1, 100_000, out var incomplete);
        Assert.That(incomplete, Is.True, "offset zero must have been evicted");
        Assert.That(lag, Is.EqualTo(99_998));
        Assert.That(history.GetLagTicks(WalAppendTimeline.Capacity, 100_000, out incomplete), Is.Zero);
        Assert.That(incomplete, Is.False);
    }

    [Test]
    public void Out_of_order_append_acknowledgements_preserve_offset_identity()
    {
        var history = new WalAppendTimeline();
        history.Record(2, 100);
        history.Record(1, 110);
        Assert.That(history.GetLagTicks(0, 200, out var incomplete), Is.EqualTo(90));
        Assert.That(incomplete, Is.False);
        Assert.That(history.GetLagTicks(1, 200, out incomplete), Is.EqualTo(100));
    }
}
