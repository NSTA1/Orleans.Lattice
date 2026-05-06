using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// R-101 applier-side instrument-emission coverage. Pins the
/// behavioural contract that
/// <see cref="ReplicationApplier.RunAtomicSagaAsync"/> publishes
/// <see cref="LatticeReplicationMetrics.ApplyTxApplyDurationMs"/>
/// and <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/>
/// exactly once per terminal saga outcome (success / dlq_apply_failure)
/// across both the per-entry and batch apply paths so a future
/// refactor that quietly drops one of the two emission points trips
/// a regression test.
/// </summary>
public partial class ReplicationApplierTests
{
    [Test]
    public async Task ApplyAsync_atomic_batch_success_records_apply_tx_apply_duration_ms_with_success_outcome()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        StubAdmitComplete(h, StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 2, 0),
            trigger));

        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxApplyDurationMsName);

        var result = await h.Applier.ApplyAsync(trigger);

        var samples = collector.Measurements.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(samples, Has.Length.EqualTo(1));
            Assert.That(samples[0].Value, Is.GreaterThanOrEqualTo(0.0));
            Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == Tree));
            Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxSuccess));
        });
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_success_records_apply_tx_completed_with_success_outcome()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        StubAdmitComplete(h, StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 2, 0),
            trigger));

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        await h.Applier.ApplyAsync(trigger);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples, Has.Length.EqualTo(1));
        Assert.That(samples[0].Value, Is.EqualTo(1L));
        Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxSuccess));
        Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == Tree));
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_compensated_records_dlq_apply_failure_outcome()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        StubAdmitComplete(h, StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 2, 0),
            trigger));
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Compensated,
                AppliedCount = 0,
                FailureReason = "saga rolled back during a synthetic test failure",
            }));

        using var durationCollector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxApplyDurationMsName);
        using var completedCollector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        var result = await h.Applier.ApplyAsync(trigger);

        var durationSamples = durationCollector.Measurements.ToArray();
        var completedSamples = completedCollector.Measurements.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(durationSamples, Has.Length.EqualTo(1));
            Assert.That(durationSamples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxDlqApplyFailure));
            Assert.That(completedSamples, Has.Length.EqualTo(1));
            Assert.That(completedSamples[0].Value, Is.EqualTo(1L));
            Assert.That(completedSamples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxDlqApplyFailure));
        });
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_apply_throws_records_dlq_apply_failure_outcome()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        StubAdmitComplete(h, StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 2, 0),
            trigger));
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<AtomicApplyResult>>(_ => throw new InvalidOperationException("synthetic"));

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        var result = await h.Applier.ApplyAsync(trigger);

        var samples = collector.Measurements.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(samples, Has.Length.EqualTo(1));
            Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxDlqApplyFailure));
        });
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_cancellation_does_not_record_apply_tx_completed()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        StubAdmitComplete(h, StagedBatch(
            AtomicEntry("k0", Hlc(100), txId, 2, 0),
            trigger));
        h.Apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<AtomicApplyResult>>(_ => throw new OperationCanceledException("synthetic"));

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        Assert.That(
            async () => await h.Applier.ApplyAsync(trigger),
            Throws.InstanceOf<OperationCanceledException>());

        // Cancellation is not a terminal saga disposition — the
        // producer redelivers on the next pump cycle and the buffer
        // admits a fresh transaction-key cycle, which records the
        // eventual terminal disposition then. Recording on
        // cancellation here would inflate the success-vs-failure
        // partition the visibility surface accounts on.
        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_partial_admission_does_not_record_apply_tx_completed()
    {
        // BatchComplete=false ⇒ no saga, no tx_completed sample.
        // The buffer-grain side already incremented tx_buffered /
        // tx_buffer_bytes; the applier side stays silent until
        // completion or failure.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var trigger = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        var result = await h.Applier.ApplyAsync(trigger);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(collector.Measurements, Is.Empty);
        });
    }

    [Test]
    public async Task ApplyBatchAsync_atomic_batch_success_records_apply_tx_completed_via_batch_path()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var first = AtomicEntry("k0", Hlc(100), txId, 2, 0);
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        // Sequenced returns: the first AdmitAsync is incomplete, the
        // second triggers BatchComplete=true. Mirrors how the buffer
        // grain sees consecutive admissions.
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(
                Task.FromResult(new TxBufferAdmissionResult
                {
                    BatchComplete = false,
                    Deduped = false,
                    CompletedBatch = Array.Empty<TxStagedEntry>(),
                }),
                Task.FromResult(new TxBufferAdmissionResult
                {
                    BatchComplete = true,
                    Deduped = false,
                    CompletedBatch = StagedBatch(first, trigger),
                }));

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        var result = await h.Applier.ApplyBatchAsync(new[] { first, trigger });

        var samples = collector.Measurements.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(samples, Has.Length.EqualTo(1));
            Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxSuccess));
        });
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_duration_uses_minimum_enqueued_at_ticks_across_the_completed_batch()
    {
        // Build a CompletedBatch where index [0] has a *later*
        // EnqueuedAtTicks than index [1] (i.e. arrival order is the
        // reverse of the canonical batch order). The duration sample
        // must reflect the earlier enqueue time — operators interpret
        // it as cross-cluster end-to-end latency, so the saga's
        // visibility starts at the FIRST staged-entry's admit, not
        // whichever entry happens to land at index [0] in the
        // canonical sort.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        var first = AtomicEntry("k0", Hlc(100), txId, 2, 0);

        var nowTicks = DateTime.UtcNow.Ticks;
        // Earlier-by-500ms tick value lives at canonical index [1]:
        // index [0] is the more-recent admit, index [1] is the older.
        var earlierTicks = nowTicks - (500L * TimeSpan.TicksPerMillisecond);
        var laterTicks = nowTicks - (50L * TimeSpan.TicksPerMillisecond);
        var batch = new TxStagedEntry[]
        {
            new()
            {
                OriginClusterId = first.OriginClusterId!,
                TransactionId = first.TransactionId,
                BatchSize = first.AtomicBatchSize,
                BatchIndex = first.AtomicBatchIndex,
                Entry = first,
                EnqueuedAtTicks = laterTicks,
            },
            new()
            {
                OriginClusterId = trigger.OriginClusterId!,
                TransactionId = trigger.TransactionId,
                BatchSize = trigger.AtomicBatchSize,
                BatchIndex = trigger.AtomicBatchIndex,
                Entry = trigger,
                EnqueuedAtTicks = earlierTicks,
            },
        };
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = true,
                Deduped = false,
                CompletedBatch = batch,
            }));

        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxApplyDurationMsName);

        await h.Applier.ApplyAsync(trigger);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples, Has.Length.EqualTo(1));
        // The recorded sample must reflect `now - earlierTicks`, not
        // `now - laterTicks`. We assert it is at least 500 ms minus a
        // small tolerance to absorb test-infra clock drift, and well
        // below the wall-clock interval that would result from
        // following [0]'s tick (which would be ~50 ms).
        Assert.That(samples[0].Value, Is.GreaterThanOrEqualTo(450.0),
            "duration must reflect the minimum (earliest) EnqueuedAtTicks across the batch");
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_duration_clamps_to_zero_when_enqueued_at_ticks_is_in_the_future()
    {
        // A future-dated EnqueuedAtTicks (rehydration carries forward
        // a tick from a prior silo whose wall clock was ahead of the
        // current silo's, or an in-flight NTP correction) must produce
        // a 0 ms sample, not a negative one. A negative sample would
        // corrupt the histogram's distribution and mislead operator
        // alerts on the {success} bucket.
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var trigger = AtomicEntry("k1", Hlc(101), txId, 2, 1);
        var first = AtomicEntry("k0", Hlc(100), txId, 2, 0);

        // Both entries' EnqueuedAtTicks are 1 hour in the future.
        var futureTicks = DateTime.UtcNow.Ticks + (TimeSpan.TicksPerMillisecond * 60L * 60L * 1000L);
        var batch = new TxStagedEntry[]
        {
            new()
            {
                OriginClusterId = first.OriginClusterId!,
                TransactionId = first.TransactionId,
                BatchSize = first.AtomicBatchSize,
                BatchIndex = first.AtomicBatchIndex,
                Entry = first,
                EnqueuedAtTicks = futureTicks,
            },
            new()
            {
                OriginClusterId = trigger.OriginClusterId!,
                TransactionId = trigger.TransactionId,
                BatchSize = trigger.AtomicBatchSize,
                BatchIndex = trigger.AtomicBatchIndex,
                Entry = trigger,
                EnqueuedAtTicks = futureTicks,
            },
        };
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = true,
                Deduped = false,
                CompletedBatch = batch,
            }));

        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxApplyDurationMsName);

        await h.Applier.ApplyAsync(trigger);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples, Has.Length.EqualTo(1));
        Assert.That(samples[0].Value, Is.EqualTo(0.0),
            "negative `now - future` must clamp to 0 so the histogram never sees a negative sample");
    }
}
