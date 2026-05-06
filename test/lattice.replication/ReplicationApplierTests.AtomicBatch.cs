using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Receiver-side atomic-batch staging-buffer admission tests for the
/// canonical <see cref="ReplicationApplier"/> (R-097). The applier's
/// only responsibility for atomic-batch entries is to gate on the
/// per-tree opt-in, hand the entry off to
/// <see cref="IReplicationTxBufferGrain.AdmitAsync"/>, and return
/// <c>Applied = false</c> with the per-origin high-water-mark
/// unchanged. The actual hand-off-to-atomic-apply trigger on batch
/// completion is R-098's deliverable and is intentionally not wired
/// here.
/// </summary>
public partial class ReplicationApplierTests
{
    private sealed class AtomicBatchHarness
    {
        public required ReplicationApplier Applier { get; init; }
        public required IGrainFactory Factory { get; init; }
        public required IReplicationApplyGrain Apply { get; init; }
        public required IReplicationHighWaterMarkGrain Hwm { get; init; }
        public required IReplicationTxBufferGrain Buffer { get; init; }
        public required IReplicationDeadLetterGrain Dlq { get; init; }
        public required Dictionary<string, HybridLogicalClock> HwmRows { get; init; }
    }

    private static AtomicBatchHarness CreateAtomicHarness(bool atomicBatchDelivery)
    {
        var rows = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        var factory = Substitute.For<IGrainFactory>();
        var apply = Substitute.For<IReplicationApplyGrain>();
        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        var buffer = Substitute.For<IReplicationTxBufferGrain>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();

        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(apply);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        factory.GetGrain<IReplicationTxBufferGrain>(Tree).Returns(buffer);
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);

        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(
                rows.TryGetValue((string)call[0], out var v) ? v : HybridLogicalClock.Zero));
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TxBufferAdmissionResult
            {
                BatchComplete = false,
                Deduped = false,
                CompletedBatch = Array.Empty<TxStagedEntry>(),
            }));

        // Default: saga commits successfully so opt-in callers that
        // do not override the substitute observe a committed outcome.
        apply.ApplyManyAtomicAsync(
                Arg.Any<IReadOnlyList<AtomicApplyEntry>>(),
                Arg.Any<Guid>(),
                Arg.Any<string>(),
                Arg.Any<VersionVector?>(),
                Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new AtomicApplyResult
            {
                Outcome = AtomicApplyOutcome.Committed,
                AppliedCount = ((IReadOnlyList<AtomicApplyEntry>)call[0]).Count,
                FailureReason = null,
            }));

        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            AtomicBatchDelivery = atomicBatchDelivery,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new AtomicBatchHarness
        {
            Applier = new ReplicationApplier(factory, monitor, new LocalVectorClockCache(factory)),
            Factory = factory,
            Apply = apply,
            Hwm = hwm,
            Buffer = buffer,
            Dlq = dlq,
            HwmRows = rows,
        };
    }

    private static ReplogEntry AtomicEntry(
        string key,
        HybridLogicalClock ts,
        Guid txId,
        int batchSize,
        int batchIndex,
        string origin = RemoteCluster) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = origin,
        Mode = ReplicationMode.LwwRegister,
        AtomicBatchSize = batchSize,
        AtomicBatchIndex = batchIndex,
        TransactionId = txId,
    };

    [Test]
    public async Task ApplyAsync_atomic_batch_admits_to_buffer_when_opt_in_enabled()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Buffer.Received(1).AdmitAsync(
            Arg.Is<ReplogEntry>(e =>
                e.AtomicBatchSize == 3
                && e.AtomicBatchIndex == 0
                && e.TransactionId == entry.TransactionId),
            Arg.Any<CancellationToken>());
        // Critically, the apply path is not invoked and HWM is not advanced.
        await h.Apply.DidNotReceive().ApplySetAsync(
            Arg.Any<string>(),
            Arg.Any<byte[]>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<string>(),
            Arg.Any<VersionVector?>(),
            Arg.Any<long>());
        await h.Hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_skips_buffer_when_opt_in_disabled()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: false);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0);

        var result = await h.Applier.ApplyAsync(entry);

        // Without opt-in, the entry routes through the regular point-apply
        // path (apply succeeds, HWM advances).
        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True);
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(100)));
        });
        await h.Buffer.DidNotReceive().AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.Received(1).ApplySetAsync(
            "k0",
            Arg.Any<byte[]>(),
            Hlc(100),
            RemoteCluster,
            Arg.Any<VersionVector?>(),
            Arg.Any<long>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_admits_after_hwm_dedupe_check()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        // HWM is at 100; entry at HLC 100 should be deduped before
        // hitting the buffer.
        h.HwmRows[RemoteCluster] = Hlc(100);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        await h.Buffer.DidNotReceive().AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_atomic_batch_throws_when_transaction_id_is_empty()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0)
            with { TransactionId = Guid.Empty };

        Assert.That(
            async () => await h.Applier.ApplyAsync(entry),
            Throws.ArgumentException);
        await h.Buffer.DidNotReceive().AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ApplyAsync_local_origin_atomic_batch_short_circuits_before_buffer()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        // Local-origin entry: defence-in-depth gate runs before the
        // buffer admission gate.
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0, origin: LocalCluster);

        var result = await h.Applier.ApplyAsync(entry);

        Assert.That(result.Applied, Is.False);
        await h.Buffer.DidNotReceive().AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
    }

    // -------- Batch path mirror (ApplyBatchAsync) --------

    [Test]
    public async Task ApplyBatchAsync_atomic_batch_admits_each_entry_to_buffer()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            AtomicEntry("k0", Hlc(100), txId, 3, 0),
            AtomicEntry("k1", Hlc(101), txId, 3, 1),
            AtomicEntry("k2", Hlc(102), txId, 3, 2),
        };

        var result = await h.Applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.False);
            Assert.That(result.HighWaterMark, Is.EqualTo(HybridLogicalClock.Zero));
        });
        await h.Buffer.Received(3).AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.DidNotReceive().ApplyMergeManyAsync(Arg.Any<IReadOnlyList<ApplyMergeItem>>());
    }

    [Test]
    public async Task ApplyBatchAsync_mixed_atomic_and_point_writes_routes_each_correctly()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var txId = Guid.NewGuid();
        var entries = new[]
        {
            AtomicEntry("k-atomic-0", Hlc(100), txId, 2, 0),
            AtomicEntry("k-atomic-1", Hlc(101), txId, 2, 1),
            // Non-atomic point write follows: should apply normally.
            new ReplogEntry
            {
                TreeId = Tree,
                Op = ReplogOp.Set,
                Key = "k-point",
                Value = new byte[] { 42 },
                Timestamp = Hlc(200),
                OriginClusterId = RemoteCluster,
                Mode = ReplicationMode.LwwRegister,
                // AtomicBatchSize = 0 (default) - skips buffer.
            },
        };

        var result = await h.Applier.ApplyBatchAsync(entries);

        Assert.Multiple(() =>
        {
            Assert.That(result.Applied, Is.True, "the non-atomic point write should apply");
            Assert.That(result.HighWaterMark, Is.EqualTo(Hlc(200)));
        });
        await h.Buffer.Received(2).AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>());
        await h.Apply.Received(1).ApplyMergeManyAsync(
            Arg.Is<IReadOnlyList<ApplyMergeItem>>(items =>
                items.Count == 1 && items[0].Key == "k-point"));
    }

    [Test]
    public async Task ApplyBatchAsync_atomic_batch_throws_when_transaction_id_empty_in_batch_path()
    {
        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        var entries = new[]
        {
            AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0)
                with { TransactionId = Guid.Empty },
            AtomicEntry("k1", Hlc(101), Guid.NewGuid(), 3, 1),
        };

        Assert.That(
            async () => await h.Applier.ApplyBatchAsync(entries),
            Throws.ArgumentException);
    }

    [Test]
    public async Task ApplyAsync_records_outcome_failure_when_buffer_admit_throws()
    {
        // A throwing IReplicationTxBufferGrain.AdmitAsync must surface
        // through the apply pipeline as a regular failure: the apply-
        // duration histogram records outcome=failure (the default in
        // the try/finally), not outcome=atomic-buffered. Verifies the
        // gate's outcome-tagging order — outcome is set to
        // OutcomeAtomicBuffered only after AdmitAsync returns, so a
        // throw before the assignment leaves the default in place.
        using var collector = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyDurationName);

        var h = CreateAtomicHarness(atomicBatchDelivery: true);
        h.Buffer.AdmitAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<Task<TxBufferAdmissionResult>>(_ => throw new InvalidOperationException("buffer unavailable"));
        var entry = AtomicEntry("k0", Hlc(100), Guid.NewGuid(), 3, 0);

        Assert.That(
            async () => await h.Applier.ApplyAsync(entry),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(HasTree(only.Tags, Tree), Is.True);
            Assert.That(HasOutcome(only.Tags, LatticeReplicationMetrics.OutcomeFailure), Is.True);
        });
        await h.Hwm.DidNotReceive().TryAdvanceAsync(
            Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(),
            Arg.Any<CancellationToken>());
    }
}
