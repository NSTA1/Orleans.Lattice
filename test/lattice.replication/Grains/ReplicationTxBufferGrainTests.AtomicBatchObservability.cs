using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// R-101 buffer-grain instrument-emission coverage. Pins the
/// behavioural contract that
/// <see cref="ReplicationTxBufferGrain"/> publishes
/// <see cref="LatticeReplicationMetrics.ApplyTxBuffered"/>,
/// <see cref="LatticeReplicationMetrics.ApplyTxBufferBytes"/>, and
/// <see cref="LatticeReplicationMetrics.ApplyTxCompleted"/> at the
/// admission, removal, capacity-eviction, and orphan-sweep call
/// sites so a future refactor that quietly drops one of the four
/// emission points trips a regression test.
/// </summary>
public partial class ReplicationTxBufferGrainTests
{
    [Test]
    public async Task AdmitAsync_first_entry_of_new_transaction_increments_apply_tx_buffered()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferedName);

        await grain.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples, Has.Length.EqualTo(1));
        Assert.That(samples[0].Value, Is.EqualTo(1L));
        Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == TreeId));
    }

    [Test]
    public async Task AdmitAsync_subsequent_index_into_existing_transaction_does_not_increment_apply_tx_buffered()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferedName);

        await grain.AdmitAsync(MakeEntry(tx, 3, 1), CancellationToken.None);

        Assert.That(collector.Measurements, Is.Empty,
            "subsequent index admission to an existing transaction must not grow the transaction-count counter");
    }

    [Test]
    public async Task AdmitAsync_each_admission_increments_apply_tx_buffer_bytes()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferBytesName);

        var tx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx, 3, 0, value: new byte[] { 1, 2, 3 }), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 3, 1, value: new byte[] { 4, 5 }), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples, Has.Length.EqualTo(2));
        Assert.That(samples.All(s => s.Value > 0), Is.True,
            "every admission contributes a positive byte delta");
        foreach (var sample in samples)
        {
            Assert.That(sample.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == TreeId));
        }
    }

    [Test]
    public async Task AdmitAsync_completing_batch_decrements_apply_tx_buffered_to_zero_net()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferedName);

        var tx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx, 2, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 2, 1), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        // First admission: +1 (new transaction). Final admission removes the
        // completed batch from in-memory state: -1. Net delta: 0.
        Assert.That(samples.Sum(s => s.Value), Is.Zero);
        Assert.That(samples.Select(s => s.Value), Is.EqualTo(new[] { 1L, -1L }));
    }

    [Test]
    public async Task AdmitAsync_completing_batch_decrements_apply_tx_buffer_bytes_to_zero_net()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferBytesName);

        var tx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx, 2, 0, value: new byte[] { 1, 2, 3 }), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 2, 1, value: new byte[] { 4, 5 }), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples.Sum(s => s.Value), Is.Zero,
            "every admit byte-delta must be reversed by a single removal byte-delta on completion");
    }

    [Test]
    public async Task AdmitAsync_idempotent_redelivery_does_not_emit_metrics()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);

        using var bufferedCollector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferedName);
        using var bytesCollector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferBytesName);

        // Same (tx, index, origin) → admission is a no-op; counters stay flat.
        var redelivery = await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(redelivery.Deduped, Is.True);
            Assert.That(bufferedCollector.Measurements, Is.Empty);
            Assert.That(bytesCollector.Measurements, Is.Empty);
        });
    }

    [Test]
    public async Task AdmitAsync_capacity_eviction_increments_apply_tx_completed_with_evicted_capacity_outcome()
    {
        // maxTransactions=1 forces every new transaction to evict the
        // prior one. We admit two distinct transactions and assert the
        // counter increments once on the eviction.
        var (grain, _, _, _) = await CreateGrainAsync(maxTransactions: 1);

        var tx1 = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx1, 2, 0), CancellationToken.None);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        var tx2 = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx2, 2, 0), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples, Has.Length.EqualTo(1));
        Assert.That(samples[0].Value, Is.EqualTo(1L));
        Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == TreeId));
        Assert.That(samples[0].Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxEvictedCapacity));
    }

    [Test]
    public async Task SweepOrphansAsync_increments_apply_tx_completed_with_dlq_orphan_outcome_per_orphan()
    {
        var (grain, _, _) = await CreateOrphanSweepGrainAsync();

        // Admit two distinct partial transactions, then sweep with a
        // negligibly-small timeout so both are recognised as orphans.
        await grain.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0, key: "a"), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0, key: "b"), CancellationToken.None);

        // Wait one tick so the cutoff strictly excludes the just-admitted
        // entries (the sweep uses `oldestStagedTicks > cutoffTicks`).
        await Task.Delay(2);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        var evicted = await grain.SweepOrphansAsync(TimeSpan.FromTicks(1), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(evicted, Is.EqualTo(2));
            Assert.That(samples, Has.Length.EqualTo(2));
            foreach (var sample in samples)
            {
                Assert.That(sample.Value, Is.EqualTo(1L));
                Assert.That(sample.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                    t.Key == "tree" && (string?)t.Value == TreeId));
                Assert.That(sample.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                    t.Key == "outcome" && (string?)t.Value == LatticeReplicationMetrics.OutcomeTxDlqOrphan));
            }
        });
    }

    [Test]
    public async Task SweepOrphansAsync_decrements_apply_tx_buffered_per_orphan()
    {
        var (grain, _, _) = await CreateOrphanSweepGrainAsync();

        await grain.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0, key: "k-other"), CancellationToken.None);
        await Task.Delay(2);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferedName);

        await grain.SweepOrphansAsync(TimeSpan.FromTicks(1), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        // Each orphan sweep removes one transaction → one -1 emission
        // per orphan. Two orphans → two -1 emissions, sum = -2.
        Assert.That(samples.Sum(s => s.Value), Is.EqualTo(-2L));
    }

    [Test]
    public async Task RehydratedTransaction_terminal_removal_does_not_emit_buffered_or_buffer_bytes_decrement()
    {
        // Live admit on grain A populates the shared store. Activate
        // grain B on the same store: BulkLoadAsync rehydrates the
        // staged entries via AdmitInMemory(_, isRehydration: true),
        // which suppresses the +1 / +bytes increments. The matching
        // decrements on the eventual terminal removal must therefore
        // also be suppressed — otherwise the gauges dip below the
        // live-admission volume on the next silo restart that
        // rehydrated a non-empty buffer (visible to operators as a
        // negative reading on `apply.tx_buffered`).
        var (storeA, dataA) = FakeSystemLattice.Create();
        var contextA = Substitute.For<IGrainContext>();
        var factoryA = Substitute.For<IGrainFactory>();
        var dlqA = Substitute.For<IReplicationDeadLetterGrain>();
        var hwmA = Substitute.For<IReplicationHighWaterMarkGrain>();
        factoryA.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlqA);
        factoryA.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwmA);
        hwmA.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));
        var options = new LatticeReplicationOptions
        {
            ClusterId = OriginB,
            AtomicBatchDelivery = true,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var grainA = new ReplicationTxBufferGrain(contextA, factoryA, monitor, Serializer);
        await grainA.InitializeForTestingAsync(TreeId, storeA, CancellationToken.None);
        await grainA.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0), CancellationToken.None);
        await grainA.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0, key: "k-other"), CancellationToken.None);

        // Activate a fresh grain on a NEW store substitute that wraps
        // the SAME backing dictionary. BulkLoadAsync replays both
        // staged entries through the rehydration carve-out.
        var storeB = Substitute.For<ISystemLattice>();
        storeB.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci => RehydrationEntries(dataA, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<CancellationToken>(4)));
        storeB.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci => { dataA[ci.Arg<string>()] = ci.Arg<byte[]>(); return Task.CompletedTask; });
        storeB.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(dataA.Remove(ci.Arg<string>())));

        var contextB = Substitute.For<IGrainContext>();
        var factoryB = Substitute.For<IGrainFactory>();
        var dlqB = Substitute.For<IReplicationDeadLetterGrain>();
        var hwmB = Substitute.For<IReplicationHighWaterMarkGrain>();
        factoryB.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlqB);
        factoryB.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(hwmB);
        hwmB.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));

        var grainB = new ReplicationTxBufferGrain(contextB, factoryB, monitor, Serializer);
        await grainB.InitializeForTestingAsync(TreeId, storeB, CancellationToken.None);

        // Pre-condition: grain B sees the rehydrated transactions.
        Assert.That(await grainB.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(2),
            "rehydration must populate the in-memory index from the shared store");

        // Sweep all rehydrated orphans on grain B with collectors active.
        // The rehydration path skipped the +1 / +bytes emissions; the
        // terminal-removal path must skip the matching -1 / -bytes
        // emissions. The terminal-disposition counter
        // (apply.tx_completed) is unrelated to the gauges and DOES
        // emit per orphan — that is the documented behaviour.
        await Task.Delay(2);
        using var bufferedCollector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferedName);
        using var bytesCollector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferBytesName);
        using var completedCollector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxCompletedName);

        await grainB.SweepOrphansAsync(TimeSpan.FromTicks(1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(bufferedCollector.Measurements, Is.Empty,
                "rehydrated transactions must not emit a -1 on apply.tx_buffered when removed");
            Assert.That(bytesCollector.Measurements, Is.Empty,
                "rehydrated transactions must not emit a -bytes on apply.tx_buffer_bytes when removed");
            Assert.That(completedCollector.Measurements.Count, Is.EqualTo(2),
                "terminal-disposition counter still increments per orphan; only the gauges are session-scoped");
        });
    }

    [Test]
    public async Task RehydratedTransaction_followed_by_live_admit_emits_normally_for_the_live_key()
    {
        // Defence-in-depth on the rehydration carve-out: a grain that
        // rehydrates one transaction then admits a *different* live
        // transaction must emit +1 / +bytes for the live key only.
        // The rehydrated key's bookkeeping is independent.
        var (storeA, dataA) = FakeSystemLattice.Create();
        var contextA = Substitute.For<IGrainContext>();
        var factoryA = Substitute.For<IGrainFactory>();
        factoryA.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(Substitute.For<IReplicationDeadLetterGrain>());
        factoryA.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(Substitute.For<IReplicationHighWaterMarkGrain>());
        var options = new LatticeReplicationOptions
        {
            ClusterId = OriginB,
            AtomicBatchDelivery = true,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var grainA = new ReplicationTxBufferGrain(contextA, factoryA, monitor, Serializer);
        await grainA.InitializeForTestingAsync(TreeId, storeA, CancellationToken.None);
        await grainA.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0), CancellationToken.None);

        var storeB = Substitute.For<ISystemLattice>();
        storeB.EntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(ci => RehydrationEntries(dataA, ci.ArgAt<string?>(0), ci.ArgAt<string?>(1), ci.ArgAt<CancellationToken>(4)));
        storeB.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci => { dataA[ci.Arg<string>()] = ci.Arg<byte[]>(); return Task.CompletedTask; });
        storeB.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(dataA.Remove(ci.Arg<string>())));

        var contextB = Substitute.For<IGrainContext>();
        var factoryB = Substitute.For<IGrainFactory>();
        factoryB.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(Substitute.For<IReplicationDeadLetterGrain>());
        factoryB.GetGrain<IReplicationHighWaterMarkGrain>(Arg.Any<string>()).Returns(Substitute.For<IReplicationHighWaterMarkGrain>());

        var grainB = new ReplicationTxBufferGrain(contextB, factoryB, monitor, Serializer);
        await grainB.InitializeForTestingAsync(TreeId, storeB, CancellationToken.None);

        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ApplyTxBufferedName);

        // Live admit of a NEW transaction key on grain B: must emit +1.
        await grainB.AdmitAsync(MakeEntry(Guid.NewGuid(), 3, 0, key: "live"), CancellationToken.None);

        var samples = collector.Measurements.ToArray();
        Assert.That(samples, Has.Length.EqualTo(1));
        Assert.That(samples[0].Value, Is.EqualTo(1L),
            "live admits on a rehydrated grain still emit normally for the live key");
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> RehydrationEntries(
        SortedDictionary<string, byte[]> data,
        string? start,
        string? end,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        // Snapshot the keys before iterating so concurrent modifications
        // (e.g. RemoveTransactionAsync deleting from `data` while the
        // sweep is mid-iteration) do not throw
        // InvalidOperationException ("Collection was modified") on the
        // SortedDictionary enumerator.
        var snapshot = data.ToArray();
        foreach (var kvp in snapshot)
        {
            ct.ThrowIfCancellationRequested();
            if (start is not null && string.CompareOrdinal(kvp.Key, start) < 0) continue;
            if (end is not null && string.CompareOrdinal(kvp.Key, end) >= 0) continue;
            yield return kvp;
            await Task.Yield();
        }
    }
}
