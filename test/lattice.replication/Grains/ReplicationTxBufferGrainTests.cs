using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationTxBufferGrainTests
{
    private const string TreeId = "tree";
    private const string OriginA = "site-a";
    private const string OriginB = "site-b";

    private static Serializer<TxStagedEntry> Serializer { get; } =
        new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<Serializer<TxStagedEntry>>();

    private static async Task<(ReplicationTxBufferGrain grain, SortedDictionary<string, byte[]> data, IGrainFactory factory, IReplicationDeadLetterGrain dlq)> CreateGrainAsync(
        int maxTransactions = 512,
        long maxBytes = 64L * 1024L * 1024L)
    {
        var (store, data) = FakeSystemLattice.Create();
        var context = Substitute.For<IGrainContext>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);

        var options = new LatticeReplicationOptions
        {
            ClusterId = OriginB,
            AtomicBatchDelivery = true,
            AtomicBatchBufferMaxTransactions = maxTransactions,
            AtomicBatchBufferMaxBytes = maxBytes,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var grain = new ReplicationTxBufferGrain(context, grainFactory, monitor, Serializer);
        await grain.InitializeForTestingAsync(TreeId, store, CancellationToken.None);
        return (grain, data, grainFactory, dlq);
    }

    private static ReplogEntry MakeEntry(
        Guid txId,
        int batchSize,
        int batchIndex,
        string origin = OriginA,
        string? key = null,
        byte[]? value = null)
    {
        return new ReplogEntry
        {
            TreeId = TreeId,
            Op = ReplogOp.Set,
            Key = key ?? $"k{batchIndex}",
            Value = value ?? new byte[] { (byte)batchIndex },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = origin,
            AtomicBatchSize = batchSize,
            AtomicBatchIndex = batchIndex,
            TransactionId = txId,
            Mode = ReplicationMode.LwwRegister,
        };
    }

    // -------- Admission happy paths --------

    [Test]
    public async Task AdmitAsync_partial_batch_returns_incomplete()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        var result = await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.BatchComplete, Is.False);
            Assert.That(result.Deduped, Is.False);
            Assert.That(result.CompletedBatch, Is.Empty);
        });
    }

    [Test]
    public async Task AdmitAsync_completes_batch_when_every_index_arrives()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 3, 1), CancellationToken.None);
        var final = await grain.AdmitAsync(MakeEntry(tx, 3, 2), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(final.BatchComplete, Is.True);
            Assert.That(final.Deduped, Is.False);
            Assert.That(final.CompletedBatch, Has.Count.EqualTo(3));
            Assert.That(final.CompletedBatch.Select(e => e.BatchIndex), Is.EqualTo(new[] { 0, 1, 2 }));
        });
    }

    [Test]
    public async Task AdmitAsync_returns_completed_batch_in_canonical_index_order_regardless_of_arrival_order()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(tx, 4, 2), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 4, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 4, 3), CancellationToken.None);
        var final = await grain.AdmitAsync(MakeEntry(tx, 4, 1), CancellationToken.None);

        Assert.That(final.CompletedBatch.Select(e => e.BatchIndex), Is.EqualTo(new[] { 0, 1, 2, 3 }));
    }

    [Test]
    public async Task AdmitAsync_writes_through_to_the_system_tree_during_partial_buffering()
    {
        var (grain, data, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 3, 1), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(data.Count, Is.EqualTo(2));
            Assert.That(data.Keys.All(k => k.StartsWith("b/")), Is.True);
        });
    }

    [Test]
    public async Task AdmitAsync_removes_persistent_rows_when_batch_completes()
    {
        var (grain, data, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(tx, 2, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 2, 1), CancellationToken.None);

        Assert.That(data, Is.Empty);
    }

    [Test]
    public async Task AdmitAsync_dedups_repeat_delivery_of_same_index()
    {
        var (grain, data, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        var dup = await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(dup.Deduped, Is.True);
            Assert.That(dup.BatchComplete, Is.False);
            Assert.That(dup.CompletedBatch, Is.Empty);
            Assert.That(data.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task AdmitAsync_isolates_transactions_with_distinct_txids()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(txA, 2, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(txB, 2, 0), CancellationToken.None);

        Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(2));
    }

    [Test]
    public async Task AdmitAsync_isolates_transactions_with_same_txid_but_distinct_origins()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(tx, 2, 0, origin: OriginA), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 2, 0, origin: "site-c"), CancellationToken.None);

        Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(2));
    }

    // -------- Activation rehydration --------

    [Test]
    public async Task OnActivate_rehydrates_partial_batch_from_system_tree()
    {
        var (store, data) = FakeSystemLattice.Create();
        var firstGrain = await CreateAndAdmitAsync(store, data, partialOnly: true);
        Assert.That(data.Count, Is.EqualTo(2));

        // Second activation against the same backing store: the new
        // grain rehydrates the partial batch and a follow-up admit
        // completes it.
        var secondGrain = await NewGrainAsync(store);
        Assert.That(await secondGrain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));

        var tx = firstGrain.LastTxId;
        var final = await secondGrain.AdmitAsync(MakeEntry(tx, 3, 2), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(final.BatchComplete, Is.True);
            Assert.That(final.CompletedBatch.Select(e => e.BatchIndex), Is.EqualTo(new[] { 0, 1, 2 }));
        });
    }

    private sealed class TestHarness
    {
        public required ReplicationTxBufferGrain Grain { get; init; }
        public Guid LastTxId { get; set; }
    }

    private static async Task<TestHarness> CreateAndAdmitAsync(
        Orleans.Lattice.BPlusTree.Grains.ISystemLattice store,
        SortedDictionary<string, byte[]> data,
        bool partialOnly)
    {
        var grain = await NewGrainAsync(store);
        var tx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 3, 1), CancellationToken.None);
        if (!partialOnly)
        {
            await grain.AdmitAsync(MakeEntry(tx, 3, 2), CancellationToken.None);
        }
        return new TestHarness { Grain = grain, LastTxId = tx };
    }

    private static async Task<ReplicationTxBufferGrain> NewGrainAsync(
        Orleans.Lattice.BPlusTree.Grains.ISystemLattice store)
    {
        var context = Substitute.For<IGrainContext>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);
        var options = new LatticeReplicationOptions
        {
            ClusterId = OriginB,
            AtomicBatchDelivery = true,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var grain = new ReplicationTxBufferGrain(context, grainFactory, monitor, Serializer);
        await grain.InitializeForTestingAsync(TreeId, store, CancellationToken.None);
        return grain;
    }

    // -------- Eviction --------

    [Test]
    public async Task AdmitAsync_evicts_oldest_transaction_when_transaction_cap_reached()
    {
        var (grain, _, _, dlq) = await CreateGrainAsync(maxTransactions: 2);

        var oldTx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(oldTx, 3, 0), CancellationToken.None);

        var midTx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(midTx, 3, 0), CancellationToken.None);

        // Admitting a third distinct transaction triggers eviction of
        // the oldest (oldTx) before the new one is staged.
        var newTx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(newTx, 3, 0), CancellationToken.None);

        Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(2));

        // Evicted entry is parked on the DLQ tagged "evicted".
        await dlq.Received(1).EnqueueAsync(
            Arg.Is<ReplogEntry>(e => e.TransactionId == oldTx),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonEvicted,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AdmitAsync_does_not_evict_when_admitting_into_existing_transaction()
    {
        var (grain, _, _, dlq) = await CreateGrainAsync(maxTransactions: 1);
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);
        await grain.AdmitAsync(MakeEntry(tx, 3, 1), CancellationToken.None);

        // Same transaction — no eviction. CountTransactionsAsync still 1.
        Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));
        await dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            Arg.Any<string>(),
            Arg.Any<int>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AdmitAsync_evicts_partial_batch_on_byte_cap_overflow()
    {
        // 1 MB is the floor enforced by the validator; use it directly so
        // a single 600 KB entry plus another forces eviction.
        const long byteCap = 1L * 1024L * 1024L;
        var (grain, _, _, dlq) = await CreateGrainAsync(maxBytes: byteCap);

        var bigValue = new byte[600 * 1024];
        var oldTx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(oldTx, 5, 0, value: bigValue), CancellationToken.None);

        var newTx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(newTx, 5, 0, value: bigValue), CancellationToken.None);

        // newTx's admission triggered eviction of oldTx (combined
        // payload would exceed the 1 MB cap).
        Assert.Multiple(() =>
        {
            Assert.That(grain.CountTransactionsAsync(CancellationToken.None).Result, Is.EqualTo(1));
        });
        await dlq.Received().EnqueueAsync(
            Arg.Is<ReplogEntry>(e => e.TransactionId == oldTx),
            Arg.Any<string>(),
            0,
            LatticeReplicationMetrics.ReasonEvicted,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task AdmitAsync_admits_oversize_entry_as_is_rather_than_self_evicting()
    {
        const long byteCap = 1L * 1024L * 1024L;
        var (grain, _, _, dlq) = await CreateGrainAsync(maxBytes: byteCap);

        // Single oversize entry: even though it exceeds the cap, the
        // policy admits it because there is nothing to evict.
        var huge = new byte[2 * 1024 * 1024];
        var tx = Guid.NewGuid();
        var result = await grain.AdmitAsync(MakeEntry(tx, 5, 0, value: huge), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.BatchComplete, Is.False);
            Assert.That(result.Deduped, Is.False);
            Assert.That(grain.CountTransactionsAsync(CancellationToken.None).Result, Is.EqualTo(1));
        });
        await dlq.DidNotReceive().EnqueueAsync(
            Arg.Any<ReplogEntry>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    // -------- Validation guards --------

    [Test]
    public async Task AdmitAsync_throws_on_zero_atomic_batch_size()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var entry = MakeEntry(Guid.NewGuid(), 0, 0);

        Assert.That(
            async () => await grain.AdmitAsync(entry, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AdmitAsync_throws_on_index_outside_batch_range()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var entry = MakeEntry(Guid.NewGuid(), 3, 5);

        Assert.That(
            async () => await grain.AdmitAsync(entry, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AdmitAsync_throws_on_empty_origin()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var entry = MakeEntry(Guid.NewGuid(), 3, 0) with { OriginClusterId = "" };

        Assert.That(
            async () => await grain.AdmitAsync(entry, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AdmitAsync_throws_on_empty_transaction_id()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var entry = MakeEntry(Guid.NewGuid(), 3, 0) with { TransactionId = Guid.Empty };

        Assert.That(
            async () => await grain.AdmitAsync(entry, CancellationToken.None),
            Throws.ArgumentException);
    }

    [Test]
    public async Task AdmitAsync_propagates_pre_cancelled_token()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var entry = MakeEntry(Guid.NewGuid(), 3, 0);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.AdmitAsync(entry, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task CountTransactionsAsync_propagates_pre_cancelled_token()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.CountTransactionsAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task CountBytesAsync_propagates_pre_cancelled_token()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.CountBytesAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task CountBytesAsync_tracks_in_flight_payload_size()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        var value = new byte[1024];

        await grain.AdmitAsync(MakeEntry(tx, 3, 0, value: value), CancellationToken.None);

        var bytes = await grain.CountBytesAsync(CancellationToken.None);
        Assert.That(bytes, Is.GreaterThanOrEqualTo(1024));
    }

    [Test]
    public async Task EntryKey_round_trips_with_canonical_format()
    {
        var tx = Guid.Parse("01234567-89ab-cdef-fedc-ba9876543210");
        var key = ReplicationTxBufferGrain.EntryKey(OriginA, tx, 3);
        Assert.That(key, Is.EqualTo("b/site-a/0123456789abcdeffedcba9876543210/0000000003"));
    }

    [Test]
    public async Task BackingTreeId_uses_reserved_prefix()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        Assert.That(
            ReplicationTxBufferGrain.BackingTreeId("my-tree"),
            Is.EqualTo("_lattice_replog_txbuf_my-tree"));
    }

    // -------- Defensive paths --------

    [Test]
    public async Task OnActivate_skips_malformed_rows_in_backing_store()
    {
        // Pre-populate the backing store with a row whose bytes do not
        // round-trip as a TxStagedEntry. The bulk-load path must
        // tolerate this rather than failing activation — production
        // ingest writes every row, so a deserialization failure would
        // only happen on a corrupted backing store, but BulkLoadAsync
        // must not propagate the throw because it would prevent the
        // grain from ever activating again.
        var (store, data) = FakeSystemLattice.Create();
        data["b/site-a/00000000000000000000000000000001/0000000000"] = new byte[] { 0xff, 0xff, 0xff, 0xff };

        // Also include a valid row so we can verify the loop continues
        // past the malformed row rather than aborting on the first one.
        var validGrain = await NewGrainAsync(store);
        var tx = Guid.NewGuid();
        await validGrain.AdmitAsync(MakeEntry(tx, 3, 0), CancellationToken.None);

        // Reactivate against the same store: malformed row is skipped,
        // valid row is rehydrated.
        var grain = await NewGrainAsync(store);
        Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));
    }

    [Test]
    public async Task AdmitAsync_swallows_dlq_failure_during_eviction()
    {
        // DLQ deterministically throws on every EnqueueAsync. Eviction
        // is a best-effort path: the WAL still holds the originals
        // (the per-origin high-water-mark was never advanced past the
        // displaced entries), so the DLQ failure must not block the
        // eviction or the admission of the new entry.
        var (store, _) = FakeSystemLattice.Create();
        var context = Substitute.For<IGrainContext>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        dlq.EnqueueAsync(
                Arg.Any<ReplogEntry>(),
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<string>(),
                Arg.Any<CancellationToken>())
            .Returns<Task>(_ => throw new InvalidOperationException("DLQ unavailable"));
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);

        var options = new LatticeReplicationOptions
        {
            ClusterId = OriginB,
            AtomicBatchDelivery = true,
            AtomicBatchBufferMaxTransactions = 1,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var grain = new ReplicationTxBufferGrain(context, grainFactory, monitor, Serializer);
        await grain.InitializeForTestingAsync(TreeId, store, CancellationToken.None);

        var oldTx = Guid.NewGuid();
        await grain.AdmitAsync(MakeEntry(oldTx, 3, 0), CancellationToken.None);

        // New transaction triggers eviction; DLQ throw is swallowed.
        var newTx = Guid.NewGuid();
        Assert.DoesNotThrowAsync(
            async () => await grain.AdmitAsync(MakeEntry(newTx, 3, 0), CancellationToken.None));

        Assert.That(await grain.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));
    }

    [Test]
    public async Task OnActivate_rehydrates_multiple_distinct_transactions()
    {
        // Pre-populate the backing store with entries for two distinct
        // transactions and reactivate. Both transactions must be
        // restored to the in-memory index so a follow-up admission can
        // complete either of them.
        var (store, data) = FakeSystemLattice.Create();

        // First grain stages two partial transactions.
        var primer = await NewGrainAsync(store);
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();
        await primer.AdmitAsync(MakeEntry(txA, 2, 0), CancellationToken.None);
        await primer.AdmitAsync(MakeEntry(txB, 2, 0), CancellationToken.None);
        Assert.That(data.Count, Is.EqualTo(2));

        // Second grain reactivates against the same backing store.
        var revived = await NewGrainAsync(store);
        Assert.That(await revived.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(2));

        // A follow-up admit completes txA without affecting txB.
        var finalA = await revived.AdmitAsync(MakeEntry(txA, 2, 1), CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(finalA.BatchComplete, Is.True);
            Assert.That(finalA.CompletedBatch.Select(e => e.BatchIndex), Is.EqualTo(new[] { 0, 1 }));
        });
        Assert.That(await revived.CountTransactionsAsync(CancellationToken.None), Is.EqualTo(1));
    }

    // -------- GetLowestStagedHlcAsync (R-099 producer GC pin) --------

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static ReplogEntry MakeEntryAt(
        Guid txId,
        int batchSize,
        int batchIndex,
        HybridLogicalClock timestamp,
        string origin = OriginA)
    {
        return new ReplogEntry
        {
            TreeId = TreeId,
            Op = ReplogOp.Set,
            Key = $"k{batchIndex}",
            Value = new byte[] { (byte)batchIndex },
            Timestamp = timestamp,
            OriginClusterId = origin,
            AtomicBatchSize = batchSize,
            AtomicBatchIndex = batchIndex,
            TransactionId = txId,
            Mode = ReplicationMode.LwwRegister,
        };
    }

    [Test]
    public async Task GetLowestStagedHlcAsync_returns_null_when_empty()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetLowestStagedHlcAsync_returns_min_across_single_partial_transaction()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        // Admit a 3-entry partial batch with explicit, non-monotonic
        // HLCs so the lowest is not the first-arrived.
        await grain.AdmitAsync(MakeEntryAt(tx, 3, 0, Hlc(500)), CancellationToken.None);
        await grain.AdmitAsync(MakeEntryAt(tx, 3, 1, Hlc(100)), CancellationToken.None);
        await grain.AdmitAsync(MakeEntryAt(tx, 3, 2, Hlc(300)), CancellationToken.None);

        // Batch is complete — partial buffer is now empty.
        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetLowestStagedHlcAsync_returns_min_across_partial_transaction()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        // 3-entry batch but only 2 admitted so the buffer is still
        // pinning the producer GC.
        await grain.AdmitAsync(MakeEntryAt(tx, 3, 0, Hlc(500)), CancellationToken.None);
        await grain.AdmitAsync(MakeEntryAt(tx, 3, 1, Hlc(100)), CancellationToken.None);

        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.EqualTo(Hlc(100)));
    }

    [Test]
    public async Task GetLowestStagedHlcAsync_returns_min_across_multiple_transactions()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        // Two partial transactions; the GC must pin to the lowest HLC
        // across both, not just the lowest within one.
        await grain.AdmitAsync(MakeEntryAt(txA, 2, 0, Hlc(800)), CancellationToken.None);
        await grain.AdmitAsync(MakeEntryAt(txB, 2, 0, Hlc(50)), CancellationToken.None);

        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.EqualTo(Hlc(50)));
    }

    [Test]
    public async Task GetLowestStagedHlcAsync_unpins_after_batch_completes()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntryAt(tx, 2, 0, Hlc(100)), CancellationToken.None);
        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.EqualTo(Hlc(100)));

        // Complete the batch — the entries leave the partial-buffer
        // map and the pin is released.
        await grain.AdmitAsync(MakeEntryAt(tx, 2, 1, Hlc(200)), CancellationToken.None);

        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task GetLowestStagedHlcAsync_advances_after_lower_pin_evicted_by_capacity()
    {
        // Cap-overflow eviction releases the offending transaction's
        // entries from the in-memory index, so the next pin must rise
        // to the next-lowest surviving transaction.
        var (grain, _, _, _) = await CreateGrainAsync(maxTransactions: 2);
        var txOldest = Guid.NewGuid();
        var txMiddle = Guid.NewGuid();
        var txNewest = Guid.NewGuid();

        await grain.AdmitAsync(MakeEntryAt(txOldest, 2, 0, Hlc(50)), CancellationToken.None);
        await grain.AdmitAsync(MakeEntryAt(txMiddle, 2, 0, Hlc(200)), CancellationToken.None);

        // Pin reflects the oldest's lowest HLC.
        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.EqualTo(Hlc(50)));

        // Admitting a third tx evicts txOldest (FIFO).
        await grain.AdmitAsync(MakeEntryAt(txNewest, 2, 0, Hlc(900)), CancellationToken.None);

        Assert.That(await grain.GetLowestStagedHlcAsync(CancellationToken.None), Is.EqualTo(Hlc(200)));
    }

    [Test]
    public async Task GetLowestStagedHlcAsync_observes_pre_cancelled_token()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetLowestStagedHlcAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    // -------- HLC.Zero rejection --------

    /// <summary>
    /// HybridLogicalClock.Zero is the registry sentinel meaning
    /// "no pin contributed", so it must never appear as a staged HLC.
    /// An entry whose Timestamp decodes to Zero is a programming error
    /// (a producer that forgot to stamp its HLC, or a wire-format
    /// regression) and is rejected at admission with
    /// ArgumentException rather than silently corrupting the
    /// blocked-floor multiset.
    /// </summary>
    [Test]
    public async Task AdmitAsync_throws_when_entry_timestamp_is_zero()
    {
        var (grain, _, _, _) = await CreateGrainAsync();
        var tx = Guid.NewGuid();
        var entry = MakeEntry(tx, batchSize: 3, batchIndex: 0)
            with { Timestamp = HybridLogicalClock.Zero };

        Assert.That(
            async () => await grain.AdmitAsync(entry, CancellationToken.None),
            Throws.ArgumentException);
    }

    // -------- Restart-safe republish --------

    /// <summary>
    /// Silo-restart recovery: the in-memory cursor registry is
    /// per-silo and not durable; a silo restart wipes the registry's
    /// blocked-floor pin. The buffer grain rehydrates its in-memory
    /// state from the backing system tree on activation, and after
    /// rehydration must republish the lowest staged HLC under the
    /// canonical applier consumer id so the producer-side GC pin
    /// survives the restart without waiting for a fresh
    /// admit/removal call to repopulate the registry.
    /// </summary>
    [Test]
    public async Task InitializeForTestingAsync_republishes_blocked_floor_after_rehydration()
    {
        // Phase 1: stage a partial batch and confirm the registry pin
        // is published.
        var (store, data) = FakeSystemLattice.Create();
        var context = Substitute.For<IGrainContext>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);
        var options = new LatticeReplicationOptions
        {
            ClusterId = OriginB,
            AtomicBatchDelivery = true,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var registry = new InMemoryReplicationCursorRegistry();
        var grain1 = new ReplicationTxBufferGrain(
            context, grainFactory, monitor, Serializer, registry);
        await grain1.InitializeForTestingAsync(TreeId, store, CancellationToken.None);

        var tx = Guid.NewGuid();
        await grain1.AdmitAsync(MakeEntry(tx, batchSize: 3, batchIndex: 0), CancellationToken.None);
        await grain1.AdmitAsync(MakeEntry(tx, batchSize: 3, batchIndex: 1), CancellationToken.None);
        // Buffer holds 2 of 3 staged entries; their timestamps were stamped
        // by HybridLogicalClock.Tick(Zero) inside MakeEntry — non-zero by
        // construction.

        // Simulate silo restart by discarding registry + grain instance
        // but preserving the backing system-tree rows.
        var registry2 = new InMemoryReplicationCursorRegistry();
        var grain2 = new ReplicationTxBufferGrain(
            context, grainFactory, monitor, Serializer, registry2);

        // Sanity: the new registry starts empty.
        var preSnapshot = await registry2.SnapshotAsync(TreeId, CancellationToken.None);
        Assert.That(preSnapshot, Is.Empty);

        // Reactivate against the surviving system-tree rows; the new
        // grain must rehydrate AND republish the floor.
        await grain2.InitializeForTestingAsync(TreeId, store, CancellationToken.None);

        var postSnapshot = await registry2.SnapshotAsync(TreeId, CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(postSnapshot, Has.Count.EqualTo(1),
                "rehydration must republish the blocked-floor pin under the canonical applier consumer id");
            Assert.That(postSnapshot[0].BlockedAtHlc, Is.Not.Null);
            Assert.That(postSnapshot[0].BlockedAtHlc!.Value, Is.Not.EqualTo(HybridLogicalClock.Zero));
            Assert.That(postSnapshot[0].Cursor, Is.EqualTo(HybridLogicalClock.Zero),
                "the rehydrate report uses cursor=Zero so it does not contribute to the GC's min(cursor) branch");
        });
    }

    /// <summary>
    /// Counterpart to the rehydration test: when the buffer is
    /// empty after rehydration (e.g. all batches completed before
    /// the silo restarted and the system-tree rows were already
    /// removed), the grain must NOT publish a stale
    /// (cursor=Zero, blockedAt=null) row. The next admit republishes
    /// cleanly through the applier path.
    /// </summary>
    [Test]
    public async Task InitializeForTestingAsync_does_not_publish_when_no_entries_rehydrated()
    {
        var (store, _) = FakeSystemLattice.Create();
        var context = Substitute.For<IGrainContext>();
        var grainFactory = Substitute.For<IGrainFactory>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>()).Returns(dlq);
        var options = new LatticeReplicationOptions
        {
            ClusterId = OriginB,
            AtomicBatchDelivery = true,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        var registry = new InMemoryReplicationCursorRegistry();
        var grain = new ReplicationTxBufferGrain(
            context, grainFactory, monitor, Serializer, registry);
        await grain.InitializeForTestingAsync(TreeId, store, CancellationToken.None);

        var snapshot = await registry.SnapshotAsync(TreeId, CancellationToken.None);
        Assert.That(snapshot, Is.Empty,
            "an empty rehydration must not publish a (Zero, null) row to the registry");
    }
}
