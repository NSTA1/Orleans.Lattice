using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Adapters;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Integration;

/// <summary>
/// Cross-layer integration coverage for the hardening
/// of the TX-aware GC pin. Wires real
/// <see cref="ReplicationApplier"/>, real
/// <see cref="ReplicationTxBufferGrain"/> (initialised through
/// <see cref="ReplicationTxBufferGrain.InitializeForTestingAsync"/>),
/// real <see cref="InMemoryReplicationCursorRegistry"/>, and real
/// <see cref="LatticeReplicationGc"/> against an
/// <see cref="InMemoryWalStorageProvider"/> so an end-to-end admit ->
/// pin -> GC-respects-pin -> batch-completes -> drain -> GC-trims-through
/// loop is exercised through the same code paths a host runs in
/// production. Substitutes are limited to grains that are not under
/// test in this suite: <see cref="IReplicationApplyGrain"/> (the
/// per-key apply seam) and <see cref="IReplicationHighWaterMarkGrain"/>
/// (the dedupe gate).
/// </summary>
[TestFixture]
public class BlockedFloorCrossLayerIntegrationTests
{
    private const string Tree = "tree";
    private const string Origin = "site-a";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static Serializer<TxStagedEntry> StagedSerializer { get; } =
        new ServiceCollection().AddSerializer().BuildServiceProvider()
            .GetRequiredService<Serializer<TxStagedEntry>>();

    private sealed class Harness
    {
        public required ReplicationApplier Applier { get; init; }
        public required ReplicationTxBufferGrain Buffer { get; init; }
        public required InMemoryReplicationCursorRegistry Registry { get; init; }
        public required LatticeReplicationGc Gc { get; init; }
        public required InMemoryWalStorageProvider Wal { get; init; }
        public required IReplicationApplyGrain ApplyGrain { get; init; }
        public required IReplicationHighWaterMarkGrain Hwm { get; init; }
    }

    private static async Task<Harness> CreateHarnessAsync()
    {
        var (store, _) = FakeSystemLattice.Create();
        var grainContext = Substitute.For<IGrainContext>();
        var factory = Substitute.For<IGrainFactory>();

        var registry = new InMemoryReplicationCursorRegistry();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "local",
            AtomicBatchDelivery = true,
            ReplogPartitions = 1,
            AtomicBatchBufferMaxTransactions = 64,
            AtomicBatchBufferMaxBytes = 1L * 1024L * 1024L,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        // Real buffer grain with the registry hooked up so admits and
        // drains publish the blocked-floor pin through the production
        // code path.
        var buffer = new ReplicationTxBufferGrain(
            grainContext, factory, monitor, StagedSerializer, registry);
        await buffer.InitializeForTestingAsync(Tree, store, CancellationToken.None);

        // Substituted grains we are not testing in this suite.
        var applyGrain = Substitute.For<IReplicationApplyGrain>();
        applyGrain.ApplyManyAtomicAsync(
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

        var hwm = Substitute.For<IReplicationHighWaterMarkGrain>();
        // HWM zero so all entries are admitted (no dedupe).
        hwm.GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(HybridLogicalClock.Zero));
        hwm.TryAdvanceAsync(Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(true);
        hwm.GetVectorAsync(Arg.Any<CancellationToken>()).Returns(new VersionVector());

        var dlq = Substitute.For<IReplicationDeadLetterGrain>();

        factory.GetGrain<IReplicationTxBufferGrain>(Tree).Returns(buffer);
        factory.GetGrain<IReplicationApplyGrain>(Tree).Returns(applyGrain);
        factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree).Returns(hwm);
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);

        var applier = new ReplicationApplier(
            factory, monitor, new LocalVectorClockCache(factory),
            registry,
            NullLogger<ReplicationApplier>.Instance);

        var wal = new InMemoryWalStorageProvider();
        var sc = new ServiceCollection();
        sc.AddSingleton<IWalStorageProvider>(wal);
        var sp = sc.BuildServiceProvider();
        var gc = new LatticeReplicationGc(sp, registry, monitor);

        return new Harness
        {
            Applier = applier,
            Buffer = buffer,
            Registry = registry,
            Gc = gc,
            Wal = wal,
            ApplyGrain = applyGrain,
            Hwm = hwm,
        };
    }

    private static async Task SeedWalAsync(IWalStorageProvider wal, params ReplogEntry[] entries)
    {
        var rows = entries.Select((e, i) => new WalEntry
        {
            Offset = i,
            Mutation = ReplogEntryConverter.FromReplogEntry(e),
        }).ToArray();
        await wal.AppendBatchAsync(Tree, shardIndex: 0, rows, CancellationToken.None);
    }

    private static ReplogEntry PointSet(string key, HybridLogicalClock ts) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = Origin,
        Mode = ReplicationMode.LwwRegister,
    };

    private static ReplogEntry AtomicSet(
        string key,
        HybridLogicalClock ts,
        Guid txId,
        int batchSize,
        int batchIndex) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = Origin,
        Mode = ReplicationMode.LwwRegister,
        AtomicBatchSize = batchSize,
        AtomicBatchIndex = batchIndex,
        TransactionId = txId,
    };

    /// <summary>
    /// Full cross-layer admit -> pin -> GC-respects-pin ->
    /// batch-completes -> drain -> GC-trims-through loop. Every
    /// component (applier, buffer grain, registry, GC) is the real
    /// production type; only the per-key apply grain and the
    /// per-origin HWM grain are substituted.
    /// </summary>
    [Test]
    public async Task Atomic_batch_admit_drain_pins_GC_then_releases_after_completion()
    {
        var h = await CreateHarnessAsync();
        var tx = Guid.NewGuid();

        // Seed the producer-side WAL with a mix of point writes and
        // the three siblings of one atomic batch.
        await SeedWalAsync(h.Wal,
            PointSet("p1", Hlc(50)),                           // offset 0 — below floor
            AtomicSet("k0", Hlc(100), tx, 3, 0),               // offset 1 — at floor
            AtomicSet("k1", Hlc(110), tx, 3, 1),               // offset 2 — above floor
            AtomicSet("k2", Hlc(120), tx, 3, 2),               // offset 3 — above floor
            PointSet("p2", Hlc(200)));                         // offset 4 — above floor

        // A peer reports a generous cursor so the HLC-shaped clause
        // does not gate trimming on its own; the only thing holding
        // the GC back is the buffer pin.
        await h.Registry.ReportCursorAsync(Tree, "peer-X", Hlc(1000));

        // -- Phase 1: admit two of three siblings (partial batch) ---
        await h.Applier.ApplyAsync(AtomicSet("k0", Hlc(100), tx, 3, 0));
        await h.Applier.ApplyAsync(AtomicSet("k1", Hlc(110), tx, 3, 1));

        // The applier published the buffer's lowest staged HLC (=100)
        // through the registry under the canonical consumer id.
        var floorAfterPartial = await h.Registry.GetBlockedFloorAsync(Tree);
        Assert.That(floorAfterPartial, Is.EqualTo(Hlc(100)),
            "applier must publish the lowest staged HLC after each admit");

        // GC pass: the strict-less floor clause holds back every entry
        // whose Timestamp >= 100. Only the HLC=50 point write survives
        // the predicate (it is below the floor) so exactly one entry
        // is trimmed. Offsets are dense so trimming offset 0 leaves
        // offsets 1..4 in place.
        var report1 = await h.Gc.RunOnceAsync(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(report1.EntriesTrimmed, Is.EqualTo(1L),
                "GC must trim exactly the entry below the floor while a partial batch is staged");
            Assert.That(report1.BlockedFloor, Is.EqualTo(Hlc(100)));
        });

        var survivors1 = new List<WalEntry>();
        await foreach (var e in h.Wal.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors1.Add(e);
        }
        Assert.That(survivors1, Has.Count.EqualTo(4),
            "exactly one entry (the below-floor point write) is trimmed");
        Assert.That(survivors1.Min(e => e.Offset), Is.EqualTo(1L),
            "the at-floor entry survives via the strict-less clause");

        // -- Phase 2: admit the final sibling (batch completes) -----
        var final = await h.Applier.ApplyAsync(AtomicSet("k2", Hlc(120), tx, 3, 2));
        Assert.That(final.Applied, Is.True,
            "the batch-completing admit drives the saga path through the applier");

        // The buffer drained: the applier's drain-transition publish
        // unregistered the consumer entirely so the registry surface is
        // clean. The unit-level drain-transition test asserts the
        // unregister directly; here we assert the floor read is null
        // through the registry surface.
        var floorAfterDrain = await h.Registry.GetBlockedFloorAsync(Tree);
        Assert.That(floorAfterDrain, Is.Null,
            "drained buffer must clear the registry pin so the GC can trim through");

        // GC pass after drain: nothing is holding the log back any
        // more; the cursor permits trimming up to HLC 1000 and the
        // remaining four entries (HLC 100, 110, 120, 200) are all
        // eligible. The provider's TrimAsync semantics trim through
        // the highest eligible offset, leaving the WAL effectively
        // empty.
        var report2 = await h.Gc.RunOnceAsync(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(report2.EntriesTrimmed, Is.EqualTo(4L),
                "GC must trim every remaining entry once the buffer drains");
            Assert.That(report2.BlockedFloor, Is.Null);
        });

        var survivors2 = new List<WalEntry>();
        await foreach (var e in h.Wal.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors2.Add(e);
        }
        Assert.That(survivors2, Is.Empty,
            "post-drain GC must be able to trim the entire log up to the cursor");
    }

    /// <summary>
    /// When two atomic batches are partially staged simultaneously,
    /// the registry pin reflects the lower of the two and the GC
    /// holds back every entry at or above it. Draining the lower
    /// batch advances the floor to the higher one; only then do
    /// entries between the two old/new floors become trim-eligible.
    /// This is the multi-batch generalisation of the strict-less
    /// clause.
    /// </summary>
    [Test]
    public async Task Two_partial_batches_pin_floor_to_lowest_then_advances_when_lower_drains()
    {
        var h = await CreateHarnessAsync();
        var txLow = Guid.NewGuid();
        var txHigh = Guid.NewGuid();

        // Seed the WAL with the four siblings (two batches, two
        // siblings each). Partition: txLow's lowest is HLC 100;
        // txHigh's lowest is HLC 200.
        await SeedWalAsync(h.Wal,
            AtomicSet("low0", Hlc(100), txLow, 2, 0),
            AtomicSet("low1", Hlc(110), txLow, 2, 1),
            AtomicSet("high0", Hlc(200), txHigh, 2, 0),
            AtomicSet("high1", Hlc(210), txHigh, 2, 1));

        await h.Registry.ReportCursorAsync(Tree, "peer-X", Hlc(1000));

        // Stage one sibling of each batch — both are partial.
        await h.Applier.ApplyAsync(AtomicSet("low0", Hlc(100), txLow, 2, 0));
        await h.Applier.ApplyAsync(AtomicSet("high0", Hlc(200), txHigh, 2, 0));

        var floorBoth = await h.Registry.GetBlockedFloorAsync(Tree);
        Assert.That(floorBoth, Is.EqualTo(Hlc(100)),
            "floor must be the lower of the two staged batches");

        var reportInitial = await h.Gc.RunOnceAsync(Tree);
        Assert.That(reportInitial.EntriesTrimmed, Is.EqualTo(0L),
            "every entry in the WAL is at or above the floor — nothing trims");

        // -- Drain the lower batch by admitting its second sibling --
        await h.Applier.ApplyAsync(AtomicSet("low1", Hlc(110), txLow, 2, 1));

        var floorAfterLowDrain = await h.Registry.GetBlockedFloorAsync(Tree);
        Assert.That(floorAfterLowDrain, Is.EqualTo(Hlc(200)),
            "floor must advance to the next-lowest staged HLC once the lower batch drains");

        // GC pass: the floor is now 200; entries below 200 (HLC 100,
        // 110) are trim-eligible. The two siblings of the high batch
        // (HLC 200, 210) remain.
        var reportAfterDrain = await h.Gc.RunOnceAsync(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(reportAfterDrain.EntriesTrimmed, Is.EqualTo(2L),
                "the two below-floor entries trim once the lower batch drains");
            Assert.That(reportAfterDrain.BlockedFloor, Is.EqualTo(Hlc(200)));
        });

        var survivors = new List<WalEntry>();
        await foreach (var e in h.Wal.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(e);
        }
        Assert.That(survivors, Has.Count.EqualTo(2),
            "the two siblings of the still-partial high batch survive the trim");
    }
}
