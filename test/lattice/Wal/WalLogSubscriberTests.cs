using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Wal;

namespace Orleans.Lattice.Tests.Wal;

/// <summary>
/// Unit tests for <see cref="WalLogSubscriber"/>, the reusable per-shard WAL
/// tailing loop. Cover the standard behaviours the seam provides once for every
/// consumer: cursor advance from a durable checkpoint, fall-off-log detection,
/// ShardIndex partition filtering, maintenance filtering, per-partition
/// back-pressure (batch cap), dynamic shard onboarding, atomic-batch metadata
/// surfacing, high-water-mark tracking across skipped entries, and WAL pinning.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class WalLogSubscriberTests
{
    private const string Tree = "src-tree";
    private const string Consumer = "view:test";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static LatticeMutation Set(long ticks, int shardIndex = 0, MutationCategory category = MutationCategory.User) =>
        new()
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k" + ticks,
            Timestamp = Hlc(ticks),
            ShardIndex = shardIndex,
            Category = category,
        };

    private sealed class CollectingHandler : IWalSubscriptionHandler
    {
        public List<WalSubscriptionEntry> Entries { get; } = new();

        public HybridLogicalClock? BlockedAtHlc { get; set; }

        public void OnEntry(in WalSubscriptionEntry entry) => Entries.Add(entry);
    }

    private static (WalLogSubscriber Subscriber, FakeCommitLogReader Reader, InMemoryWalCursorRegistry Registry) Create()
    {
        var reader = new FakeCommitLogReader();
        var registry = new InMemoryWalCursorRegistry();
        return (new WalLogSubscriber(reader, registry), reader, registry);
    }

    private static WalSubscriptionContext Context(
        int partitions,
        IReadOnlyDictionary<int, long>? checkpoints = null,
        int? shardFilter = null,
        WalMaintenancePolicy maintenance = WalMaintenancePolicy.Skip,
        int batchSize = 256,
        bool pinWal = true) =>
        new(Tree, Consumer, partitions, checkpoints ?? new Dictionary<int, long>())
        {
            ShardIndexFilter = shardFilter,
            MaintenancePolicy = maintenance,
            BatchSize = batchSize,
            PinWal = pinWal,
        };

    [Test]
    public async Task DrainAsync_surfaces_entries_from_checkpoint_in_offset_order()
    {
        var (subscriber, reader, _) = Create();
        reader.Append(Tree, 0, Set(10));
        reader.Append(Tree, 0, Set(20));
        reader.Append(Tree, 0, Set(30));
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(Context(1), handler, CancellationToken.None);

        Assert.That(handler.Entries.Select(e => e.Offset), Is.EqualTo(new long[] { 0, 1, 2 }));
        Assert.That(result.AdvancedOffsets[0], Is.EqualTo(2));
        Assert.That(result.EntriesSurfaced, Is.EqualTo(3));
        Assert.That(result.FellOffLog, Is.False);
    }

    [Test]
    public async Task DrainAsync_resumes_from_supplied_checkpoint()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 5; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(
            Context(1, new Dictionary<int, long> { [0] = 2 }), handler, CancellationToken.None);

        Assert.That(handler.Entries.Select(e => e.Offset), Is.EqualTo(new long[] { 3, 4 }));
        Assert.That(result.AdvancedOffsets[0], Is.EqualTo(4));
    }

    [Test]
    public async Task DrainAsync_reports_fell_off_log_when_checkpoint_trimmed()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 5; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }
        // Trim offsets below 3; a consumer checkpointed at 0 needs offsets 1..2.
        reader.TrimBefore(Tree, 0, 3);
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(
            Context(1, new Dictionary<int, long> { [0] = 0 }), handler, CancellationToken.None);

        Assert.That(result.FellOffLog, Is.True);
        Assert.That(handler.Entries, Is.Empty);
        Assert.That(result.AdvancedOffsets, Is.Empty);
    }

    [Test]
    public async Task DrainAsync_does_not_fall_off_when_checkpoint_at_trim_boundary()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 5; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }
        reader.TrimBefore(Tree, 0, 3);
        var handler = new CollectingHandler();

        // Checkpoint at 2 -> next-to-read is 3, exactly the oldest readable.
        var result = await subscriber.DrainAsync(
            Context(1, new Dictionary<int, long> { [0] = 2 }), handler, CancellationToken.None);

        Assert.That(result.FellOffLog, Is.False);
        Assert.That(handler.Entries.Select(e => e.Offset), Is.EqualTo(new long[] { 3, 4 }));
    }

    [Test]
    public async Task DrainAsync_skips_maintenance_entries_but_advances_cursor()
    {
        var (subscriber, reader, registry) = Create();
        reader.Append(Tree, 0, Set(10));
        reader.Append(Tree, 0, Set(20, category: MutationCategory.Maintenance));
        reader.Append(Tree, 0, Set(30));
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(Context(1), handler, CancellationToken.None);

        Assert.That(handler.Entries.Select(e => e.Timestamp.WallClockTicks), Is.EqualTo(new long[] { 10, 30 }));
        // Cursor advances past the skipped maintenance entry to the head HLC.
        Assert.That(result.HighestTimestamp, Is.EqualTo(Hlc(30)));
        Assert.That(result.AdvancedOffsets[0], Is.EqualTo(2));
        var minCursor = await registry.GetMinCursorAsync(Tree, CancellationToken.None);
        Assert.That(minCursor, Is.EqualTo(Hlc(30)));
    }

    [Test]
    public async Task DrainAsync_includes_maintenance_entries_under_include_policy()
    {
        var (subscriber, reader, _) = Create();
        reader.Append(Tree, 0, Set(10));
        reader.Append(Tree, 0, Set(20, category: MutationCategory.Maintenance));
        var handler = new CollectingHandler();

        await subscriber.DrainAsync(
            Context(1, maintenance: WalMaintenancePolicy.Include), handler, CancellationToken.None);

        Assert.That(handler.Entries, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task DrainAsync_filters_by_shard_index_but_advances_cursor()
    {
        var (subscriber, reader, _) = Create();
        // Two logical shards share physical partition 0.
        reader.Append(Tree, 0, Set(10, shardIndex: 0));
        reader.Append(Tree, 0, Set(20, shardIndex: 1));
        reader.Append(Tree, 0, Set(30, shardIndex: 0));
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(
            Context(1, shardFilter: 0), handler, CancellationToken.None);

        Assert.That(handler.Entries.Select(e => e.Timestamp.WallClockTicks), Is.EqualTo(new long[] { 10, 30 }));
        Assert.That(result.HighestTimestamp, Is.EqualTo(Hlc(30)));
        Assert.That(result.AdvancedOffsets[0], Is.EqualTo(2));
    }

    [Test]
    public async Task DrainAsync_caps_reads_per_partition_at_batch_size()
    {
        var (subscriber, reader, _) = Create();
        for (var i = 0; i < 10; i++)
        {
            reader.Append(Tree, 0, Set((i + 1) * 10));
        }
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(Context(1, batchSize: 3), handler, CancellationToken.None);

        Assert.That(handler.Entries, Has.Count.EqualTo(3));
        Assert.That(result.AdvancedOffsets[0], Is.EqualTo(2));
        Assert.That(result.EntriesRead, Is.EqualTo(3));
    }

    [Test]
    public async Task DrainAsync_reads_every_partition_onboarding_new_shards()
    {
        var (subscriber, reader, _) = Create();
        reader.Append(Tree, 0, Set(10));
        reader.Append(Tree, 1, Set(20));
        reader.Append(Tree, 2, Set(30));
        var handler = new CollectingHandler();

        // Partition count grew to 3 since a prior 1-partition drain.
        var result = await subscriber.DrainAsync(Context(3), handler, CancellationToken.None);

        Assert.That(handler.Entries, Has.Count.EqualTo(3));
        Assert.That(result.AdvancedOffsets.Keys, Is.EquivalentTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public async Task DrainAsync_surfaces_atomic_batch_metadata()
    {
        var (subscriber, reader, _) = Create();
        var txId = Guid.NewGuid();
        reader.Append(Tree, 0, new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = "k",
            Timestamp = Hlc(10),
            IsPrepared = true,
            AtomicBatchSize = 3,
            AtomicShardCount = 2,
            TransactionId = txId,
            CrossTreeOperationId = "xt-1",
        });
        var handler = new CollectingHandler();

        await subscriber.DrainAsync(Context(1), handler, CancellationToken.None);

        var entry = handler.Entries.Single();
        Assert.That(entry.IsPrepared, Is.True);
        Assert.That(entry.AtomicBatchSize, Is.EqualTo(3));
        Assert.That(entry.AtomicShardCount, Is.EqualTo(2));
        Assert.That(entry.TransactionId, Is.EqualTo(txId));
        Assert.That(entry.CrossTreeOperationId, Is.EqualTo("xt-1"));
    }

    [Test]
    public async Task DrainAsync_reports_blocked_floor_pin_from_handler()
    {
        var (subscriber, reader, registry) = Create();
        reader.Append(Tree, 0, Set(50));
        var handler = new CollectingHandler { BlockedAtHlc = Hlc(40) };

        await subscriber.DrainAsync(Context(1), handler, CancellationToken.None);

        var floor = await registry.GetBlockedFloorAsync(Tree, CancellationToken.None);
        Assert.That(floor, Is.EqualTo(Hlc(40)));
    }

    [Test]
    public async Task DrainAsync_does_not_pin_wal_when_pinning_disabled()
    {
        var (subscriber, reader, registry) = Create();
        reader.Append(Tree, 0, Set(10));
        var handler = new CollectingHandler();

        await subscriber.DrainAsync(Context(1, pinWal: false), handler, CancellationToken.None);

        var minCursor = await registry.GetMinCursorAsync(Tree, CancellationToken.None);
        Assert.That(minCursor, Is.Null);
    }

    [Test]
    public async Task DrainAsync_empty_partition_advances_nothing()
    {
        var (subscriber, _, _) = Create();
        var handler = new CollectingHandler();

        var result = await subscriber.DrainAsync(Context(1), handler, CancellationToken.None);

        Assert.That(handler.Entries, Is.Empty);
        Assert.That(result.AdvancedOffsets, Is.Empty);
        Assert.That(result.HighestTimestamp, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task DrainAsync_lagging_producer_pins_wal_below_an_advanced_consumer()
    {
        // A lagging producer-tailer consumer must pin the WAL: GetMinCursorAsync
        // (which the WAL GC consults before trimming) must reflect the lower of
        // an advanced view consumer and the lagging replication producer, so the
        // GC cannot trim past the producer's cursor.
        var reader = new FakeCommitLogReader();
        var registry = new InMemoryWalCursorRegistry();
        var subscriber = new WalLogSubscriber(reader, registry);
        reader.Append(Tree, 0, Set(10));
        reader.Append(Tree, 0, Set(20));

        // An already-advanced consumer sits ahead of the lagging producer.
        await registry.ReportCursorAsync(Tree, "view:ahead", Hlc(20), CancellationToken.None);

        // The replication producer drains only the first entry (lags behind).
        var producerCtx = new WalSubscriptionContext(Tree, "replication:peer-b", 1, new Dictionary<int, long>())
        {
            BatchSize = 1,
        };
        await subscriber.DrainAsync(producerCtx, new CollectingHandler(), CancellationToken.None);

        var minCursor = await registry.GetMinCursorAsync(Tree, CancellationToken.None);
        Assert.That(minCursor, Is.EqualTo(Hlc(10)),
            "WAL GC min-cursor must be pinned to the lagging producer, not the advanced consumer.");
    }

    [Test]
    public async Task DrainAsync_throws_on_null_arguments()
    {
        var (subscriber, _, _) = Create();
        Assert.That(
            async () => await subscriber.DrainAsync(null!, new CollectingHandler(), CancellationToken.None),
            Throws.ArgumentNullException);
        Assert.That(
            async () => await subscriber.DrainAsync(Context(1), null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }
}
