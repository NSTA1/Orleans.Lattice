using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Wal;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="IncrementalDeltaCollector"/>.
/// Covers the early-return branches in <c>OnEntry</c> (TxCommit/TxAbort/Tombstone,
/// out-of-scope key), the per-origin high-water accounting (lines 207-214), the
/// scope-boundary guard in <c>KeyInScope</c> (lines 346, 350), and the
/// fell-off-log break in <c>StreamAsync</c> (lines 276-277).
/// </summary>
[TestFixture]
public sealed partial class IncrementalDeltaCollectorTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    // Helper: create a collector with an optional key scope.
    private IncrementalDeltaCollector MakeCollector(
        IWalSubscriber? subscriber = null,
        string? startInclusive = null,
        string? endExclusive = null,
        IReadOnlyDictionary<Guid, TxStatus>? decisions = null,
        int partitions = 1,
        IReadOnlyList<Guid>? baseUndecided = null)
    {
        subscriber ??= Substitute.For<IWalSubscriber>();
        decisions ??= new Dictionary<Guid, TxStatus>();
        return new IncrementalDeltaCollector(
            _serializer,
            subscriber,
            treeId: "orders",
            consumerId: "test-consumer",
            partitions: partitions,
            baseOffsets: new Dictionary<int, long>(),
            startInclusive: startInclusive,
            endExclusive: endExclusive,
            mergeMode: BackupKeyMergeMode.LastWriterWins,
            baseBackupId: "base-id",
            batchSize: 100,
            resolveDecisions: (txIds, _) => Task.FromResult<IReadOnlyDictionary<Guid, TxStatus>>(
                txIds.Where(decisions.ContainsKey).ToDictionary(t => t, t => decisions[t])),
            baseUndecided: baseUndecided);
    }

    // Helper: build a WalSubscriptionEntry with the given mutation.
    private static WalSubscriptionEntry MakeEntry(LatticeMutation mutation)
        => new WalSubscriptionEntry(0, 1L, mutation);

    // ---------------------------------------------------------------------------
    // OnEntry early-return branches (lines 152-173)
    // ---------------------------------------------------------------------------

    [Test]
    public void OnEntry_TxCommit_mutation_is_skipped()
    {
        // Line 154: TxCommit, TxAbort, and Tombstone mutations return immediately
        // before the key-scope filter, so KeyDescriptors stays empty.
        var collector = MakeCollector();
        var mutation = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.TxCommit,
            Key = "any-key",
        };

        collector.OnEntry(MakeEntry(mutation));

        Assert.That(collector.KeyDescriptors, Is.Empty);
    }

    [Test]
    public void OnEntry_Tombstone_mutation_is_skipped()
    {
        // Line 154: Tombstone kind also hits the early return.
        var collector = MakeCollector();
        var mutation = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Tombstone,
            Key = "any-key",
        };

        collector.OnEntry(MakeEntry(mutation));

        Assert.That(collector.KeyDescriptors, Is.Empty);
    }

    [Test]
    public void OnEntry_key_before_scope_start_is_skipped()
    {
        // Line 172 + line 346: when KeyInScope returns false because key < startInclusive,
        // OnEntry returns without adding to KeyDescriptors or PerOriginHighWater.
        var collector = MakeCollector(startInclusive: "m", endExclusive: null);
        var mutation = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = "a", // "a" < "m", out of scope
            OriginClusterId = "cluster-x",
            Timestamp = new HybridLogicalClock { WallClockTicks = 10L },
        };

        collector.OnEntry(MakeEntry(mutation));

        Assert.That(collector.KeyDescriptors, Is.Empty);
        Assert.That(collector.PerOriginHighWater, Is.Empty);
    }

    [Test]
    public void OnEntry_key_at_end_exclusive_is_skipped()
    {
        // Line 172 + line 350: when KeyInScope returns false because key >= endExclusive,
        // OnEntry returns without adding to KeyDescriptors.
        var collector = MakeCollector(startInclusive: null, endExclusive: "z");
        var mutation = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = "z", // "z" >= "z", out of scope
        };

        collector.OnEntry(MakeEntry(mutation));

        Assert.That(collector.KeyDescriptors, Is.Empty);
    }

    // ---------------------------------------------------------------------------
    // Per-origin high-water accounting (lines 205-216)
    // ---------------------------------------------------------------------------

    [Test]
    public void OnEntry_negative_origin_ticks_are_clamped_to_zero()
    {
        // Lines 207-210: when mutation.Timestamp.WallClockTicks < 0, it is clamped
        // to 0 before being stored in the per-origin high-water dictionary.
        var collector = MakeCollector();
        var mutation = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = "k1",
            OriginClusterId = "cluster-neg",
            Timestamp = new HybridLogicalClock { WallClockTicks = -50L },
        };

        collector.OnEntry(MakeEntry(mutation));

        Assert.That(collector.PerOriginHighWater["cluster-neg"], Is.EqualTo(0L));
    }

    [Test]
    public void OnEntry_positive_origin_ticks_are_stored_as_high_water()
    {
        // Lines 207, 212-214: positive ticks are stored directly as the high-water
        // mark for the origin when no prior entry exists.
        var collector = MakeCollector();
        var mutation = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = "k2",
            OriginClusterId = "cluster-pos",
            Timestamp = new HybridLogicalClock { WallClockTicks = 77L },
        };

        collector.OnEntry(MakeEntry(mutation));

        Assert.That(collector.PerOriginHighWater["cluster-pos"], Is.EqualTo(77L));
    }

    [Test]
    public void OnEntry_lower_ticks_do_not_replace_higher_high_water()
    {
        // Line 212 (false branch): when a later entry for the same origin has ticks
        // lower than the stored high-water, the stored value must not change.
        var collector = MakeCollector();

        var highEntry = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = "k-high",
            OriginClusterId = "cluster-c",
            Timestamp = new HybridLogicalClock { WallClockTicks = 200L },
        };
        var lowEntry = new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = "k-low",
            OriginClusterId = "cluster-c",
            Timestamp = new HybridLogicalClock { WallClockTicks = 5L },
        };

        collector.OnEntry(MakeEntry(highEntry));
        collector.OnEntry(MakeEntry(lowEntry));

        Assert.That(collector.PerOriginHighWater["cluster-c"], Is.EqualTo(200L));
    }

    // ---------------------------------------------------------------------------
    // StreamAsync fell-off-log path (lines 274-277)
    // ---------------------------------------------------------------------------

    [Test]
    public async Task StreamAsync_sets_FellOffLog_when_drain_reports_fell_off_log()
    {
        // Lines 274-277: when the subscriber's DrainAsync returns FellOffLog = true,
        // StreamAsync sets collector.FellOffLog = true and breaks out of the drain loop.
        var subscriber = Substitute.For<IWalSubscriber>();
        subscriber.DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalDrainResult { FellOffLog = true }));

        var collector = MakeCollector(subscriber);

        await foreach (var _ in collector.StreamAsync(CancellationToken.None))
        {
            // Drain to completion.
        }

        Assert.That(collector.FellOffLog, Is.True);
    }

    // ---------------------------------------------------------------------------
    // Atomic writes in the delta window (issue #4589)
    // ---------------------------------------------------------------------------

    [Test]
    public async Task An_aborted_sagas_prepared_write_is_not_captured()
    {
        // The #4441 review's probe: a prepared Set followed by its TxAbort.
        var txId = Guid.NewGuid();
        var collector = await DrainAsync(
            decisions: new Dictionary<Guid, TxStatus> { [txId] = TxStatus.Aborted },
            Prepared(txId, "k", index: 0, size: 1, offset: 1),
            Terminal(txId, MutationKind.TxAbort, offset: 2));

        Assert.Multiple(() =>
        {
            Assert.That(collector.KeyDescriptors, Is.Empty,
                "an aborted saga's prepared write must not be captured by an incremental backup");
            Assert.That(collector.RequiresSagaFallback, Is.False);
            Assert.That(collector.BlockedFloor, Is.Null, "an aborted saga holds nothing back");
        });
    }

    [Test]
    public async Task An_undecided_sagas_prepared_writes_are_left_out_and_hold_the_frontier_back()
    {
        var txId = Guid.NewGuid();
        var collector = await DrainAsync(
            decisions: new Dictionary<Guid, TxStatus>(),
            Ordinary("before", offset: 0),
            Prepared(txId, "a", index: 0, size: 2, offset: 1),
            Ordinary("between", offset: 2),
            Prepared(txId, "b", index: 1, size: 2, offset: 3),
            Ordinary("after", offset: 4));

        Assert.Multiple(() =>
        {
            Assert.That(collector.KeyDescriptors.Select(d => d.Key), Is.EqualTo(new[] { "before", "between", "after" }),
                "an undecided saga's writes are not captured; ordinary writes are");
            Assert.That(collector.NewPartitionOffsets()[0], Is.EqualTo(1L),
                "the next increment must resume at the undecided saga's first prepare");
            Assert.That(collector.BlockedFloor, Is.EqualTo(Hlc(1)), "the held prepare's timestamp pins the WAL");
            Assert.That(collector.RequiresSagaFallback, Is.False);
        });
    }

    [Test]
    public async Task A_committed_saga_is_captured_whole_once_its_prepares_cover_the_batch()
    {
        var txId = Guid.NewGuid();
        var collector = await DrainAsync(
            decisions: new Dictionary<Guid, TxStatus> { [txId] = TxStatus.Committed },
            Prepared(txId, "a", index: 0, size: 2, offset: 0),
            Prepared(txId, "b", index: 1, size: 2, offset: 1),
            Terminal(txId, MutationKind.TxCommit, offset: 2, shardCount: 1));

        Assert.Multiple(() =>
        {
            Assert.That(collector.KeyDescriptors.Select(d => d.Key).OrderBy(k => k, StringComparer.Ordinal),
                Is.EqualTo(new[] { "a", "b" }));
            Assert.That(collector.NewPartitionOffsets()[0], Is.EqualTo(3L), "a settled saga holds nothing back");
            Assert.That(collector.BlockedFloor, Is.Null);
        });
    }

    [Test]
    public async Task A_committed_saga_whose_prepares_precede_the_window_forces_a_full_backup()
    {
        var txId = Guid.NewGuid();
        var collector = await DrainAsync(
            decisions: new Dictionary<Guid, TxStatus> { [txId] = TxStatus.Committed },
            Prepared(txId, "b", index: 1, size: 2, offset: 0),
            Terminal(txId, MutationKind.TxCommit, offset: 1, shardCount: 1));

        Assert.Multiple(() =>
        {
            Assert.That(collector.RequiresSagaFallback, Is.True,
                "half of a committed batch in the window cannot be captured whole by a delta");
            Assert.That(collector.KeyDescriptors, Is.Empty, "the half batch is never emitted");
        });
    }

    [Test]
    public async Task An_out_of_scope_prepare_still_counts_towards_its_batch()
    {
        var txId = Guid.NewGuid();
        var collector = await DrainAsync(
            decisions: new Dictionary<Guid, TxStatus> { [txId] = TxStatus.Committed },
            startInclusive: "m",
            Prepared(txId, "a", index: 0, size: 2, offset: 0),
            Prepared(txId, "z", index: 1, size: 2, offset: 1),
            Terminal(txId, MutationKind.TxCommit, offset: 2, shardCount: 1));

        Assert.Multiple(() =>
        {
            Assert.That(collector.RequiresSagaFallback, Is.False, "the batch is whole in the window");
            Assert.That(collector.KeyDescriptors.Select(d => d.Key), Is.EqualTo(new[] { "z" }),
                "only the in-scope half is emitted");
        });
    }

    [Test]
    public async Task A_saga_the_base_held_undecided_is_looked_up_even_with_no_record_in_the_window()
    {
        var committed = Guid.NewGuid();
        var undecided = Guid.NewGuid();
        var subscriber = Substitute.For<IWalSubscriber>();
        subscriber.DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new WalDrainResult { EntriesRead = 0 }));

        var collector = MakeCollector(
            subscriber,
            decisions: new Dictionary<Guid, TxStatus> { [committed] = TxStatus.Committed },
            baseUndecided: new[] { committed, undecided });
        await foreach (var _ in collector.StreamAsync(CancellationToken.None))
        {
        }

        Assert.Multiple(() =>
        {
            Assert.That(collector.RequiresSagaFallback, Is.True,
                "a saga the base held pre-saga, committed since, cannot be captured by an empty delta");
            Assert.That(collector.CarriedUndecided, Is.EqualTo(new[] { undecided }),
                "a saga still undecided is handed on to the next increment");
        });
    }

    // Drains the scripted entries through StreamAsync as one page from partition 0.
    private async Task<IncrementalDeltaCollector> DrainAsync(
        IReadOnlyDictionary<Guid, TxStatus> decisions,
        params WalSubscriptionEntry[] entries) =>
        await DrainAsync(decisions, startInclusive: null, entries);

    private async Task<IncrementalDeltaCollector> DrainAsync(
        IReadOnlyDictionary<Guid, TxStatus> decisions,
        string? startInclusive,
        params WalSubscriptionEntry[] entries)
    {
        var subscriber = Substitute.For<IWalSubscriber>();
        var pass = 0;
        subscriber.DrainAsync(Arg.Any<WalSubscriptionContext>(), Arg.Any<IWalSubscriptionHandler>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                if (pass++ > 0)
                {
                    return Task.FromResult(new WalDrainResult { EntriesRead = 0 });
                }

                var handler = call.ArgAt<IWalSubscriptionHandler>(1);
                foreach (var entry in entries)
                {
                    handler.OnEntry(entry);
                }

                return Task.FromResult(new WalDrainResult
                {
                    EntriesRead = entries.Length,
                    AdvancedOffsets = new Dictionary<int, long> { [0] = entries.Max(e => e.Offset) },
                });
            });

        var collector = MakeCollector(subscriber, startInclusive: startInclusive, decisions: decisions);
        await foreach (var _ in collector.StreamAsync(CancellationToken.None))
        {
        }

        return collector;
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = 1_000 + ticks };

    private static WalSubscriptionEntry Ordinary(string key, long offset) =>
        new(0, offset, new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = key,
            Value = [1],
            Timestamp = Hlc(offset),
            TransactionId = Guid.NewGuid(),
        });

    private static WalSubscriptionEntry Prepared(Guid txId, string key, int index, int size, long offset) =>
        new(0, offset, new LatticeMutation
        {
            TreeId = "orders",
            Kind = MutationKind.Set,
            Key = key,
            Value = [2],
            Timestamp = Hlc(offset),
            TransactionId = txId,
            IsPrepared = true,
            AtomicBatchIndex = index,
            AtomicBatchSize = size,
        });

    private static WalSubscriptionEntry Terminal(Guid txId, MutationKind kind, long offset, int shardCount = 0) =>
        new(0, offset, new LatticeMutation
        {
            TreeId = "orders",
            Kind = kind,
            Key = "0",
            Timestamp = Hlc(offset),
            TransactionId = txId,
            ShardIndex = 0,
            AtomicShardCount = shardCount,
        });
}
