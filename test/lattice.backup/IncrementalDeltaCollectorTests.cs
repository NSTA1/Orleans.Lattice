using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
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
public sealed class IncrementalDeltaCollectorTests
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
        string? endExclusive = null)
    {
        subscriber ??= Substitute.For<IWalSubscriber>();
        return new IncrementalDeltaCollector(
            _serializer,
            subscriber,
            treeId: "orders",
            consumerId: "test-consumer",
            partitions: 1,
            baseOffsets: new Dictionary<int, long>(),
            startInclusive: startInclusive,
            endExclusive: endExclusive,
            mergeMode: BackupKeyMergeMode.LastWriterWins,
            baseBackupId: "base-id",
            batchSize: 100);
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
}
