using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the three storage-usage paths the main
/// <see cref="LatticeStorageUsageGrainTests"/> fixture does not reach: the
/// work-bounded re-anchor walk an operator-forced refresh drives (issue #1972),
/// the WAL fan-out's cancellation and failure arms, and the byte-pressure
/// over-threshold gauge that only publishes once the WAL surface is fully
/// accounted.
/// </summary>
[TestFixture]
public sealed class LatticeStorageUsageGrainRefreshTests
{
    private string TreeId = null!;

    [SetUp]
    public void NewTreeId() => TreeId = $"usage-refresh-{Guid.NewGuid():N}";

    private readonly List<IDisposable> _disposables = [];

    [TearDown]
    public void DisposeMeters()
    {
        foreach (var d in _disposables)
        {
            d.Dispose();
        }
        _disposables.Clear();
    }

    private sealed record Harness(
        LatticeStorageUsageGrain Grain,
        IShardRootGrain Shard,
        IWalShardGrain Wal,
        LatticeStorageUsageMetrics Metrics);

    /// <summary>
    /// One shard root and one WAL partition, both substituted, so a test can
    /// script the bounded-refresh page sequence or fail a single surface.
    /// </summary>
    private Harness CreateGrain(LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("ol.lsu", TreeId));

        var factory = Substitute.For<IGrainFactory>();
        options ??= new LatticeOptions();
        options.WalPartitions = 1;

        var lattice = Substitute.For<ILattice>();
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new RoutingInfo(TreeId, ShardMap.CreateDefault(16, 1)));
        factory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var shard = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>($"{TreeId}/0").Returns(shard);

        var wal = Substitute.For<IWalShardGrain>();
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(0L);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        var usageMetrics = new LatticeStorageUsageMetrics();
        var admissionMetrics = new LatticeAdmissionMetrics();
        _disposables.Add(usageMetrics);
        _disposables.Add(admissionMetrics);

        var grain = new LatticeStorageUsageGrain(
            context,
            factory,
            TestOptionsResolver.ForFactory(factory, options),
            usageMetrics,
            admissionMetrics,
            NullLogger<LatticeStorageUsageGrain>.Instance);

        return new Harness(grain, shard, wal, usageMetrics);
    }

    /// <summary>
    /// Reads the current value of the 0/1 <c>storage.policy.over_threshold</c>
    /// observable gauge for a tree, or <see langword="null"/> when the tree has
    /// no measurement (never evaluated, or deliberately left unpublished).
    /// </summary>
    private static long? ReadPolicyGauge(string tree)
    {
        long? found = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter)
                    && inst.Name == LatticeMetrics.StoragePolicyOverThresholdName)
                {
                    l.EnableMeasurementEvents(inst);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var t in tags)
            {
                if (t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree)
                {
                    found = value;
                }
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return found;
    }

    private static ShardStorageUsagePage Page(long leaf, long snapshot, long liveKeys, string? resumeFrom) =>
        new()
        {
            Usage = new ShardStorageUsage
            {
                LeafStateBytes = leaf,
                SnapshotBytes = snapshot,
                LiveKeys = liveKeys,
            },
            ResumeFromInclusive = resumeFrom,
        };

    // ------------------------------------------- work-bounded forced refresh

    [Test]
    public async Task A_forced_refresh_sums_every_bounded_batch_in_the_leaf_chain()
    {
        // Each batch releases the shard, so the whole-chain figure is only
        // correct if the caller keeps driving batches until the shard reports no
        // continuation. Stopping at the first page would under-report the tree.
        var h = CreateGrain();
        h.Shard.RefreshLeafByteFootprintsBoundedAsync(
                Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>())
            .Returns(
                Page(leaf: 100, snapshot: 10, liveKeys: 3, resumeFrom: "k1"),
                Page(leaf: 200, snapshot: 20, liveKeys: 5, resumeFrom: "k2"),
                Page(leaf: 300, snapshot: 30, liveKeys: 7, resumeFrom: null));

        var report = await h.Grain.GetReportAsync(forceRefresh: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.LeafStateBytes, Is.EqualTo(600), "all three batches must be summed");
            Assert.That(report.SnapshotBytes, Is.EqualTo(60));
            Assert.That(report.LiveKeys, Is.EqualTo(15));
            Assert.That(report.Partial, Is.False, "every surface answered");
        });
        await h.Shard.Received(3).RefreshLeafByteFootprintsBoundedAsync(
            Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_forced_refresh_threads_the_running_total_and_cursor_into_each_batch()
    {
        // The shard re-anchors its activation-scoped totals from the whole-chain
        // figure on the final batch, so it has to be handed the running total -
        // never a partial sum it would then mistake for the whole chain.
        var h = CreateGrain();
        var cursors = new List<string?>();
        var totals = new List<ShardStorageUsage>();
        var call = 0;
        h.Shard.RefreshLeafByteFootprintsBoundedAsync(
                Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                cursors.Add((string?)ci[0]);
                totals.Add((ShardStorageUsage)ci[1]!);
                call++;
                return call switch
                {
                    1 => Page(leaf: 100, snapshot: 1, liveKeys: 2, resumeFrom: "k1"),
                    2 => Page(leaf: 50, snapshot: 2, liveKeys: 3, resumeFrom: "k2"),
                    _ => Page(leaf: 25, snapshot: 4, liveKeys: 4, resumeFrom: null),
                };
            });

        await h.Grain.GetReportAsync(forceRefresh: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(cursors, Is.EqualTo(new string?[] { null, "k1", "k2" }),
                "each batch must resume from the cursor the previous one returned");
            Assert.That(totals.Select(t => t.LeafStateBytes), Is.EqualTo(new long[] { 0, 100, 150 }),
                "the running total handed to each batch must be the sum of every batch before it");
            Assert.That(totals.Select(t => t.LiveKeys), Is.EqualTo(new long[] { 0, 2, 5 }));
        });
    }

    [Test]
    public async Task A_forced_refresh_stops_after_one_batch_when_the_chain_fits_in_it()
    {
        var h = CreateGrain();
        h.Shard.RefreshLeafByteFootprintsBoundedAsync(
                Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>())
            .Returns(Page(leaf: 42, snapshot: 7, liveKeys: 1, resumeFrom: null));

        var report = await h.Grain.GetReportAsync(forceRefresh: true, CancellationToken.None);

        Assert.That(report.LeafStateBytes, Is.EqualTo(42));
        await h.Shard.Received(1).RefreshLeafByteFootprintsBoundedAsync(
            Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_forced_refresh_that_fails_mid_chain_flags_the_report_partial()
    {
        // A shard that dies part-way through its chain has contributed a partial
        // sum. Publishing that as a complete figure would understate the tree
        // while still presenting the total as authoritative.
        var h = CreateGrain();
        var call = 0;
        h.Shard.RefreshLeafByteFootprintsBoundedAsync(
                Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>())
            .Returns(_ => ++call == 1
                ? Page(leaf: 100, snapshot: 10, liveKeys: 3, resumeFrom: "k1")
                : throw new InvalidOperationException("shard went away mid-walk"));

        var report = await h.Grain.GetReportAsync(forceRefresh: true, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.LeafStateBytes, Is.Zero,
                "a shard that did not finish contributes nothing rather than a partial sum");
        });
    }

    [Test]
    public void A_forced_refresh_observes_cancellation_between_batches()
    {
        // The walk is unbounded in batch count, so the per-batch cancellation
        // check is the only thing that stops a cancelled refresh from walking an
        // entire leaf chain it has already been told to abandon.
        var h = CreateGrain();
        using var cts = new CancellationTokenSource();
        h.Shard.RefreshLeafByteFootprintsBoundedAsync(
                Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return Page(leaf: 100, snapshot: 10, liveKeys: 3, resumeFrom: "k1");
            });

        Assert.ThrowsAsync<OperationCanceledException>(
            () => h.Grain.GetReportAsync(forceRefresh: true, cts.Token));

        h.Shard.Received(1).RefreshLeafByteFootprintsBoundedAsync(
            Arg.Any<string?>(), Arg.Any<ShardStorageUsage>(), Arg.Any<CancellationToken>());
    }

    // ------------------------------------------------------- WAL fan-out arms

    [Test]
    public void A_cancelled_WAL_fanout_propagates_rather_than_reporting_no_data()
    {
        // The -1 "no data" sentinel means "this surface could not answer", which
        // the report renders as Partial. A caller-requested cancellation is not
        // that: swallowing it would hand back a report the caller no longer
        // wants and quietly mark the WAL surface unsupported.
        var h = CreateGrain();
        using var cts = new CancellationTokenSource();
        h.Wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>())
            .Returns<Task<long>>(_ =>
            {
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });
        h.Shard.GetStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ShardStorageUsage { LeafStateBytes = 1, SnapshotBytes = 1, LiveKeys = 1 });

        Assert.ThrowsAsync<OperationCanceledException>(
            () => h.Grain.GetReportAsync(forceRefresh: false, cts.Token));
    }

    [Test]
    public async Task A_WAL_fanout_that_throws_without_cancellation_reports_no_data()
    {
        // A transient WAL failure must contribute the -1 sentinel, not a wrong
        // zero, so the assembled report is flagged Partial instead of claiming
        // the tree retains no WAL bytes at all.
        var h = CreateGrain();
        h.Wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>())
            .Returns<Task<long>>(_ => throw new TimeoutException("wal partition unreachable"));
        h.Shard.GetStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ShardStorageUsage { LeafStateBytes = 64, SnapshotBytes = 8, LiveKeys = 2 });

        var report = await h.Grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.WalRetainedBytes, Is.Zero);
            Assert.That(report.LeafStateBytes, Is.EqualTo(64), "the shard surface still contributes");
        });
    }

    // ------------------------------------------------- byte-pressure gauge

    [Test]
    public async Task The_over_threshold_gauge_publishes_once_the_WAL_surface_is_fully_accounted()
    {
        var h = CreateGrain(new LatticeOptions { WalMaxRetainedBytes = 1_000 });
        h.Shard.GetStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ShardStorageUsage { LeafStateBytes = 10, SnapshotBytes = 0, LiveKeys = 1 });
        h.Wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(4_096L);

        var report = await h.Grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.False);
            Assert.That(report.WalRetainedBytes, Is.EqualTo(4_096));
            Assert.That(ReadPolicyGauge(TreeId), Is.EqualTo(1),
                "retained WAL bytes above the configured ceiling must raise the gauge");
        });
    }

    [Test]
    public async Task The_over_threshold_gauge_clears_when_retention_is_under_the_ceiling()
    {
        var h = CreateGrain(new LatticeOptions { WalMaxRetainedBytes = 1_000_000 });
        h.Shard.GetStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ShardStorageUsage { LeafStateBytes = 10, SnapshotBytes = 0, LiveKeys = 1 });
        h.Wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(4_096L);

        await h.Grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.That(ReadPolicyGauge(TreeId), Is.EqualTo(0));
    }

    [Test]
    public async Task The_over_threshold_gauge_is_left_untouched_when_the_WAL_surface_did_not_answer()
    {
        // A gauge that reads "not over threshold" because the WAL surface failed
        // is worse than no reading at all: it tells an operator the tree is
        // healthy on exactly the signal that could not be measured.
        var h = CreateGrain(new LatticeOptions { WalMaxRetainedBytes = 1_000 });
        h.Shard.GetStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ShardStorageUsage { LeafStateBytes = 10, SnapshotBytes = 0, LiveKeys = 1 });
        h.Wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>())
            .Returns<Task<long>>(_ => throw new TimeoutException("wal partition unreachable"));

        var report = await h.Grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(ReadPolicyGauge(TreeId), Is.Null,
                "an unmeasured WAL surface must leave the gauge unpublished, not publish a false negative");
        });
    }

    [Test]
    public async Task A_cached_report_still_republishes_the_over_threshold_gauge()
    {
        // The gauge is observable and scraped on the meter's schedule, not the
        // grain's, so a cache hit has to re-push the last reading or the gauge
        // would go silent for the whole cache window.
        var h = CreateGrain(new LatticeOptions
        {
            WalMaxRetainedBytes = 1_000,
            StorageUsageCacheTtl = TimeSpan.FromMinutes(10),
        });
        h.Shard.GetStorageUsageAsync(Arg.Any<CancellationToken>())
            .Returns(new ShardStorageUsage { LeafStateBytes = 10, SnapshotBytes = 0, LiveKeys = 1 });
        h.Wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(4_096L);

        var first = await h.Grain.GetReportAsync(forceRefresh: false, CancellationToken.None);
        var second = await h.Grain.GetReportAsync(forceRefresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(second.SampledAt, Is.EqualTo(first.SampledAt), "the second read must be a cache hit");
            Assert.That(ReadPolicyGauge(TreeId), Is.EqualTo(1));
        });
        await h.Wal.Received(1).GetRetainedByteSizeAsync(Arg.Any<CancellationToken>());
    }
}
