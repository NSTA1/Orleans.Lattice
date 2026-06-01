using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeStorageUsageMetrics"/>: the observable
/// storage-usage gauge sink. Verifies that published reports surface on the
/// byte gauges, that partial reports report no data rather than a wrong zero,
/// and that the over-threshold flag drives the 0/1 policy gauge.
/// </summary>
[TestFixture]
public sealed class LatticeStorageUsageMetricsTests
{
    private static TreeStorageUsageReport Report(string tree, long wal, long snap, long leaf, bool partial = false) => new()
    {
        TreeId = tree,
        WalRetainedBytes = wal,
        SnapshotBytes = snap,
        LeafStateBytes = leaf,
        TotalBytes = wal + snap + leaf,
        Partial = partial,
        SampledAt = DateTimeOffset.UtcNow,
    };

    private static long? Read(string instrument, string tree)
    {
        long? found = null;
        using var listener = new MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter) && inst.Name == instrument)
                    l.EnableMeasurementEvents(inst);
            },
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, _) =>
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

    [Test]
    public void Publish_surfaces_byte_gauges_for_the_tree()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.Publish(Report(tree, wal: 100, snap: 40, leaf: 25));

        Assert.Multiple(() =>
        {
            Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(100));
            Assert.That(Read(LatticeMetrics.StorageSnapshotBytesName, tree), Is.EqualTo(40));
            Assert.That(Read(LatticeMetrics.StorageLeafStateBytesName, tree), Is.EqualTo(25));
            Assert.That(Read(LatticeMetrics.StorageTotalBytesName, tree), Is.EqualTo(165));
        });
    }

    [Test]
    public void Publish_partial_report_reports_no_data_on_byte_gauges()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.Publish(Report(tree, wal: 100, snap: 40, leaf: 25, partial: true));

        Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.Null,
            "a partial report must not publish a wrong byte count");
    }

    [Test]
    public void PublishOverThreshold_true_reports_one_on_policy_gauge()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.PublishOverThreshold(tree, overThreshold: true);

        Assert.That(Read(LatticeMetrics.StoragePolicyOverThresholdName, tree), Is.EqualTo(1));
    }

    [Test]
    public void PublishOverThreshold_false_reports_zero_on_policy_gauge()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.PublishOverThreshold(tree, overThreshold: false);

        Assert.That(Read(LatticeMetrics.StoragePolicyOverThresholdName, tree), Is.EqualTo(0));
    }

    [Test]
    public void Policy_gauge_reports_no_data_for_unobserved_tree()
    {
        _ = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        Assert.That(Read(LatticeMetrics.StoragePolicyOverThresholdName, tree), Is.Null,
            "a tree never evaluated for byte-pressure must report no measurement");
    }

    [Test]
    public void Publish_null_tree_id_throws()
    {
        var sut = new LatticeStorageUsageMetrics();
        var report = new TreeStorageUsageReport { TreeId = null! };

        Assert.That(() => sut.Publish(report), Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void PublishOverThreshold_null_tree_throws()
    {
        var sut = new LatticeStorageUsageMetrics();

        Assert.That(() => sut.PublishOverThreshold(null!, true), Throws.TypeOf<ArgumentNullException>());
    }

    [Test]
    public void Byte_gauge_drops_series_after_staleness_horizon_elapses()
    {
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var sut = new LatticeStorageUsageMetrics(clock) { StalenessHorizon = TimeSpan.FromSeconds(30) };
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.Publish(Report(tree, wal: 100, snap: 40, leaf: 25));
        Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(100),
            "freshly published series should be observed");

        // Advance past the horizon without re-publishing: this models the
        // tree's aggregator migrating to another silo so this silo stops
        // refreshing the series.
        clock.Advance(TimeSpan.FromSeconds(31));

        Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.Null,
            "a series not refreshed within the horizon must stop being observed so a migrated tree is not double-counted across silos");
    }

    [Test]
    public void Byte_gauge_keeps_series_refreshed_within_horizon()
    {
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var sut = new LatticeStorageUsageMetrics(clock) { StalenessHorizon = TimeSpan.FromSeconds(30) };
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.Publish(Report(tree, wal: 100, snap: 40, leaf: 25));
        clock.Advance(TimeSpan.FromSeconds(20));
        // Re-publish before the horizon elapses (the poller's next tick).
        sut.Publish(Report(tree, wal: 110, snap: 40, leaf: 25));
        clock.Advance(TimeSpan.FromSeconds(20));

        Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(110),
            "a series refreshed within the horizon keeps reporting the latest value");
    }

    [Test]
    public void Policy_gauge_drops_series_after_staleness_horizon_elapses()
    {
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var sut = new LatticeStorageUsageMetrics(clock) { StalenessHorizon = TimeSpan.FromSeconds(30) };
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.PublishOverThreshold(tree, overThreshold: true);
        Assert.That(Read(LatticeMetrics.StoragePolicyOverThresholdName, tree), Is.EqualTo(1));

        clock.Advance(TimeSpan.FromSeconds(31));

        Assert.That(Read(LatticeMetrics.StoragePolicyOverThresholdName, tree), Is.Null,
            "a stale over-threshold series must expire so a migrated tree is not reported by two silos");
    }

    /// <summary>
    /// Minimal mutable <see cref="TimeProvider"/> for driving the sink's
    /// staleness horizon deterministically without a package dependency.
    /// </summary>
    private sealed class MutableTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public void Advance(TimeSpan by) => _now += by;

        public override DateTimeOffset GetUtcNow() => _now;
    }
}
