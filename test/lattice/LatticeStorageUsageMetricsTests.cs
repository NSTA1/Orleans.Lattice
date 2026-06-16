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
    public void PublishWal_surfaces_only_the_wal_bytes_gauge_without_clobbering_deep_values()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        // Seed a deep publish so snapshot / leaf-state / total surfaces
        // have a value.
        sut.Publish(Report(tree, wal: 100, snap: 40, leaf: 25));

        // A subsequent WAL-only publish refreshes only the WAL bytes;
        // snapshot and leaf-state must be preserved from the deep publish,
        // and total bytes must be recomputed against the new WAL value.
        sut.PublishWal(new TreeWalUsageReport
        {
            TreeId = tree,
            WalRetainedBytes = 150,
            Partial = false,
            SampledAt = DateTimeOffset.UtcNow,
        });

        Assert.Multiple(() =>
        {
            Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(150));
            Assert.That(Read(LatticeMetrics.StorageSnapshotBytesName, tree), Is.EqualTo(40),
                "WAL-only publish must not clobber the snapshot surface");
            Assert.That(Read(LatticeMetrics.StorageLeafStateBytesName, tree), Is.EqualTo(25),
                "WAL-only publish must not clobber the leaf-state surface");
            Assert.That(Read(LatticeMetrics.StorageTotalBytesName, tree), Is.EqualTo(215),
                "total must be recomputed against the fresh WAL value");
        });
    }

    [Test]
    public void PublishWal_without_prior_deep_publish_seeds_wal_bytes_only()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.PublishWal(new TreeWalUsageReport
        {
            TreeId = tree,
            WalRetainedBytes = 500,
            Partial = false,
            SampledAt = DateTimeOffset.UtcNow,
        });

        Assert.Multiple(() =>
        {
            Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(500));
            Assert.That(Read(LatticeMetrics.StorageSnapshotBytesName, tree), Is.EqualTo(0));
            Assert.That(Read(LatticeMetrics.StorageLeafStateBytesName, tree), Is.EqualTo(0));
            Assert.That(Read(LatticeMetrics.StorageTotalBytesName, tree), Is.EqualTo(500));
        });
    }

    [Test]
    public void PublishWal_partial_does_not_overwrite_existing_value()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.Publish(Report(tree, wal: 100, snap: 40, leaf: 25));
        sut.PublishWal(new TreeWalUsageReport
        {
            TreeId = tree,
            WalRetainedBytes = 0,
            Partial = true,
            SampledAt = DateTimeOffset.UtcNow,
        });

        Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(100),
            "a partial WAL publish must not clobber the last-known good value with zero");
    }

    [Test]
    public void PublishWal_null_tree_id_throws()
    {
        var sut = new LatticeStorageUsageMetrics();
        var report = new TreeWalUsageReport { TreeId = null! };
        Assert.That(() => sut.PublishWal(report), Throws.TypeOf<ArgumentNullException>());
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

    [Test]
    public void Gauges_union_series_published_through_separate_instances()
    {
        // Models two co-hosted silos in one process: each silo's DI singleton
        // is a distinct sink instance. A scrape of the process-wide gauge must
        // surface trees published through either instance, not just the most
        // recently constructed one.
        var siloA = new LatticeStorageUsageMetrics();
        var siloB = new LatticeStorageUsageMetrics();
        var treeA = $"sg-{Guid.NewGuid():N}";
        var treeB = $"sg-{Guid.NewGuid():N}";

        siloA.Publish(Report(treeA, wal: 10, snap: 0, leaf: 0));
        siloB.Publish(Report(treeB, wal: 20, snap: 0, leaf: 0));

        Assert.Multiple(() =>
        {
            Assert.That(Read(LatticeMetrics.StorageWalBytesName, treeA), Is.EqualTo(10),
                "a tree published through the first instance must still be observed after a later instance is constructed");
            Assert.That(Read(LatticeMetrics.StorageWalBytesName, treeB), Is.EqualTo(20),
                "a tree published through the second instance must be observed too");
        });
    }

    [Test]
    public void Disposed_instance_stops_contributing_to_gauges()
    {
        var sut = new LatticeStorageUsageMetrics();
        var tree = $"sg-{Guid.NewGuid():N}";

        sut.Publish(Report(tree, wal: 100, snap: 40, leaf: 25));
        Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(100));

        sut.Dispose();

        Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.Null,
            "a disposed sink must stop contributing its published series to the gauges");
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
