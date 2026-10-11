namespace Orleans.Lattice.Tests;

public sealed partial class LatticeStorageUsageMetricsTests
{
    private const string PhysicalBytesName = LatticeMetrics.StorageWalPhysicalBytesName;

    [TestCase(false)]
    [TestCase(true)]
    public void Publish_physical_bytes_surfaces_the_measured_occupancy(bool walOnly)
    {
        using var sut = new LatticeStorageUsageMetrics();
        var tree = $"physical-{Guid.NewGuid():N}";
        if (walOnly)
        {
            sut.PublishWal(new TreeWalUsageReport
            {
                TreeId = tree, WalRetainedBytes = 100, WalPhysicalBytes = 800,
            });
        }
        else
        {
            sut.Publish(Report(tree, 100, 40, 25) with { WalPhysicalBytes = 800, TotalBytes = 865 });
        }

        Assert.Multiple(() =>
        {
            Assert.That(Read(PhysicalBytesName, tree), Is.EqualTo(800));
            Assert.That(Read(LatticeMetrics.StorageWalBytesName, tree), Is.EqualTo(100),
                "the legacy series must continue to measure retained payload");
        });
    }

    [Test]
    public void PublishWal_after_deep_publish_preserves_the_physical_total()
    {
        using var sut = new LatticeStorageUsageMetrics();
        var tree = $"physical-{Guid.NewGuid():N}";
        sut.Publish(Report(tree, 100, 40, 25) with { WalPhysicalBytes = 800, TotalBytes = 865 });
        sut.PublishWal(new TreeWalUsageReport
        {
            TreeId = tree, WalRetainedBytes = 150, WalPhysicalBytes = 900,
        });

        Assert.Multiple(() =>
        {
            Assert.That(Read(LatticeMetrics.StorageTotalBytesName, tree), Is.EqualTo(965),
                "a cheap poll must not replace physical occupancy with retained payload");
            Assert.That(Read(LatticeMetrics.StorageUsageDeepPublishedName, tree), Is.EqualTo(1));
            Assert.That(Read(LatticeMetrics.StorageSnapshotBytesName, tree), Is.EqualTo(40));
            Assert.That(Read(LatticeMetrics.StorageLeafStateBytesName, tree), Is.EqualTo(25));
        });
    }

    [Test]
    public void Physical_gauge_partial_unobserved_stale_and_disposed_reports_are_absent()
    {
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        using var sut = new LatticeStorageUsageMetrics(clock) { StalenessHorizon = TimeSpan.FromSeconds(30) };
        var tree = $"physical-{Guid.NewGuid():N}";
        Assert.That(Read(PhysicalBytesName, tree), Is.Null);
        sut.Publish(Report(tree, 100, 0, 0, partial: true) with { WalPhysicalBytes = 800 });
        Assert.That(Read(PhysicalBytesName, tree), Is.Null);

        sut.Publish(Report(tree, 100, 0, 0) with { WalPhysicalBytes = 800 });
        sut.PublishWal(new TreeWalUsageReport { TreeId = tree, WalRetainedBytes = 100, WalPhysicalBytes = 800 });
        Assert.That(Read(PhysicalBytesName, tree), Is.EqualTo(800));
        sut.PublishWal(new TreeWalUsageReport { TreeId = tree, Partial = true });
        Assert.That(Read(PhysicalBytesName, tree), Is.EqualTo(800));
        clock.Advance(TimeSpan.FromSeconds(31));
        Assert.That(Read(PhysicalBytesName, tree), Is.Null);

        sut.PublishWal(new TreeWalUsageReport { TreeId = tree, WalPhysicalBytes = 900 });
        Assert.That(Read(PhysicalBytesName, tree), Is.EqualTo(900));
        sut.Dispose();
        Assert.That(Read(PhysicalBytesName, tree), Is.Null);
    }
}
