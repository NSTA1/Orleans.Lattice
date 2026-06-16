using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Pins the default values for the byte-accurate storage-usage and
/// advisory byte-pressure WAL policy options so a future default flip is a
/// conscious, test-visible change.
/// </summary>
[TestFixture]
public sealed class StorageUsageOptionsDefaultsTests
{
    [Test]
    public void WalMaxRetainedBytes_defaults_to_null_disabling_the_policy()
    {
        var options = new LatticeOptions();
        Assert.That(options.WalMaxRetainedBytes, Is.Null);
    }

    [Test]
    public void WalBytePressureReclaimTarget_defaults_to_zero_point_eight()
    {
        var options = new LatticeOptions();
        Assert.That(options.WalBytePressureReclaimTarget, Is.EqualTo(0.8));
        Assert.That(LatticeOptions.DefaultWalBytePressureReclaimTarget, Is.EqualTo(0.8));
    }

    [Test]
    public void StorageUsageCacheTtl_defaults_to_ten_seconds()
    {
        var options = new LatticeOptions();
        Assert.That(options.StorageUsageCacheTtl, Is.EqualTo(TimeSpan.FromSeconds(10)));
        Assert.That(LatticeOptions.DefaultStorageUsageCacheTtl, Is.EqualTo(TimeSpan.FromSeconds(10)));
    }

    [Test]
    public void StorageUsagePollInterval_defaults_to_fifteen_seconds()
    {
        var options = new LatticeOptions();
        Assert.That(options.StorageUsagePollInterval, Is.EqualTo(TimeSpan.FromSeconds(15)));
        Assert.That(LatticeOptions.DefaultStorageUsagePollInterval, Is.EqualTo(TimeSpan.FromSeconds(15)));
    }

    [Test]
    public void StorageUsageDeepPollInterval_defaults_to_zero_disabling_the_deep_poll()
    {
        var options = new LatticeOptions();
        Assert.That(options.StorageUsageDeepPollInterval, Is.EqualTo(TimeSpan.Zero));
        Assert.That(LatticeOptions.DefaultStorageUsageDeepPollInterval, Is.EqualTo(TimeSpan.Zero));
    }
}
