namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the two no-op collaborators a host may register to keep an axis
/// inert: <see cref="NoOpSplitActivityProbe"/> must never suppress scale-in on
/// split grounds, and <see cref="NoOpStoragePressureCollector"/> must contribute
/// a zero, not-over-threshold <see cref="StoragePressure"/> so the storage axis
/// contributes nothing.
/// </summary>
[TestFixture]
public sealed class NoOpScalingDefaultsTests
{
    [Test]
    public async Task Split_probe_reports_no_split_in_flight()
    {
        ISplitActivityProbe probe = new NoOpSplitActivityProbe();

        Assert.That(await probe.AnySplitInFlightAsync(CancellationToken.None), Is.False);
    }

    [Test]
    public async Task Storage_collector_reports_the_zero_default_pressure()
    {
        IStoragePressureCollector collector = new NoOpStoragePressureCollector();

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure, Is.EqualTo(default(StoragePressure)));
            Assert.That(pressure.OverThreshold, Is.False);
            Assert.That(pressure.WalRetainedBytes, Is.Zero);
            Assert.That(pressure.Accounts, Is.Empty);
            Assert.That(pressure.Recommendation, Is.Null);
        });
    }
}
