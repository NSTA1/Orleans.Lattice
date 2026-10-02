namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// The advisory threshold of a budget whose scaled product is below one byte.
/// <para>
/// The threshold was <c>(long)(budget * ratio)</c>, which truncates to zero for
/// any budget smaller than <c>1 / ratio</c>. The aggregate comparison is guarded
/// only on the budget, so it then reported the estate over threshold with
/// nothing retained; the per-account comparison is guarded on the threshold
/// itself, so the same account never reported over threshold however much it
/// retained. A small advisory ratio makes that reachable with ordinary ceilings.
/// </para>
/// </summary>
public sealed partial class StoragePressureCollectorTests
{
    // 1_000 bytes at a ratio of 0.0001 scales to 0.1 bytes, which used to
    // truncate to a threshold of zero.
    private const long SubByteCeiling = 1_000L;
    private const double SubByteRatio = 0.0001;

    [Test]
    public async Task A_sub_byte_threshold_does_not_report_an_empty_budget_over_threshold()
    {
        var sample = Sample([Default], Tree("t1", 0L, ceiling: SubByteCeiling, partitions: (0, Default)));
        var collector = Collector(new FakeSource(sample), configure: o => o.RetainedBytesAdvisoryRatio = SubByteRatio);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.OverThreshold, Is.False, "Nothing is retained, so no budget can be exceeded.");
            Assert.That(Account(pressure, Default).OverThreshold, Is.False);
            Assert.That(Account(pressure, Default).Classification, Is.EqualTo(WalPressureClassification.None));
        });
    }

    [Test]
    public async Task A_sub_byte_threshold_reports_an_account_over_threshold_once_it_retains_a_byte()
    {
        var sample = Sample([Default], Tree("t1", 1L, ceiling: SubByteCeiling, partitions: (0, Default)));
        var collector = Collector(new FakeSource(sample), configure: o => o.RetainedBytesAdvisoryRatio = SubByteRatio);

        var pressure = await collector.CollectAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(pressure.OverThreshold, Is.True);
            Assert.That(
                Account(pressure, Default).OverThreshold,
                Is.True,
                "The account and the aggregate weigh the same bytes against the same budget, so they must agree.");
            Assert.That(
                Account(pressure, Default).Classification,
                Is.EqualTo(WalPressureClassification.CapacityBound));
        });
    }
}
