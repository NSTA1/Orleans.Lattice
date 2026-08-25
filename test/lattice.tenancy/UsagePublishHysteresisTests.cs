using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="UsagePublishHysteresis"/>: the pure gate that
/// suppresses a negligible movement and admits a significant one. The significance
/// band on each dimension is the larger of the absolute floor and the relative
/// fraction of the last published value.
/// </summary>
[TestFixture]
public sealed class UsagePublishHysteresisTests
{
    [Test]
    public void No_movement_is_suppressed()
    {
        var sample = Sample(1_000, 10, 100, 1);

        Assert.That(
            UsagePublishHysteresis.ShouldPublish(sample, sample, minAbsoluteDelta: 64 * 1024, minRelativeDelta: 0.05),
            Is.False);
    }

    [Test]
    public void A_movement_below_the_absolute_floor_is_suppressed()
    {
        var last = Sample(bytes: 1_000);
        var candidate = Sample(bytes: 2_000); // +1000, below the 64 KiB floor

        Assert.That(
            UsagePublishHysteresis.ShouldPublish(last, candidate, minAbsoluteDelta: 64 * 1024, minRelativeDelta: 0.05),
            Is.False);
    }

    [Test]
    public void A_movement_clearing_the_absolute_floor_publishes()
    {
        var last = Sample(bytes: 0);
        var candidate = Sample(bytes: 128 * 1024); // +128 KiB, clears the 64 KiB floor

        Assert.That(
            UsagePublishHysteresis.ShouldPublish(last, candidate, minAbsoluteDelta: 64 * 1024, minRelativeDelta: 0.05),
            Is.True);
    }

    [Test]
    public void The_band_is_the_larger_of_absolute_and_relative()
    {
        // last is large, so the relative fraction (5% of 10,000,000 = 500,000)
        // dominates the small absolute floor. A 100,000 move is below it.
        var last = Sample(bytes: 10_000_000);

        Assert.Multiple(() =>
        {
            Assert.That(
                UsagePublishHysteresis.ShouldPublish(last, Sample(bytes: 10_100_000), minAbsoluteDelta: 1_024, minRelativeDelta: 0.05),
                Is.False,
                "a 100k move is below the 5% (500k) relative band");
            Assert.That(
                UsagePublishHysteresis.ShouldPublish(last, Sample(bytes: 10_600_000), minAbsoluteDelta: 1_024, minRelativeDelta: 0.05),
                Is.True,
                "a 600k move clears the 5% (500k) relative band");
        });
    }

    [Test]
    public void Any_single_dimension_clearing_the_band_publishes()
    {
        var last = Sample(1_000, 10, 100, 1);
        var candidate = Sample(1_000, 10, 100, 100); // only tree count moves, by 99

        Assert.That(
            UsagePublishHysteresis.ShouldPublish(last, candidate, minAbsoluteDelta: 50, minRelativeDelta: 0.0),
            Is.True);
    }

    [Test]
    public void A_decrease_clearing_the_band_publishes()
    {
        var last = Sample(bytes: 200 * 1024);
        var candidate = Sample(bytes: 0); // -200 KiB

        Assert.That(
            UsagePublishHysteresis.ShouldPublish(last, candidate, minAbsoluteDelta: 64 * 1024, minRelativeDelta: 0.05),
            Is.True);
    }

    [Test]
    public void Negative_thresholds_are_treated_as_zero_so_any_nonzero_move_publishes()
    {
        var last = Sample(bytes: 100);
        var candidate = Sample(bytes: 101);

        Assert.That(
            UsagePublishHysteresis.ShouldPublish(last, candidate, minAbsoluteDelta: -1, minRelativeDelta: -1.0),
            Is.True);
    }
}
