namespace Orleans.Lattice.Tests.Views;

using Orleans.Lattice.Views;

/// <summary>
/// Unit tests for <see cref="ViewBackpressure"/>, the pure mapping from a source
/// tree's <see cref="WalSaturationState"/> onto the view maintainer's self-throttle
/// decisions (scaled drain batch + background-tick deferral).
/// </summary>
[TestFixture]
public class ViewBackpressureTests
{
    [Test]
    public void ScaleBatch_healthy_returns_full_batch()
    {
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Healthy, 256, 0.5d, 16), Is.EqualTo(256));
    }

    [Test]
    public void ScaleBatch_throttled_applies_ratio_with_ceiling()
    {
        // ceil(256 * 0.5) = 128.
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Throttled, 256, 0.5d, 16), Is.EqualTo(128));
        // ceil(10 * 0.25) = 3 (rounds up, never down to a starving 2).
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Throttled, 10, 0.25d, 16), Is.EqualTo(3));
    }

    [Test]
    public void ScaleBatch_throttled_clamps_ratio_into_unit_interval()
    {
        // A ratio above 1 cannot inflate the batch above its configured cap.
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Throttled, 256, 5d, 16), Is.EqualTo(256));
        // A negative ratio floors at a single entry, never zero.
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Throttled, 256, -1d, 16), Is.EqualTo(1));
    }

    [Test]
    public void ScaleBatch_saturated_uses_drip_feed_clamped_to_batch()
    {
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Saturated, 256, 0.5d, 16), Is.EqualTo(16));
        // The drip-feed can never exceed the configured full batch.
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Saturated, 8, 0.5d, 16), Is.EqualTo(8));
        // A non-positive drip-feed floors at a single entry.
        Assert.That(ViewBackpressure.ScaleBatch(WalSaturationState.Saturated, 256, 0.5d, 0), Is.EqualTo(1));
    }

    [Test]
    public void PauseMs_healthy_is_zero()
    {
        Assert.That(ViewBackpressure.PauseMs(WalSaturationState.Healthy, 50, 500), Is.Zero);
    }

    [Test]
    public void PauseMs_maps_each_regime_to_its_configured_pause()
    {
        Assert.That(ViewBackpressure.PauseMs(WalSaturationState.Throttled, 50, 500), Is.EqualTo(50));
        Assert.That(ViewBackpressure.PauseMs(WalSaturationState.Saturated, 50, 500), Is.EqualTo(500));
    }

    [Test]
    public void PauseMs_non_positive_configured_pause_disables_deferral()
    {
        Assert.That(ViewBackpressure.PauseMs(WalSaturationState.Throttled, 0, 500), Is.Zero);
        Assert.That(ViewBackpressure.PauseMs(WalSaturationState.Saturated, 50, -10), Is.Zero);
    }
}
