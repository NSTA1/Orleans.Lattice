namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="BoundedExponentialBackoff"/>, the schedule shared by
/// <see cref="BoundedExponentialRetryPolicy"/> and the bootstrap coordinator's
/// drain retry loop.
/// </summary>
[TestFixture]
public class BoundedExponentialBackoffTests
{
    [Test]
    public void Compute_doubles_from_the_initial_delay_until_the_max()
    {
        var initial = TimeSpan.FromMilliseconds(50);
        var max = TimeSpan.FromSeconds(1);

        Assert.Multiple(() =>
        {
            Assert.That(BoundedExponentialBackoff.Compute(1, initial, max), Is.EqualTo(TimeSpan.FromMilliseconds(50)));
            Assert.That(BoundedExponentialBackoff.Compute(2, initial, max), Is.EqualTo(TimeSpan.FromMilliseconds(100)));
            Assert.That(BoundedExponentialBackoff.Compute(5, initial, max), Is.EqualTo(TimeSpan.FromMilliseconds(800)));
            Assert.That(BoundedExponentialBackoff.Compute(6, initial, max), Is.EqualTo(max));
            Assert.That(BoundedExponentialBackoff.Compute(1000, initial, max), Is.EqualTo(max));
        });
    }

    [Test]
    public void Compute_returns_the_max_rather_than_a_wrapped_zero_when_the_product_overflows()
    {
        // 2^34 ticks * 2^30 = 2^64, which wraps to exactly zero in unchecked
        // long arithmetic; the old schedule returned a zero delay here.
        var initial = TimeSpan.FromTicks(1L << 34);
        var max = TimeSpan.FromDays(10);

        Assert.That(BoundedExponentialBackoff.Compute(31, initial, max), Is.EqualTo(max));
    }

    [Test]
    public void Compute_returns_the_max_rather_than_a_wrapped_small_delay_when_the_product_overflows()
    {
        // (2^34 + 1) * 2^30 wraps to 2^30 ticks (about 107 seconds), positive
        // and below the max, so a sign check alone cannot catch it.
        var initial = TimeSpan.FromTicks((1L << 34) + 1);
        var max = TimeSpan.FromDays(10);

        Assert.That(BoundedExponentialBackoff.Compute(31, initial, max), Is.EqualTo(max));
    }

    [Test]
    public void Compute_never_exceeds_the_longest_delay_Task_Delay_accepts()
    {
        var delay = BoundedExponentialBackoff.Compute(1, TimeSpan.FromDays(100), TimeSpan.MaxValue);

        Assert.That(delay, Is.EqualTo(BoundedExponentialBackoff.MaxTaskDelay));
        Assert.That(() => { _ = Task.Delay(delay, new CancellationToken(canceled: true)); }, Throws.Nothing);
    }

    [Test]
    public void Compute_keeps_doubling_past_the_32nd_attempt_while_below_the_max()
    {
        // The old schedule saturated to the max at attempt 32 regardless of the
        // documented min(max, initial * 2^(attempt-1)).
        var delay = BoundedExponentialBackoff.Compute(32, TimeSpan.FromTicks(1), TimeSpan.FromDays(1));

        Assert.That(delay, Is.EqualTo(TimeSpan.FromTicks(1L << 31)));
    }

    [Test]
    public void Compute_returns_zero_for_a_zero_initial_delay_at_any_attempt()
    {
        Assert.That(BoundedExponentialBackoff.Compute(64, TimeSpan.Zero, TimeSpan.FromSeconds(2)), Is.EqualTo(TimeSpan.Zero));
    }
}
