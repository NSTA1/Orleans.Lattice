using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the sender-side AIMD batch-size controller
/// (<see cref="AdaptiveBatchSizeController"/>). Exercises the
/// additive-increase / multiplicative-decrease rule, the
/// <c>[1, maxBatchSize]</c> clamp, the sliding-window mean, error-driven
/// decrease, and constructor argument validation.
/// </summary>
[TestFixture]
public sealed class AdaptiveBatchSizeControllerTests
{
    private static AdaptiveBatchSizeController CreateController(
        int maxBatchSize = 256,
        int additiveIncrement = 8,
        double multiplicativeDecreaseFactor = 0.5,
        TimeSpan? latencyThreshold = null,
        int windowLength = 16) =>
        new(
            maxBatchSize,
            additiveIncrement,
            multiplicativeDecreaseFactor,
            latencyThreshold ?? TimeSpan.FromMilliseconds(50),
            windowLength);

    [Test]
    public void Constructor_starts_effective_size_at_max_batch_size()
    {
        var controller = CreateController(maxBatchSize: 64);
        Assert.That(controller.CurrentBatchSize, Is.EqualTo(64));
    }

    [Test]
    public void Constructor_has_no_window_latency_before_any_ack()
    {
        var controller = CreateController();
        Assert.That(controller.WindowAckLatency, Is.Null);
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void Constructor_rejects_non_positive_max_batch_size(int maxBatchSize) =>
        Assert.That(() => CreateController(maxBatchSize: maxBatchSize),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    [TestCase(0)]
    [TestCase(-1)]
    public void Constructor_rejects_non_positive_additive_increment(int increment) =>
        Assert.That(() => CreateController(additiveIncrement: increment),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    [TestCase(0.0)]
    [TestCase(-0.1)]
    [TestCase(1.0)]
    [TestCase(1.5)]
    [TestCase(double.NaN)]
    public void Constructor_rejects_decrease_factor_outside_open_unit_interval(double factor) =>
        Assert.That(() => CreateController(multiplicativeDecreaseFactor: factor),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    [Test]
    public void Constructor_rejects_non_positive_latency_threshold() =>
        Assert.That(() => CreateController(latencyThreshold: TimeSpan.Zero),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    [TestCase(0)]
    [TestCase(-1)]
    public void Constructor_rejects_non_positive_window_length(int windowLength) =>
        Assert.That(() => CreateController(windowLength: windowLength),
            Throws.InstanceOf<ArgumentOutOfRangeException>());

    [Test]
    public void RecordAck_stays_at_max_when_already_at_ceiling_on_fast_ack()
    {
        var controller = CreateController(maxBatchSize: 32, latencyThreshold: TimeSpan.FromMilliseconds(50));

        controller.RecordAck(TimeSpan.FromMilliseconds(1));

        Assert.That(controller.CurrentBatchSize, Is.EqualTo(32),
            "a fast ack at the ceiling keeps the effective size pinned at max");
    }

    [Test]
    public void RecordAck_grows_additively_after_a_backoff_on_fast_acks()
    {
        var controller = CreateController(
            maxBatchSize: 256,
            additiveIncrement: 8,
            multiplicativeDecreaseFactor: 0.5,
            latencyThreshold: TimeSpan.FromMilliseconds(50),
            windowLength: 1);

        // Drive a slow ack to back off from the ceiling first.
        controller.RecordAck(TimeSpan.FromMilliseconds(500));
        var afterBackoff = controller.CurrentBatchSize;
        Assert.That(afterBackoff, Is.EqualTo(128),
            "a slow ack halves the effective size from the 256 ceiling");

        // Now a fast ack must grow it additively by the increment.
        controller.RecordAck(TimeSpan.FromMilliseconds(1));
        Assert.That(controller.CurrentBatchSize, Is.EqualTo(afterBackoff + 8),
            "a fast ack grows the effective size by the additive increment");
    }

    [Test]
    public void RecordAck_decreases_multiplicatively_on_slow_ack()
    {
        var controller = CreateController(
            maxBatchSize: 100,
            multiplicativeDecreaseFactor: 0.5,
            latencyThreshold: TimeSpan.FromMilliseconds(50),
            windowLength: 1);

        controller.RecordAck(TimeSpan.FromMilliseconds(500));

        Assert.That(controller.CurrentBatchSize, Is.EqualTo(50),
            "a slow ack multiplies the effective size by the decrease factor");
    }

    [Test]
    public void RecordAck_additive_increase_is_capped_at_max_batch_size()
    {
        var controller = CreateController(
            maxBatchSize: 10,
            additiveIncrement: 8,
            latencyThreshold: TimeSpan.FromMilliseconds(50),
            windowLength: 1);

        // Back off then re-grow twice; the increase must never exceed max.
        controller.RecordAck(TimeSpan.FromMilliseconds(500)); // -> 5
        controller.RecordAck(TimeSpan.FromMilliseconds(1));   // -> 5 + 8 capped to 10
        controller.RecordAck(TimeSpan.FromMilliseconds(1));   // -> stays at 10

        Assert.That(controller.CurrentBatchSize, Is.EqualTo(10));
    }

    [Test]
    public void RecordAck_multiplicative_decrease_is_floored_at_one()
    {
        var controller = CreateController(
            maxBatchSize: 4,
            multiplicativeDecreaseFactor: 0.5,
            latencyThreshold: TimeSpan.FromMilliseconds(50),
            windowLength: 1);

        for (var i = 0; i < 20; i++)
        {
            controller.RecordAck(TimeSpan.FromMilliseconds(500));
        }

        Assert.That(controller.CurrentBatchSize, Is.EqualTo(1),
            "repeated back-offs floor the effective size at 1, never below");
    }

    [Test]
    public void RecordError_decreases_multiplicatively()
    {
        var controller = CreateController(maxBatchSize: 80, multiplicativeDecreaseFactor: 0.5);

        controller.RecordError();

        Assert.That(controller.CurrentBatchSize, Is.EqualTo(40),
            "an error shrinks the effective size the same way a slow ack does");
    }

    [Test]
    public void RecordError_is_floored_at_one()
    {
        var controller = CreateController(maxBatchSize: 4, multiplicativeDecreaseFactor: 0.5);

        for (var i = 0; i < 20; i++)
        {
            controller.RecordError();
        }

        Assert.That(controller.CurrentBatchSize, Is.EqualTo(1));
    }

    [Test]
    public void WindowAckLatency_reports_sliding_window_mean()
    {
        var controller = CreateController(windowLength: 2, latencyThreshold: TimeSpan.FromSeconds(5));

        controller.RecordAck(TimeSpan.FromMilliseconds(10));
        controller.RecordAck(TimeSpan.FromMilliseconds(30));

        Assert.That(controller.WindowAckLatency, Is.EqualTo(TimeSpan.FromMilliseconds(20)),
            "the diagnostic reports the mean of the two windowed samples");
    }

    [Test]
    public void WindowAckLatency_evicts_oldest_sample_when_window_is_full()
    {
        var controller = CreateController(windowLength: 2, latencyThreshold: TimeSpan.FromSeconds(5));

        controller.RecordAck(TimeSpan.FromMilliseconds(10));
        controller.RecordAck(TimeSpan.FromMilliseconds(30));
        // The third sample evicts the first (10), leaving {30, 50} -> mean 40.
        controller.RecordAck(TimeSpan.FromMilliseconds(50));

        Assert.That(controller.WindowAckLatency, Is.EqualTo(TimeSpan.FromMilliseconds(40)));
    }

    [Test]
    public void RecordAck_floors_negative_latency_at_zero()
    {
        var controller = CreateController(windowLength: 1, latencyThreshold: TimeSpan.FromMilliseconds(50));

        // A negative (clock-skew) sample is floored at zero, which is
        // below the threshold and therefore a healthy ack.
        controller.RecordAck(TimeSpan.FromMilliseconds(-100));

        Assert.That(controller.WindowAckLatency, Is.EqualTo(TimeSpan.Zero));
    }

    [Test]
    public void RecordAck_uses_window_mean_not_single_sample_for_aimd_decision()
    {
        // A long window smooths a single transient spike: with fifteen
        // fast acks already in a sixteen-sample window, one slow ack
        // barely moves the mean and must not trip a back-off.
        var controller = CreateController(
            maxBatchSize: 256,
            multiplicativeDecreaseFactor: 0.5,
            latencyThreshold: TimeSpan.FromMilliseconds(50),
            windowLength: 16);

        for (var i = 0; i < 15; i++)
        {
            controller.RecordAck(TimeSpan.FromMilliseconds(1));
        }
        // One transient spike; mean = (15*1 + 200)/16 ~= 13.4 ms < 50 ms.
        controller.RecordAck(TimeSpan.FromMilliseconds(200));

        Assert.That(controller.CurrentBatchSize, Is.EqualTo(256),
            "a single spike inside a long window must not trip the back-off");
    }
}

