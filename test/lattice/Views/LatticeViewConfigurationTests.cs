namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for <see cref="LatticeViewOptions"/> and <see cref="LatticeViewDefinition"/>.</summary>
[TestFixture]
public class LatticeViewConfigurationTests
{
    [Test]
    public void Options_defaults_are_batch_size_256_and_50ms_window()
    {
        var options = new LatticeViewOptions();

        Assert.That(options.BatchSize, Is.EqualTo(256));
        Assert.Multiple(() =>
        {
            Assert.That(options.CoalesceWindow, Is.EqualTo(TimeSpan.FromMilliseconds(50)));
            Assert.That(options.ShipViewProducerClusterId, Is.Null);
        });
    }

    [Test]
    public void Definition_binds_name_and_projection()
    {
        var projection = new PredicateLatticeViewProjection();
        var definition = new LatticeViewDefinition("orders-open", projection);

        Assert.That(definition.ViewName, Is.EqualTo("orders-open"));
        Assert.That(definition.Projection, Is.SameAs(projection));
    }

    [Test]
    public void Definition_null_name_throws()
    {
        Assert.That(
            () => new LatticeViewDefinition(null!, new PredicateLatticeViewProjection()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Definition_empty_name_throws()
    {
        Assert.That(
            () => new LatticeViewDefinition(string.Empty, new PredicateLatticeViewProjection()),
            Throws.ArgumentException);
    }

    [Test]
    public void Definition_null_projection_throws()
    {
        Assert.That(
            () => new LatticeViewDefinition("v", (ILatticeViewProjection)null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Options_source_backpressure_defaults_are_obey_half_ratio_drip16()
    {
        var options = new LatticeViewOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.ObeySourceBackpressure, Is.True);
            Assert.That(options.ThrottledBatchRatio, Is.EqualTo(0.5d));
            Assert.That(options.ThrottledPauseMs, Is.EqualTo(50));
            Assert.That(options.SaturatedBatchSize, Is.EqualTo(16));
            Assert.That(options.SaturatedPauseMs, Is.EqualTo(500));
        });
    }

    [Test]
    public void Validator_accepts_default_backpressure_options()
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions());

        Assert.That(result.Succeeded, Is.True);
    }

    [Test]
    public void Validator_rejects_throttled_ratio_outside_unit_interval()
    {
        var validator = new Orleans.Lattice.Views.LatticeViewOptionsValidator();

        Assert.Multiple(() =>
        {
            Assert.That(validator.Validate(null, new LatticeViewOptions { ThrottledBatchRatio = 1.5d }).Failed, Is.True);
            Assert.That(validator.Validate(null, new LatticeViewOptions { ThrottledBatchRatio = -0.1d }).Failed, Is.True);
        });
    }

    [Test]
    public void Validator_rejects_a_NaN_throttled_ratio()
    {
        // NaN compares false against both bounds, so a range check alone admits
        // it; the backpressure scaler then drains one entry per throttled pass.
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions { ThrottledBatchRatio = double.NaN });

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_rejects_non_positive_saturated_batch_size()
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions { SaturatedBatchSize = 0 });

        Assert.That(result.Failed, Is.True);
    }

    /// <summary>
    /// The coalesce window is the view maintainer's grain-timer period, and a grain
    /// timer refuses a period above <c>0xFFFFFFFE</c> ms. A longer window used to
    /// pass validation and then throw every time the maintainer armed its timer,
    /// so the view was never maintained.
    /// </summary>
    [TestCase(49.72)]
    [TestCase(60.0)]
    public void Validator_rejects_a_coalesce_window_above_the_timer_ceiling(double days)
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions { CoalesceWindow = TimeSpan.FromDays(days) });

        Assert.Multiple(() =>
        {
            Assert.That(result.Failed, Is.True);
            Assert.That(result.FailureMessage, Does.Contain(nameof(LatticeViewOptions.CoalesceWindow) + " must be at most"));
        });
    }

    [Test]
    public void Validator_rejects_TimeSpan_MaxValue_as_a_coalesce_window()
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions { CoalesceWindow = TimeSpan.MaxValue });

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_accepts_a_coalesce_window_at_the_timer_ceiling()
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions { CoalesceWindow = TimeSpan.FromMilliseconds(uint.MaxValue - 1) });

        Assert.That(result.Succeeded, Is.True, result.FailureMessage);
    }

    [Test]
    public void Options_default_source_identity_backstop_is_30_seconds()
    {
        Assert.That(
            new LatticeViewOptions().SourceIdentityBackstopInterval,
            Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void Validator_rejects_non_positive_source_identity_backstop()
    {
        var validator = new Orleans.Lattice.Views.LatticeViewOptionsValidator();

        Assert.Multiple(() =>
        {
            Assert.That(
                validator.Validate(null, new LatticeViewOptions { SourceIdentityBackstopInterval = TimeSpan.Zero }).Failed,
                Is.True);
            Assert.That(
                validator.Validate(null, new LatticeViewOptions { SourceIdentityBackstopInterval = TimeSpan.FromSeconds(-1) }).Failed,
                Is.True);
        });
    }

    [TestCase("")]
    [TestCase(" ")]
    public void Validator_rejects_blank_ship_view_producer(string producerClusterId)
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions
            {
                ReplicationMode = LatticeViewReplicationMode.ShipView,
                ShipViewProducerClusterId = producerClusterId,
            });

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_rejects_ship_view_producer_for_derive_locally()
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions { ShipViewProducerClusterId = "site-a" });

        Assert.That(result.Failed, Is.True);
    }

    [Test]
    public void Validator_accepts_ship_view_producer_for_ship_view()
    {
        var result = new Orleans.Lattice.Views.LatticeViewOptionsValidator()
            .Validate(null, new LatticeViewOptions
            {
                ReplicationMode = LatticeViewReplicationMode.ShipView,
                ShipViewProducerClusterId = "site-a",
            });

        Assert.That(result.Succeeded, Is.True);
    }
}
