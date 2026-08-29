using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Exercises <see cref="TelemetryQueryBounds"/>: the three-arm step clamp and every
/// arm of <see cref="TelemetryQueryBounds.Validate(TelemetryTimeRange, DateTimeOffset)"/>,
/// including the precedence between two simultaneously violated bounds. The
/// evaluation instant is passed in rather than read from a clock, so every case is
/// deterministic.
/// </summary>
[TestFixture]
public sealed class TelemetryQueryBoundsTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static TelemetryQueryBounds Bounded() => new()
    {
        MinStep = TimeSpan.FromSeconds(15),
        MaxStep = TimeSpan.FromMinutes(10),
        DefaultStep = TimeSpan.FromMinutes(1),
        MaxRange = TimeSpan.FromHours(6),
        MaxLookback = TimeSpan.FromDays(7),
        MaxPoints = 1000,
    };

    [Test]
    public void Unbounded_is_the_default_value_and_reports_itself_unbounded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryQueryBounds.Unbounded, Is.EqualTo(default(TelemetryQueryBounds)));
            Assert.That(TelemetryQueryBounds.Unbounded.IsUnbounded, Is.True);
        });
    }

    [Test]
    public void IsUnbounded_is_false_once_any_dimension_is_constrained()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new TelemetryQueryBounds { MinStep = TimeSpan.FromSeconds(1) }.IsUnbounded, Is.False);
            Assert.That(new TelemetryQueryBounds { MaxStep = TimeSpan.FromSeconds(1) }.IsUnbounded, Is.False);
            Assert.That(new TelemetryQueryBounds { MaxRange = TimeSpan.FromSeconds(1) }.IsUnbounded, Is.False);
            Assert.That(new TelemetryQueryBounds { MaxLookback = TimeSpan.FromSeconds(1) }.IsUnbounded, Is.False);
            Assert.That(new TelemetryQueryBounds { MaxPoints = 1 }.IsUnbounded, Is.False);
        });
    }

    [Test]
    public void IsUnbounded_ignores_the_default_step_which_constrains_nothing()
    {
        var bounds = new TelemetryQueryBounds { DefaultStep = TimeSpan.FromMinutes(1) };

        Assert.That(bounds.IsUnbounded, Is.True);
    }

    [Test]
    public void EffectiveStep_clamps_all_three_arms()
    {
        var bounds = Bounded();

        Assert.Multiple(() =>
        {
            Assert.That(bounds.EffectiveStep(TimeSpan.Zero), Is.EqualTo(bounds.DefaultStep));
            Assert.That(bounds.EffectiveStep(TimeSpan.FromSeconds(1)), Is.EqualTo(bounds.MinStep));
            Assert.That(bounds.EffectiveStep(TimeSpan.FromHours(1)), Is.EqualTo(bounds.MaxStep));
            Assert.That(bounds.EffectiveStep(TimeSpan.FromMinutes(2)), Is.EqualTo(TimeSpan.FromMinutes(2)));
        });
    }

    [Test]
    public void EffectiveStep_treats_a_negative_request_as_unset()
    {
        var bounds = Bounded();

        Assert.That(bounds.EffectiveStep(TimeSpan.FromSeconds(-30)), Is.EqualTo(bounds.DefaultStep));
    }

    [Test]
    public void EffectiveStep_clamps_an_unset_request_up_when_the_default_is_below_the_minimum()
    {
        var bounds = new TelemetryQueryBounds
        {
            MinStep = TimeSpan.FromMinutes(1),
            DefaultStep = TimeSpan.FromSeconds(5),
        };

        Assert.That(bounds.EffectiveStep(TimeSpan.Zero), Is.EqualTo(TimeSpan.FromMinutes(1)));
    }

    [Test]
    public void EffectiveStep_passes_anything_through_when_unbounded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryQueryBounds.Unbounded.EffectiveStep(TimeSpan.FromSeconds(7)),
                Is.EqualTo(TimeSpan.FromSeconds(7)));
            Assert.That(TelemetryQueryBounds.Unbounded.EffectiveStep(TimeSpan.Zero), Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public void Validate_accepts_a_window_inside_every_bound()
    {
        var range = TelemetryTimeRange.Between(Now.AddHours(-1), Now, TimeSpan.FromMinutes(1));

        Assert.That(Bounded().Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.None));
    }

    [Test]
    public void Validate_accepts_an_unset_step_rather_than_reporting_it_below_the_minimum()
    {
        var range = TelemetryTimeRange.Between(Now.AddHours(-1), Now, TimeSpan.Zero);

        Assert.That(Bounded().Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.None));
    }

    [Test]
    public void Validate_rejects_a_descending_window()
    {
        var range = TelemetryTimeRange.Between(Now, Now.AddHours(-1), TimeSpan.FromMinutes(1));

        Assert.That(Bounded().Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.RangeNotAscending));
    }

    [Test]
    public void Validate_rejects_a_step_below_the_minimum()
    {
        var range = TelemetryTimeRange.Between(Now.AddHours(-1), Now, TimeSpan.FromSeconds(1));

        Assert.That(Bounded().Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.StepBelowMinimum));
    }

    [Test]
    public void Validate_rejects_a_negative_step_even_when_unbounded()
    {
        var range = TelemetryTimeRange.Between(Now.AddHours(-1), Now, TimeSpan.FromSeconds(-1));

        Assert.That(TelemetryQueryBounds.Unbounded.Validate(range, Now),
            Is.EqualTo(TelemetryBoundsViolation.StepBelowMinimum));
    }

    [Test]
    public void Validate_rejects_a_step_above_the_maximum()
    {
        var range = TelemetryTimeRange.Between(Now.AddHours(-1), Now, TimeSpan.FromMinutes(30));

        Assert.That(Bounded().Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.StepAboveMaximum));
    }

    [Test]
    public void Validate_rejects_a_window_longer_than_the_maximum_range()
    {
        var range = TelemetryTimeRange.Between(Now.AddHours(-7), Now, TimeSpan.FromMinutes(5));

        Assert.That(Bounded().Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.RangeTooLong));
    }

    [Test]
    public void Validate_rejects_a_window_reaching_past_the_maximum_lookback()
    {
        var bounds = Bounded() with { MaxRange = TimeSpan.Zero, MaxPoints = 0 };
        var range = TelemetryTimeRange.Between(Now.AddDays(-8), Now.AddDays(-8).AddHours(1), TimeSpan.FromMinutes(5));

        Assert.That(bounds.Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.LookbackTooOld));
    }

    [Test]
    public void Validate_rejects_a_window_exceeding_the_point_budget()
    {
        var bounds = new TelemetryQueryBounds { MaxPoints = 10 };
        var range = TelemetryTimeRange.Between(Now.AddHours(-1), Now, TimeSpan.FromMinutes(1));

        Assert.That(bounds.Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.TooManyPoints));
    }

    [Test]
    public void Validate_accepts_a_window_exactly_on_the_point_budget()
    {
        var bounds = new TelemetryQueryBounds { MaxPoints = 61 };
        var range = TelemetryTimeRange.Between(Now.AddHours(-1), Now, TimeSpan.FromMinutes(1));

        Assert.That(bounds.Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.None));
    }

    [Test]
    public void Validate_reports_the_earlier_bound_when_two_are_violated_together()
    {
        var bounds = Bounded();
        var range = TelemetryTimeRange.Between(Now.AddHours(-7), Now, TimeSpan.FromSeconds(1));

        Assert.That(bounds.Validate(range, Now), Is.EqualTo(TelemetryBoundsViolation.StepBelowMinimum),
            "The declared precedence order must be stable so a rejection reason does not drift.");
    }

    [Test]
    public void Validate_accepts_any_well_formed_window_when_unbounded()
    {
        var range = TelemetryTimeRange.Between(Now.AddYears(-5), Now, TimeSpan.FromSeconds(1));

        Assert.That(TelemetryQueryBounds.Unbounded.Validate(range, Now),
            Is.EqualTo(TelemetryBoundsViolation.None));
    }
}
