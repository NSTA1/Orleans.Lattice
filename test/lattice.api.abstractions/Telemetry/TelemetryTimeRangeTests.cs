using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Api.Abstractions.Tests.Telemetry;

/// <summary>
/// Exercises <see cref="TelemetryTimeRange"/>'s factories and computed members.
/// Every computed member is a pure function of the three stored fields and reads
/// no ambient clock, so each case here pins an exact expected value rather than a
/// tolerance - there is nothing timing-dependent to be flaky about.
/// </summary>
[TestFixture]
public sealed class TelemetryTimeRangeTests
{
    private static readonly DateTimeOffset Origin = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public void At_sets_both_endpoints_to_the_instant_and_leaves_no_step()
    {
        var range = TelemetryTimeRange.At(Origin);

        Assert.Multiple(() =>
        {
            Assert.That(range.StartUtc, Is.EqualTo(Origin));
            Assert.That(range.EndUtc, Is.EqualTo(Origin));
            Assert.That(range.Step, Is.EqualTo(TimeSpan.Zero));
            Assert.That(range.IsInstant, Is.True);
            Assert.That(range.IsAscending, Is.True);
            Assert.That(range.Duration, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public void Between_preserves_every_supplied_field()
    {
        var range = TelemetryTimeRange.Between(Origin, Origin.AddHours(1), TimeSpan.FromMinutes(5));

        Assert.Multiple(() =>
        {
            Assert.That(range.StartUtc, Is.EqualTo(Origin));
            Assert.That(range.EndUtc, Is.EqualTo(Origin.AddHours(1)));
            Assert.That(range.Step, Is.EqualTo(TimeSpan.FromMinutes(5)));
        });
    }

    [Test]
    public void Duration_is_the_signed_span_between_the_endpoints()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddMinutes(90), TimeSpan.Zero).Duration,
                Is.EqualTo(TimeSpan.FromMinutes(90)));
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddMinutes(-30), TimeSpan.Zero).Duration,
                Is.EqualTo(TimeSpan.FromMinutes(-30)));
        });
    }

    [Test]
    public void IsInstant_is_true_only_for_a_zero_length_window()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryTimeRange.Between(Origin, Origin, TimeSpan.Zero).IsInstant, Is.True);
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddTicks(1), TimeSpan.Zero).IsInstant, Is.False);
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddTicks(-1), TimeSpan.Zero).IsInstant, Is.False);
        });
    }

    [Test]
    public void IsAscending_is_false_only_when_the_window_descends()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddHours(1), TimeSpan.Zero).IsAscending, Is.True);
            Assert.That(TelemetryTimeRange.Between(Origin, Origin, TimeSpan.Zero).IsAscending, Is.True);
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddTicks(-1), TimeSpan.Zero).IsAscending, Is.False);
        });
    }

    [Test]
    public void PointCount_is_zero_for_a_descending_window()
    {
        var range = TelemetryTimeRange.Between(Origin, Origin.AddMinutes(-1), TimeSpan.FromSeconds(30));

        Assert.That(range.PointCount, Is.EqualTo(0));
    }

    [Test]
    public void PointCount_is_one_when_no_step_is_set()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryTimeRange.At(Origin).PointCount, Is.EqualTo(1));
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddHours(1), TimeSpan.Zero).PointCount,
                Is.EqualTo(1));
            Assert.That(TelemetryTimeRange.Between(Origin, Origin.AddHours(1), TimeSpan.FromSeconds(-1)).PointCount,
                Is.EqualTo(1));
        });
    }

    [Test]
    public void PointCount_counts_both_endpoints_of_an_evenly_divided_window()
    {
        var range = TelemetryTimeRange.Between(Origin, Origin.AddMinutes(10), TimeSpan.FromMinutes(1));

        Assert.That(range.PointCount, Is.EqualTo(11));
    }

    [Test]
    public void PointCount_floors_a_window_that_does_not_divide_evenly()
    {
        var range = TelemetryTimeRange.Between(Origin, Origin.AddSeconds(95), TimeSpan.FromSeconds(30));

        Assert.That(range.PointCount, Is.EqualTo(4));
    }

    [Test]
    public void WithStep_replaces_the_step_and_leaves_the_endpoints_untouched()
    {
        var original = TelemetryTimeRange.Between(Origin, Origin.AddHours(1), TimeSpan.FromMinutes(5));

        var restepped = original.WithStep(TimeSpan.FromMinutes(1));

        Assert.Multiple(() =>
        {
            Assert.That(restepped.Step, Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(restepped.StartUtc, Is.EqualTo(original.StartUtc));
            Assert.That(restepped.EndUtc, Is.EqualTo(original.EndUtc));
            Assert.That(original.Step, Is.EqualTo(TimeSpan.FromMinutes(5)),
                "WithStep must not mutate the source value.");
        });
    }

    [Test]
    public void Equal_windows_compare_equal_by_value()
    {
        var a = TelemetryTimeRange.Between(Origin, Origin.AddHours(1), TimeSpan.FromMinutes(5));
        var b = TelemetryTimeRange.Between(Origin, Origin.AddHours(1), TimeSpan.FromMinutes(5));

        Assert.Multiple(() =>
        {
            Assert.That(a, Is.EqualTo(b));
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}
