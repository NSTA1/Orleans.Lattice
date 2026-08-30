namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="TelemetryRangeGuardrails"/>: a well-ordered, in-budget
/// range is admitted; an inverted range, a non-positive step, an over-budget
/// range, and an over-budget step are each rejected with their own message; and
/// the checks are evaluated in a fixed order so a request violating several is
/// always reported against the same one.
/// </summary>
[TestFixture]
public sealed class TelemetryRangeGuardrailsTests
{
    private static readonly DateTimeOffset Start = DateTimeOffset.FromUnixTimeSeconds(1_000);

    private static LatticeTelemetryOptions Budgets(TimeSpan? maxRange = null, TimeSpan? maxStep = null)
        => new()
        {
            MaxRange = maxRange ?? TimeSpan.FromHours(24),
            MaxStep = maxStep ?? TimeSpan.FromHours(1),
        };

    [Test]
    public void An_in_budget_range_is_admitted_with_no_message()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(), Start, Start.AddHours(1), TimeSpan.FromSeconds(30), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.True);
            Assert.That(message, Is.Null);
        });
    }

    [Test]
    public void A_zero_width_range_is_admitted()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(), Start, Start, TimeSpan.FromSeconds(30), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.True);
            Assert.That(message, Is.Null);
        });
    }

    [Test]
    public void A_range_exactly_at_the_budget_is_admitted()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(maxRange: TimeSpan.FromHours(2)),
            Start,
            Start.AddHours(2),
            TimeSpan.FromHours(1),
            out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.True, "The budget is inclusive, so an exactly-at-budget request fits.");
            Assert.That(message, Is.Null);
        });
    }

    [Test]
    public void An_inverted_range_is_rejected()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(), Start, Start.AddSeconds(-1), TimeSpan.FromSeconds(30), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(message, Is.EqualTo("The range end must be at or after the range start."));
        });
    }

    [Test]
    public void A_zero_step_is_rejected()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(), Start, Start.AddHours(1), TimeSpan.Zero, out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(message, Is.EqualTo("The range step must be strictly positive."));
        });
    }

    [Test]
    public void A_negative_step_is_rejected()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(), Start, Start.AddHours(1), TimeSpan.FromSeconds(-1), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(message, Is.EqualTo("The range step must be strictly positive."));
        });
    }

    [Test]
    public void An_over_budget_range_is_rejected_quoting_both_figures()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(maxRange: TimeSpan.FromHours(1)),
            Start,
            Start.AddHours(2),
            TimeSpan.FromSeconds(30),
            out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(
                message,
                Is.EqualTo(
                    $"The requested range of {TimeSpan.FromHours(2)} exceeds the configured maximum of "
                    + $"{TimeSpan.FromHours(1)}."));
        });
    }

    [Test]
    public void An_over_budget_step_is_rejected_quoting_both_figures()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(maxStep: TimeSpan.FromMinutes(5)),
            Start,
            Start.AddHours(1),
            TimeSpan.FromMinutes(30),
            out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(
                message,
                Is.EqualTo(
                    $"The requested step of {TimeSpan.FromMinutes(30)} exceeds the configured maximum of "
                    + $"{TimeSpan.FromMinutes(5)}."));
        });
    }

    [Test]
    public void Ordering_is_reported_before_step_positivity()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(), Start, Start.AddSeconds(-1), TimeSpan.Zero, out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(message, Is.EqualTo("The range end must be at or after the range start."));
        });
    }

    [Test]
    public void The_range_budget_is_reported_before_the_step_budget()
    {
        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            Budgets(maxRange: TimeSpan.FromMinutes(1), maxStep: TimeSpan.FromSeconds(1)),
            Start,
            Start.AddHours(2),
            TimeSpan.FromMinutes(30),
            out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(message, Does.StartWith("The requested range of"));
        });
    }

    [Test]
    public void A_derived_binding_options_type_supplies_the_same_budgets()
    {
        var options = new DerivedOptions { MaxRange = TimeSpan.FromMinutes(10) };

        var admitted = TelemetryRangeGuardrails.TryValidateRange(
            options, Start, Start.AddHours(1), TimeSpan.FromSeconds(30), out var message);

        Assert.Multiple(() =>
        {
            Assert.That(admitted, Is.False);
            Assert.That(message, Does.Contain("exceeds the configured maximum"));
        });
    }

    [Test]
    public void Null_options_are_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => TelemetryRangeGuardrails.TryValidateRange(
                null!, Start, Start.AddHours(1), TimeSpan.FromSeconds(30), out _));

    private sealed class DerivedOptions : LatticeTelemetryOptions;
}
