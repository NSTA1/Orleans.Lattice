namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A range entry that declares no step ceiling passes the caller's step through
/// unclamped, so the facade must not do arithmetic on that step that can overflow
/// before the deployment-wide step guardrail gets to reject it. Driven against
/// purpose-built entries, because every curated entry declares a step ceiling.
/// </summary>
public sealed partial class LatticeTelemetryTests
{
    private const string NoStepCeiling = "overflow.no_step_ceiling";

    private static TelemetryQueryBounds NoStepCeilingBounds(int maxPoints = 0) => new()
    {
        MinStep = TimeSpan.FromMinutes(1),
        DefaultStep = TimeSpan.FromMinutes(1),
        MaxPoints = maxPoints,
    };

    [Test]
    public void QueryAsync_rejects_a_step_too_large_to_derive_a_rate_window_from_at_the_host_guardrail()
    {
        // The window is explicit, so no default span is derived: the only arithmetic on
        // the unclamped step is the rate-window derivation, which multiplies it by four.
        var harness = new TelemetryFacadeHarness()
            .WithDefinitions(RangeEntry(NoStepCeiling, NoStepCeilingBounds()));
        var request = new TelemetryQueryRequest
        {
            QueryId = NoStepCeiling,
            Range = TelemetryTimeRange.Between(
                FixedTimeProvider.Instant.AddHours(-1), FixedTimeProvider.Instant, TimeSpan.MaxValue),
        };

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await harness.Build().QueryAsync(request),
                Throws.TypeOf<TelemetryQueryBoundsException>()
                    .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                    .EqualTo(TelemetryBoundsViolation.StepAboveMaximum),
                "a step the deployment guardrail refuses must be reported as a bounds violation, "
                + "not escape as an arithmetic overflow");
            Assert.That(harness.Backend.Queries, Is.Empty);
        });
    }

    [Test]
    public void QueryAsync_rejects_a_step_too_large_to_derive_a_default_window_from_at_the_host_guardrail()
    {
        // No start is supplied, so the facade derives the window from the entry's point
        // budget at the requested step: step * (MaxPoints - 1). One hundred thousand days
        // is representable, but ninety-nine of them are not.
        var harness = new TelemetryFacadeHarness()
            .WithDefinitions(RangeEntry(NoStepCeiling, NoStepCeilingBounds(maxPoints: 100)));
        var request = new TelemetryQueryRequest
        {
            QueryId = NoStepCeiling,
            Range = new TelemetryTimeRange { Step = TimeSpan.FromDays(200_000) },
        };

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await harness.Build().QueryAsync(request),
                Throws.TypeOf<TelemetryQueryBoundsException>()
                    .With.Property(nameof(TelemetryQueryBoundsException.Violation))
                    .EqualTo(TelemetryBoundsViolation.StepAboveMaximum));
            Assert.That(harness.Backend.Queries, Is.Empty);
        });
    }

    [Test]
    public async Task A_default_window_derived_from_a_large_point_budget_is_clamped_to_the_deployment_range()
    {
        // A step the host guardrail admits, over a point budget large enough that
        // step * (MaxPoints - 1) is not representable, still derives a window: the span
        // saturates and is then clamped by the deployment range, rather than overflowing.
        var harness = new TelemetryFacadeHarness()
            .WithOptions(options =>
            {
                options.MaxRange = TimeSpan.FromDays(2);
                options.MaxStep = TimeSpan.FromDays(1);
            })
            .WithDefinitions(RangeEntry(NoStepCeiling, NoStepCeilingBounds(maxPoints: int.MaxValue)));
        var request = new TelemetryQueryRequest
        {
            QueryId = NoStepCeiling,
            Range = new TelemetryTimeRange { Step = TimeSpan.FromHours(12) },
        };

        var response = await harness.Build().QueryAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(response.Range.Duration, Is.EqualTo(TimeSpan.FromDays(2)));
            Assert.That(response.Range.Step, Is.EqualTo(TimeSpan.FromHours(12)));
            Assert.That(harness.Backend.Queries, Has.Count.EqualTo(1));
            Assert.That(harness.Backend.LastWasRange, Is.True);
        });
    }
}
