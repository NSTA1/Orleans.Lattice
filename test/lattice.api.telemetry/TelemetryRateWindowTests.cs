namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Covers the rate-window ladder: the derivation that keeps a rate honest at every
/// zoom level while staying a deterministic function of the clamped step.
/// </summary>
[TestFixture]
public sealed class TelemetryRateWindowTests
{
    [Test]
    public void For_step_falls_back_to_the_default_for_a_non_positive_step()
    {
        Assert.Multiple(() =>
        {
            Assert.That(TelemetryRateWindow.ForStep(TimeSpan.Zero), Is.EqualTo(TelemetryRateWindow.Default));
            Assert.That(
                TelemetryRateWindow.ForStep(TimeSpan.FromSeconds(-30)),
                Is.EqualTo(TelemetryRateWindow.Default));
        });
    }

    [TestCase(1, "1m")]
    [TestCase(15, "1m")]
    [TestCase(20, "2m")]
    [TestCase(60, "5m")]
    [TestCase(120, "10m")]
    [TestCase(300, "30m")]
    [TestCase(900, "1h")]
    [TestCase(3600, "6h")]
    public void For_step_rounds_up_to_the_nearest_ladder_entry(int stepSeconds, string expected)
    {
        Assert.That(
            TelemetryRateWindow.ForStep(TimeSpan.FromSeconds(stepSeconds)),
            Is.EqualTo(expected));
    }

    [Test]
    public void For_step_clamps_to_the_widest_ladder_entry()
    {
        Assert.That(TelemetryRateWindow.ForStep(TimeSpan.FromDays(7)), Is.EqualTo("24h"));
    }

    [Test]
    public void For_step_is_a_pure_function_of_its_input()
    {
        var step = TimeSpan.FromSeconds(45);

        Assert.That(
            TelemetryRateWindow.ForStep(step),
            Is.EqualTo(TelemetryRateWindow.ForStep(step)),
            "The derivation must not depend on a clock, so a rendered query is reproducible.");
    }

    [Test]
    public void The_window_always_covers_at_least_the_step_itself()
    {
        foreach (var seconds in new[] { 15, 30, 60, 120, 300, 900, 1800, 3600 })
        {
            var step = TimeSpan.FromSeconds(seconds);
            var window = ParseWindow(TelemetryRateWindow.ForStep(step));

            Assert.That(window, Is.GreaterThanOrEqualTo(step),
                $"A rate window narrower than the {step} step would silently under-sample.");
        }
    }

    private static TimeSpan ParseWindow(string text) =>
        text[^1] switch
        {
            'm' => TimeSpan.FromMinutes(int.Parse(text[..^1])),
            'h' => TimeSpan.FromHours(int.Parse(text[..^1])),
            _ => throw new FormatException($"Unexpected window unit in '{text}'."),
        };
}
