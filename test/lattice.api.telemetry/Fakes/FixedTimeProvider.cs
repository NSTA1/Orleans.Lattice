namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// A <see cref="TimeProvider"/> frozen at a fixed instant, so every bounds and
/// lookback assertion is a pure function of its inputs and no test depends on the
/// wall clock.
/// </summary>
internal sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
{
    /// <summary>The instant every test measures from.</summary>
    public static readonly DateTimeOffset Instant =
        new(2026, 1, 15, 12, 0, 0, TimeSpan.Zero);

    /// <summary>A provider frozen at <see cref="Instant"/>.</summary>
    public static FixedTimeProvider AtInstant => new(Instant);

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => now;
}
