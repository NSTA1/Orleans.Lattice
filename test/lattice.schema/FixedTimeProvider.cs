namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// A deterministic <see cref="TimeProvider"/> returning a fixed instant, so
/// dead-letter timestamps are asserted without wall-clock flakiness.
/// </summary>
internal sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => now;
}
