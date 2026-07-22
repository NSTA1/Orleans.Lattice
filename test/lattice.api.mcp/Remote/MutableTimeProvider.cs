namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// A <see cref="TimeProvider"/> whose current instant is set by the test, so
/// token-expiry and refresh-skew behaviour can be exercised deterministically
/// without real waiting.
/// </summary>
internal sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
{
    private DateTimeOffset _now = now;

    public override DateTimeOffset GetUtcNow() => _now;

    /// <summary>Moves the clock forward by <paramref name="delta"/>.</summary>
    public void Advance(TimeSpan delta) => _now += delta;
}
