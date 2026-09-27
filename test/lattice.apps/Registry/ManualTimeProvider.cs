namespace Orleans.Lattice.Apps.Tests;

/// <summary>A <see cref="TimeProvider"/> whose clock only moves when a test advances it.</summary>
internal sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now += by;
}
