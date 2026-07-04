namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// A minimal manually-advanced <see cref="TimeProvider"/> for deterministic
/// cache-expiry tests: <see cref="GetUtcNow"/> returns a fixed instant that the
/// test advances explicitly. Avoids taking a dependency on an external
/// fake-time package.
/// </summary>
internal sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now += by;
}
