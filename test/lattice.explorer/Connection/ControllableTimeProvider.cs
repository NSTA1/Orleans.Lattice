namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// A <see cref="TimeProvider"/> whose wall clock is advanced manually, so the
/// connection's degrade window can be exercised deterministically. Timer and
/// delay scheduling fall through to the base (real) implementation, keeping the
/// short retry backoffs responsive in tests.
/// </summary>
internal sealed class ControllableTimeProvider(DateTimeOffset start) : TimeProvider
{
    private long _ticks = start.UtcTicks;

    public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

    public void Advance(TimeSpan delta) => Interlocked.Add(ref _ticks, delta.Ticks);
}
