namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A <see cref="TimeProvider"/> whose clock moves only when a test moves it, so a
/// behaviour defined in minutes can be asserted in milliseconds of test time.
/// <para>
/// Both the wall clock (<see cref="GetUtcNow"/>) and the high-resolution timestamp
/// (<see cref="GetTimestamp"/>) advance together off the same offset, so a duration
/// measured with <see cref="TimeProvider.GetElapsedTime(long)"/> is exactly the
/// span the test advanced. That is what makes an assertion about a heartbeat's
/// silence floor - or about which phase an elapsed figure is relative to -
/// deterministic rather than a race against the machine.
/// </para>
/// </summary>
internal sealed class AdvanceableTimeProvider : TimeProvider
{
    private readonly DateTimeOffset _start;
    private long _advancedTicks;

    /// <summary>Creates a provider parked at a fixed, arbitrary wall-clock instant.</summary>
    internal AdvanceableTimeProvider()
        => _start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <inheritdoc />
    /// <remarks>
    /// One tick per unit, so <see cref="TimeProvider.GetElapsedTime(long)"/> converts
    /// the timestamp delta straight back into the advanced <see cref="TimeSpan"/>.
    /// </remarks>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => _start.AddTicks(Volatile.Read(ref _advancedTicks));

    /// <inheritdoc />
    public override long GetTimestamp() => Volatile.Read(ref _advancedTicks);

    /// <summary>Moves the clock forward by <paramref name="delta"/>.</summary>
    internal void Advance(TimeSpan delta) => Interlocked.Add(ref _advancedTicks, delta.Ticks);
}
