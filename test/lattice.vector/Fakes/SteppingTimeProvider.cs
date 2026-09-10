namespace Orleans.Lattice.Vector.Tests.Fakes;

/// <summary>
/// A clock that advances by a fixed step on every reading, so a wall-clock budget
/// is exercised by counting readings rather than by waiting.
/// <para>
/// A build step charges the clock once per source item, so "the item cost 200ms"
/// is expressed here as a 200ms step: the fixture states a per-item cost and a
/// budget, and the number of items a step consumes follows deterministically. A
/// fixture that instead advanced a settable clock from the outside could not do
/// that, because the step never yields to the test between items.
/// </para>
/// </summary>
internal sealed class SteppingTimeProvider(TimeSpan step) : TimeProvider
{
    private long _ticks;

    /// <summary>How many times the clock has been read.</summary>
    internal int Readings { get; private set; }

    /// <summary>
    /// Ticks, so a step of one tick is expressible and the arithmetic in
    /// <see cref="TimeProvider.GetElapsedTime(long)"/> is exact rather than
    /// scaled through a lossy frequency.
    /// </summary>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp()
    {
        var now = _ticks;
        Readings++;
        _ticks += step.Ticks;
        return now;
    }
}
