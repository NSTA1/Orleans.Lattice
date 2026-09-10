namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Fakes;

/// <summary>
/// A clock that advances by a fixed step on every reading, so a wall-clock budget
/// is exercised by counting readings rather than by waiting.
/// <para>
/// The count walk charges the clock once per key, so "the key cost two seconds" is
/// expressed here as a two second step: a fixture states a per-key cost and a
/// budget, and the number of keys the walk consumes follows deterministically. A
/// settable clock advanced from the outside could not do that, because the walk
/// never yields to the test between keys.
/// </para>
/// </summary>
internal sealed class SteppingTimeProvider(TimeSpan step) : TimeProvider
{
    private long _ticks;

    /// <summary>How many times the clock has been read.</summary>
    internal int Readings { get; private set; }

    /// <summary>
    /// Ticks, so a step of one tick is expressible and the arithmetic in
    /// <see cref="TimeProvider.GetElapsedTime(long)"/> is exact rather than scaled
    /// through a lossy frequency.
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
