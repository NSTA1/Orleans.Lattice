namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// A <see cref="TimeProvider"/> whose clock only moves when a test moves it.
/// </summary>
/// <remarks>
/// The backfill stamps its checkpoint with the time each pass finished. Reading
/// that stamp against the system clock would make an assertion about it a race;
/// against this it is an equality check. Nothing in the crawl waits on time, so
/// this is the only place time enters the tests at all.
/// </remarks>
internal sealed class StubTimeProvider : TimeProvider
{
    /// <summary>The instant every call reports until a test changes it.</summary>
    internal DateTimeOffset Now { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => Now;

    /// <summary>Moves the clock forward.</summary>
    /// <param name="delta">How far to advance.</param>
    internal void Advance(TimeSpan delta) => Now += delta;
}
