namespace Orleans.Lattice.Caching.AzureBlob.Tests;

/// <summary>
/// Minimal controllable <see cref="TimeProvider"/> for deterministic expiry
/// tests: its UTC clock is whatever <see cref="Now"/> is set to, with no
/// wall-clock or timer behaviour. Tests advance it explicitly instead of
/// sleeping.
/// </summary>
internal sealed class MutableTimeProvider(DateTimeOffset start) : TimeProvider
{
    /// <summary>The current UTC instant this provider reports.</summary>
    public DateTimeOffset Now { get; set; } = start;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => Now;

    /// <summary>Advances the clock by <paramref name="delta"/>.</summary>
    public void Advance(TimeSpan delta) => Now += delta;
}
