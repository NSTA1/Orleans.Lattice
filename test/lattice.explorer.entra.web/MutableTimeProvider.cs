namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// Minimal controllable <see cref="TimeProvider"/> for deterministic token-expiry
/// tests: its UTC clock is whatever <see cref="Now"/> is set to. Tests advance it
/// explicitly to drive silent renewal instead of sleeping.
/// </summary>
internal sealed class MutableTimeProvider(DateTimeOffset start) : TimeProvider
{
    public DateTimeOffset Now { get; set; } = start;

    public override DateTimeOffset GetUtcNow() => Now;

    public void Advance(TimeSpan delta) => Now += delta;
}
