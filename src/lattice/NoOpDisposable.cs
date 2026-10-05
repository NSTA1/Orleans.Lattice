namespace Orleans.Lattice;

/// <summary>
/// A shared, stateless <see cref="IDisposable"/> whose <see cref="Dispose"/> does
/// nothing. Returned by scope-opening helpers on their cold path (no ambient
/// state to set or restore) so the caller's <c>using</c> stays unconditional
/// without allocating a scope per call.
/// </summary>
internal sealed class NoOpDisposable : IDisposable
{
    /// <summary>The single shared instance.</summary>
    public static readonly NoOpDisposable Instance = new();

    private NoOpDisposable()
    {
    }

    /// <inheritdoc />
    public void Dispose()
    {
    }
}
