namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Collects every <see cref="WalSaturationStateChange"/> it observes
/// into an in-memory list, for use in unit tests that assert on the
/// publish side-effect of <see cref="IWalSaturationObserver"/>.
/// </summary>
internal sealed class RecordingWalSaturationObserver : IWalSaturationObserver
{
    private readonly List<WalSaturationStateChange> _changes = [];
    private readonly object _lock = new();

    /// <summary>The transitions captured so far, in publish order.</summary>
    public IReadOnlyList<WalSaturationStateChange> Changes
    {
        get { lock (_lock) return _changes.ToArray(); }
    }

    /// <inheritdoc />
    public ValueTask OnStateChangedAsync(WalSaturationStateChange change, CancellationToken cancellationToken)
    {
        lock (_lock) _changes.Add(change);
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Throws a configurable exception on every call, for testing the
/// dispatcher's swallow-and-log semantics.
/// </summary>
internal sealed class ThrowingWalSaturationObserver(Exception? toThrow = null) : IWalSaturationObserver
{
    private readonly Exception _toThrow = toThrow ?? new InvalidOperationException("test observer failure");

    /// <inheritdoc />
    public ValueTask OnStateChangedAsync(WalSaturationStateChange change, CancellationToken cancellationToken) =>
        throw _toThrow;
}
