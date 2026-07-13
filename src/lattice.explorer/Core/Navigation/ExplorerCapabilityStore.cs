namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The default in-memory <see cref="IExplorerCapabilityStore"/>. Session-scoped:
/// the map lives for the lifetime of the session and is discarded when it ends.
/// Thread-safe for the single-writer / many-reader shell usage.
/// </summary>
public sealed class ExplorerCapabilityStore : IExplorerCapabilityStore
{
    private volatile ExplorerCapabilities _current = ExplorerCapabilities.Empty;

    /// <inheritdoc />
    public ExplorerCapabilities Current => _current;

    /// <inheritdoc />
    public event Action? Changed;

    /// <inheritdoc />
    public void Set(ExplorerCapabilities capabilities)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        _current = capabilities;
        Changed?.Invoke();
    }

    /// <inheritdoc />
    public void Reset()
    {
        _current = ExplorerCapabilities.Empty;
        Changed?.Invoke();
    }
}
