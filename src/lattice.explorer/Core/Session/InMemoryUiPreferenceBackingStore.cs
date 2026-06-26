using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// A non-durable <see cref="IUiPreferenceBackingStore"/> that keeps the document
/// in process memory. Registered as the fallback so the preference store always
/// resolves (and so tests can exercise it without a browser); a host overrides it
/// with a genuinely durable backing store (browser <c>localStorage</c> on the web
/// head, the platform preference store on the desktop head).
/// </summary>
public sealed class InMemoryUiPreferenceBackingStore : IUiPreferenceBackingStore
{
    private readonly ConcurrentDictionary<string, string> _values = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task<string?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Task.FromResult(_values.TryGetValue(key, out var value) ? value : null);
    }

    /// <inheritdoc />
    public Task SetAsync(string key, string value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _values[key] = value;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _values.TryRemove(key, out _);
        return Task.CompletedTask;
    }
}
