using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Default <see cref="IUiSessionStore"/>: an in-memory map of string key to
/// boxed value. Registered with a scoped lifetime so each user session keeps its
/// own transient UI state and it is dropped when the session ends.
/// </summary>
public sealed class UiSessionStore : IUiSessionStore
{
    private readonly ConcurrentDictionary<string, object?> _values = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public bool TryGet<T>(string key, out T value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (_values.TryGetValue(key, out var stored) && stored is T typed)
        {
            value = typed;
            return true;
        }

        value = default!;
        return false;
    }

    /// <inheritdoc />
    public T GetOrDefault<T>(string key, T fallback = default!)
        => TryGet<T>(key, out var value) ? value : fallback;

    /// <inheritdoc />
    public void Set<T>(string key, T value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _values[key] = value;
    }

    /// <inheritdoc />
    public void Remove(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _values.TryRemove(key, out _);
    }
}
