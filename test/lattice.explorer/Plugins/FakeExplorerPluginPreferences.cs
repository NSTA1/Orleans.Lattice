using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Plugins;

/// <summary>
/// An in-memory <see cref="IExplorerPluginPreferences"/> that records the exact
/// keys it was asked for, so a test can assert that a plugin-scoped context
/// namespaced them.
/// </summary>
internal sealed class FakeExplorerPluginPreferences : IExplorerPluginPreferences
{
    private readonly Dictionary<string, object?> _values = new(StringComparer.Ordinal);

    public bool IsLoaded { get; private set; }

    /// <summary>Every key this store was asked for, in call order.</summary>
    public List<string> ObservedKeys { get; } = [];

    /// <summary>How many times <see cref="EnsureLoadedAsync"/> was called.</summary>
    public int EnsureLoadedCalls { get; private set; }

    public Task EnsureLoadedAsync(CancellationToken cancellationToken = default)
    {
        EnsureLoadedCalls++;
        IsLoaded = true;
        return Task.CompletedTask;
    }

    public bool TryGet<T>(string key, out T value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ObservedKeys.Add(key);

        if (_values.TryGetValue(key, out var stored) && stored is T typed)
        {
            value = typed;
            return true;
        }

        value = default!;
        return false;
    }

    public T GetOrDefault<T>(string key, T fallback = default!)
    {
        ArgumentNullException.ThrowIfNull(key);
        ObservedKeys.Add(key);
        return _values.TryGetValue(key, out var stored) && stored is T typed ? typed : fallback;
    }

    public Task SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ObservedKeys.Add(key);
        _values[key] = value;
        return Task.CompletedTask;
    }

    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ObservedKeys.Add(key);
        _values.Remove(key);
        return Task.CompletedTask;
    }
}
