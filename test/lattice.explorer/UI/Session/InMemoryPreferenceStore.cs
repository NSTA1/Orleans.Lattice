using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>An in-memory <see cref="IUiPreferenceStore"/> that is loaded from the start.</summary>
internal sealed class InMemoryPreferenceStore : IUiPreferenceStore
{
    private readonly Dictionary<string, object?> _values = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public bool IsLoaded => true;

    /// <inheritdoc />
    public Task EnsureLoadedAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public bool TryGet<T>(string key, out T value)
    {
        if (_values.TryGetValue(key, out var stored) && stored is T typed)
        {
            value = typed;
            return true;
        }

        value = default!;
        return false;
    }

    /// <inheritdoc />
    public T GetOrDefault<T>(string key, T fallback = default!) => TryGet<T>(key, out var value) ? value : fallback;

    /// <inheritdoc />
    public Task SetAsync<T>(string key, T value, string? owner = null, CancellationToken cancellationToken = default)
    {
        _values[key] = value;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        _values.Remove(key);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task GarbageCollectAsync(IReadOnlyCollection<string> liveOwners, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}
