using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Tests.Detail;

/// <summary>
/// A deterministic <see cref="IUiPreferenceStore"/> for the detail-panel tests:
/// an in-memory dictionary plus explicit control over <em>when</em> the mirror
/// hydrates, so the panel's restore-without-flicker sequence can be driven
/// without any timing.
/// </summary>
/// <remarks>
/// The real store hydrates from browser storage, which is unreachable during a
/// server prerender and reachable by the time the first render completes.
/// <see cref="HydrateOnCall"/> reproduces that exactly - and only that - by
/// counting <see cref="EnsureLoadedAsync"/> calls, and <see cref="Hang"/>
/// reproduces a hydration that never answers, so a test can observe the held
/// state itself rather than inferring it.
/// </remarks>
internal sealed class FakeUiPreferenceStore : IUiPreferenceStore
{
    private readonly Dictionary<string, object?> _values = new(StringComparer.Ordinal);
    private int _calls;

    /// <summary>
    /// The <see cref="EnsureLoadedAsync"/> call on which the mirror hydrates.
    /// <c>1</c> (the default) hydrates on the panel's first await, as an
    /// interactive render does; <c>2</c> defers to the after-render call, as a
    /// prerender does; a larger value never hydrates within a render pass, as an
    /// unreachable backing store does.
    /// </summary>
    public int HydrateOnCall { get; init; } = 1;

    /// <summary>
    /// When <see langword="true"/>, <see cref="EnsureLoadedAsync"/> returns a
    /// task that never completes, so the panel stays in its pre-restore hold.
    /// </summary>
    public bool Hang { get; init; }

    /// <summary>Every key written through <see cref="SetAsync{T}"/>, in write order.</summary>
    public List<string> Writes { get; } = [];

    /// <inheritdoc />
    public bool IsLoaded { get; private set; }

    /// <summary>Seeds a value as though it had been persisted by an earlier session.</summary>
    public FakeUiPreferenceStore Seed<T>(string key, T value)
    {
        _values[key] = value;
        return this;
    }

    /// <inheritdoc />
    public Task EnsureLoadedAsync(CancellationToken cancellationToken = default)
    {
        if (Hang)
        {
            return new TaskCompletionSource().Task;
        }

        _calls++;
        if (_calls >= HydrateOnCall)
        {
            IsLoaded = true;
        }

        return Task.CompletedTask;
    }

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
    public T GetOrDefault<T>(string key, T fallback = default!) =>
        TryGet<T>(key, out var value) ? value : fallback;

    /// <inheritdoc />
    public Task SetAsync<T>(
        string key,
        T value,
        string? owner = null,
        CancellationToken cancellationToken = default)
    {
        _values[key] = value;
        Writes.Add(key);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        _values.Remove(key);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task GarbageCollectAsync(
        IReadOnlyCollection<string> liveOwners,
        CancellationToken cancellationToken = default) => Task.CompletedTask;
}
