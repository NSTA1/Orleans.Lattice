namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// A durable, low-stakes key/value store for one plugin's UI preferences - a
/// chosen page size, a last search prefix, a selected tag. Values survive a
/// reload because the host persists them.
/// <para>
/// The instance a plugin receives from
/// <see cref="IExplorerPluginHostContext.Preferences"/> is scoped to that
/// plugin, so two plugins may use the same key without colliding and neither
/// can read the other's. Keys are opaque to the host and compared ordinally.
/// </para>
/// <para>
/// Reads are synchronous because the host mirrors the persisted values in
/// memory; the mirror is hydrated once by
/// <see cref="EnsureLoadedAsync"/>, which a plugin should await before its
/// first read (browser storage is unreachable during prerender, so hydration
/// can legitimately be deferred).
/// </para>
/// </summary>
public interface IExplorerPluginPreferences
{
    /// <summary>
    /// Whether the in-memory mirror has been hydrated. <see langword="false"/>
    /// until <see cref="EnsureLoadedAsync"/> first completes against a
    /// reachable backing store, so a read before then yields defaults.
    /// </summary>
    bool IsLoaded { get; }

    /// <summary>
    /// Hydrates the in-memory mirror the first time it is called; a no-op once
    /// loaded. Tolerant of an unreachable backing store, so it is safe to await
    /// during component initialization.
    /// </summary>
    /// <param name="cancellationToken">Cancels the hydration.</param>
    Task EnsureLoadedAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the value stored under <paramref name="key"/> when present and
    /// readable as <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The stored value's type.</typeparam>
    /// <param name="key">The preference key. Must not be <see langword="null"/>.</param>
    /// <param name="value">The stored value, or <c>default</c> when absent.</param>
    /// <returns><see langword="true"/> when a usable value was stored.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    bool TryGet<T>(string key, out T value);

    /// <summary>
    /// Reads the value stored under <paramref name="key"/>, or
    /// <paramref name="fallback"/> when nothing usable is stored.
    /// </summary>
    /// <typeparam name="T">The stored value's type.</typeparam>
    /// <param name="key">The preference key. Must not be <see langword="null"/>.</param>
    /// <param name="fallback">The value to return when nothing is stored.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    T GetOrDefault<T>(string key, T fallback = default!);

    /// <summary>Stores <paramref name="value"/> under <paramref name="key"/> and persists it.</summary>
    /// <typeparam name="T">The value's type.</typeparam>
    /// <param name="key">The preference key. Must not be <see langword="null"/>.</param>
    /// <param name="value">The value to store.</param>
    /// <param name="cancellationToken">Cancels the persist.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    Task SetAsync<T>(string key, T value, CancellationToken cancellationToken = default);

    /// <summary>Removes any value stored under <paramref name="key"/> and persists the change.</summary>
    /// <param name="key">The preference key. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the persist.</param>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <see langword="null"/>.</exception>
    Task RemoveAsync(string key, CancellationToken cancellationToken = default);
}
