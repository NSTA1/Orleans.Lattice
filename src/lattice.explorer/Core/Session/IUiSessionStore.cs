namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// A general, session-scoped key/value store for transient UI state - for
/// example a per-selection search prefix, an expanded/collapsed flag, or a last
/// chosen option. State is held in memory for the lifetime of the user session
/// only: it is never persisted and is discarded when the session ends. Keys are
/// composed by callers (typically a feature prefix plus a discriminator such as
/// a catalog selection id) so unrelated UI elements never collide.
/// </summary>
public interface IUiSessionStore
{
    /// <summary>
    /// Reads the value stored under <paramref name="key"/> when present and of
    /// type <typeparamref name="T"/>. Returns <see langword="false"/> (with
    /// <paramref name="value"/> set to its default) when nothing is stored or the
    /// stored value is of a different type.
    /// </summary>
    bool TryGet<T>(string key, out T value);

    /// <summary>
    /// Reads the value stored under <paramref name="key"/>, or
    /// <paramref name="fallback"/> when nothing of type <typeparamref name="T"/>
    /// is stored.
    /// </summary>
    T GetOrDefault<T>(string key, T fallback = default!);

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/>, replacing any
    /// existing value.
    /// </summary>
    void Set<T>(string key, T value);

    /// <summary>
    /// Removes any value stored under <paramref name="key"/>. A no-op when the key
    /// is absent.
    /// </summary>
    void Remove(string key);
}
