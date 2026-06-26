namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// A durable, browser-backed key/value store for low-stakes UI <em>preferences</em>
/// - a chosen page size, a last search prefix, a selected tag value, and the
/// like. Unlike <see cref="IUiSessionStore"/> (which is in-memory and lost when
/// the session ends), preferences survive a page reload and a browser restart
/// because they are persisted to per-origin browser storage (<c>localStorage</c>
/// on the web head, the platform preference store on the desktop head).
/// <para>
/// Values are mirrored in memory so reads are synchronous; the mirror is
/// hydrated from the backing store once per session by
/// <see cref="EnsureLoadedAsync"/>, which callers must await before their first
/// read. Each entry may carry an <c>owner</c> discriminator (typically a catalog
/// selection id) so <see cref="GarbageCollectAsync"/> can drop preferences whose
/// owner no longer exists. Stored entries also expire after a retention window
/// so the backing store never grows without bound.
/// </para>
/// </summary>
public interface IUiPreferenceStore
{
    /// <summary>
    /// Whether the in-memory mirror has been hydrated from the backing store. It
    /// is <see langword="false"/> until <see cref="EnsureLoadedAsync"/> first
    /// completes against a reachable backing store (for example, browser storage
    /// is unreachable during server prerender, so hydration is deferred).
    /// </summary>
    bool IsLoaded { get; }

    /// <summary>
    /// Hydrates the in-memory mirror from the backing store the first time it is
    /// called, and runs a retention sweep. A no-op once loaded. Tolerant of an
    /// unreachable backing store (it simply stays unloaded and retries on the
    /// next call), so it is safe to await during component initialization.
    /// </summary>
    Task EnsureLoadedAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the value stored under <paramref name="key"/> when present and
    /// deserializable as <typeparamref name="T"/>. Returns <see langword="false"/>
    /// (with <paramref name="value"/> set to its default) otherwise. Serves from
    /// the in-memory mirror, so it is only meaningful after
    /// <see cref="EnsureLoadedAsync"/> has completed.
    /// </summary>
    bool TryGet<T>(string key, out T value);

    /// <summary>
    /// Reads the value stored under <paramref name="key"/>, or
    /// <paramref name="fallback"/> when nothing usable is stored.
    /// </summary>
    T GetOrDefault<T>(string key, T fallback = default!);

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/> and persists
    /// it, tagging the entry with the optional <paramref name="owner"/>
    /// discriminator used by <see cref="GarbageCollectAsync"/>.
    /// </summary>
    Task SetAsync<T>(string key, T value, string? owner = null, CancellationToken cancellationToken = default);

    /// <summary>Removes any value stored under <paramref name="key"/> and persists the change.</summary>
    Task RemoveAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops every stored entry whose <c>owner</c> is set and absent from
    /// <paramref name="liveOwners"/>, plus any entry past its retention window,
    /// then persists if anything changed. Pass the complete set of currently
    /// valid owners (for example, all catalog selection ids); entries with no
    /// owner are retained (subject only to retention expiry).
    /// </summary>
    Task GarbageCollectAsync(IReadOnlyCollection<string> liveOwners, CancellationToken cancellationToken = default);
}
