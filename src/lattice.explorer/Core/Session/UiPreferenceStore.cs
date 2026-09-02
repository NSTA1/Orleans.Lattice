using System.Text.Json;

namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Default <see cref="IUiPreferenceStore"/>: an in-memory mirror of JSON-encoded
/// values persisted as a single document through an
/// <see cref="IUiPreferenceBackingStore"/>. Registered with a scoped lifetime so
/// each session hydrates its own mirror once, then serves reads synchronously.
/// </summary>
public sealed class UiPreferenceStore : IUiPreferenceStore, IDisposable
{
    /// <summary>The backing-store key under which the whole preference document is persisted.</summary>
    public const string BackingKey = "orleans.lattice.explorer.preferences.v1";

    /// <summary>The default retention window after which an untouched entry is swept.</summary>
    public static readonly TimeSpan DefaultRetention = TimeSpan.FromDays(90);

    private readonly IUiPreferenceBackingStore _backing;
    private readonly TimeProvider _clock;
    private readonly TimeSpan _retention;
    private readonly Dictionary<string, PreferenceEntry> _entries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, object?> _deserialized = new(StringComparer.Ordinal);

    // Serialises hydration and every mutation so the in-memory dictionaries are
    // never written from two continuations at once. The store is a scoped service
    // shared by sibling components (the navigation and detail panels), each of
    // which hydrates on startup; without this gate their concurrent writes would
    // corrupt the dictionaries.
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _loaded;

    // Set by Dispose so work that arrives after the scope has ended stops instead of
    // hydrating or persisting into a store nobody will read again. Volatile because it
    // is written on the disposing thread and read on whichever thread a pending
    // continuation resumes on.
    private volatile bool _disposed;

    /// <summary>Initialises the store over <paramref name="backing"/>.</summary>
    public UiPreferenceStore(IUiPreferenceBackingStore backing)
        : this(backing, TimeProvider.System, DefaultRetention)
    {
    }

    /// <summary>Initialises the store with an explicit clock and retention window (for testing).</summary>
    public UiPreferenceStore(IUiPreferenceBackingStore backing, TimeProvider clock, TimeSpan retention)
    {
        _backing = backing ?? throw new ArgumentNullException(nameof(backing));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _retention = retention;
    }

    /// <inheritdoc />
    public bool IsLoaded => _loaded;

    /// <inheritdoc />
    public async Task EnsureLoadedAsync(CancellationToken cancellationToken = default)
    {
        if (_loaded || _disposed)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            // Re-check under the gate: a sibling component may have hydrated while
            // this caller was waiting, so hydration runs exactly once. The disposal
            // check is here for the same reason - the scope can end while this
            // caller was queued behind another component's hydration.
            if (_loaded || _disposed)
            {
                return;
            }

            string? blob;
            try
            {
                blob = await _backing.GetAsync(BackingKey, cancellationToken);
            }
            catch
            {
                // The backing store is unreachable (e.g. browser storage during
                // server prerender). Stay unloaded so a later call retries; reads
                // fall back to their defaults in the meantime.
                return;
            }

            if (blob is not null)
            {
                try
                {
                    var map = JsonSerializer.Deserialize<Dictionary<string, PreferenceEntry>>(blob);
                    if (map is not null)
                    {
                        _entries.Clear();
                        foreach (var (key, entry) in map)
                        {
                            if (key is not null && entry is not null)
                            {
                                _entries[key] = entry;
                            }
                        }
                    }
                }
                catch (JsonException)
                {
                    // A corrupt document is discarded rather than wedging the session.
                    _entries.Clear();
                }
            }

            _loaded = true;

            if (PruneExpired())
            {
                await PersistAsync(cancellationToken);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public bool TryGet<T>(string key, out T value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (_entries.TryGetValue(key, out var entry))
        {
            if (_deserialized.TryGetValue(key, out var cached) && cached is T typedCache)
            {
                value = typedCache;
                return true;
            }

            try
            {
                var deserialized = JsonSerializer.Deserialize<T>(entry.Json);
                if (deserialized is not null)
                {
                    _deserialized[key] = deserialized;
                    value = deserialized;
                    return true;
                }
            }
            catch (JsonException)
            {
                // Fall through to the not-found result on a type/shape mismatch.
            }
        }

        value = default!;
        return false;
    }

    /// <inheritdoc />
    public T GetOrDefault<T>(string key, T fallback = default!)
        => TryGet<T>(key, out var value) ? value : fallback;

    /// <inheritdoc />
    public async Task SetAsync<T>(string key, T value, string? owner = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        await EnsureLoadedAsync(cancellationToken);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            _entries[key] = new PreferenceEntry
            {
                Json = JsonSerializer.Serialize(value),
                Owner = owner,
                TouchedUnixMs = _clock.GetUtcNow().ToUnixTimeMilliseconds(),
            };
            _deserialized[key] = value;

            await PersistAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        await EnsureLoadedAsync(cancellationToken);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_entries.Remove(key))
            {
                _deserialized.Remove(key);
                await PersistAsync(cancellationToken);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public async Task GarbageCollectAsync(IReadOnlyCollection<string> liveOwners, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(liveOwners);
        await EnsureLoadedAsync(cancellationToken);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            var live = new HashSet<string>(liveOwners, StringComparer.Ordinal);
            var changed = false;

            foreach (var key in _entries.Keys.ToArray())
            {
                var owner = _entries[key].Owner;
                if (owner is not null && !live.Contains(owner))
                {
                    _entries.Remove(key);
                    _deserialized.Remove(key);
                    changed = true;
                }
            }

            changed |= PruneExpired();

            if (changed)
            {
                await PersistAsync(cancellationToken);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    // Drops entries whose last-touch is older than the retention window. Returns
    // whether anything was removed.
    private bool PruneExpired()
    {
        if (_retention <= TimeSpan.Zero)
        {
            return false;
        }

        var cutoff = _clock.GetUtcNow().Subtract(_retention).ToUnixTimeMilliseconds();
        var removed = false;

        foreach (var key in _entries.Keys.ToArray())
        {
            if (_entries[key].TouchedUnixMs < cutoff)
            {
                _entries.Remove(key);
                _deserialized.Remove(key);
                removed = true;
            }
        }

        return removed;
    }

    private async Task PersistAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_entries.Count == 0)
            {
                await _backing.RemoveAsync(BackingKey, cancellationToken);
            }
            else
            {
                await _backing.SetAsync(BackingKey, JsonSerializer.Serialize(_entries), cancellationToken);
            }
        }
        catch
        {
            // A failed persist (unreachable backing store) leaves the in-memory
            // mirror authoritative; the next successful write reconciles it.
        }
    }

    /// <inheritdoc />
    /// <summary>
    /// Detaches the store. The gate is deliberately left to the garbage collector.
    /// </summary>
    /// <remarks>
    /// This used to call <c>_gate.Dispose()</c>, which was an active defect rather than
    /// tidiness. The store is registered <b>scoped</b>, so it is disposed when the
    /// circuit's DI scope ends - and that routinely happens while
    /// <see cref="EnsureLoadedAsync"/> is still awaiting the backing store, whose read is
    /// a JS interop call that never completes once the circuit is going away. The
    /// abandoned continuation then reached its <c>finally { _gate.Release(); }</c> against
    /// a disposed semaphore and threw <see cref="ObjectDisposedException"/>. Blazor
    /// reports that as an unhandled exception on the circuit and tears the circuit down,
    /// leaving the page rendered but inert - so the visible symptom was an unrelated
    /// later interaction doing nothing at all.
    /// <para>
    /// A <see cref="SemaphoreSlim"/> only needs disposal when its
    /// <see cref="SemaphoreSlim.AvailableWaitHandle"/> has been allocated, and this type
    /// never touches it, so letting the GC reclaim it leaks nothing and removes the
    /// fault outright.
    /// </para>
    /// </remarks>
    public void Dispose() => _disposed = true;

    /// <summary>One persisted preference: its JSON-encoded value plus GC metadata.</summary>
    internal sealed class PreferenceEntry
    {
        /// <summary>The JSON-serialized value.</summary>
        public string Json { get; set; } = "null";

        /// <summary>The optional owner discriminator used by liveness GC.</summary>
        public string? Owner { get; set; }

        /// <summary>The Unix-millisecond timestamp of the last write, used by retention GC.</summary>
        public long TouchedUnixMs { get; set; }
    }
}
