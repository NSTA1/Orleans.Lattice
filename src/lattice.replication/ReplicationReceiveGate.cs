using System.Collections.Concurrent;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationReceiveGate"/>. Consults
/// <see cref="ITreeReceiveFenceGrain"/> for a tree's paused state but caches the
/// answer for a short bounded window so the inbound apply path pays at most one
/// grain call per tree per window instead of one per applied entry.
/// <para>
/// The cache window is deliberately short (sub-second) so a resume is observed
/// promptly. Erring stale in the paused direction is safe (entries are deferred,
/// not dropped, and retried). Erring stale in the unpaused direction is NOT made
/// safe by the window: an entry can pass a stale answer just before a coordinated
/// restore pauses the tree and reach the tree after the restore's alias swap, or
/// after its lift (issue #4593). That is closed at the tree's apply seam instead,
/// structurally: each answer carries the fence's epoch, the applier stamps the
/// admitted entry with it, and a restored copy refuses an entry stamped below the
/// epoch of its restore's pause, and every entry while it is still closed.
/// </para>
/// </summary>
internal sealed class ReplicationReceiveGate(IGrainFactory grainFactory) : IReplicationReceiveGate
{
    private static readonly TimeSpan CacheWindow = TimeSpan.FromMilliseconds(250);

    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly ConcurrentDictionary<string, CacheEntry> _cache = new(StringComparer.Ordinal);

    /// <summary>
    /// The maximum number of per-tree entries retained. Entries carry an expiry but
    /// were never removed, so the map grew by one permanent entry per distinct tree
    /// id ever applied - including trees since deleted - and a peer shipping an
    /// unlimited stream of distinct tree ids could grow it without bound (CWE-770).
    /// At the cap the expired entries are swept first; a still-full map skips the
    /// insert, which only costs an extra grain call because a miss re-reads the
    /// authoritative fence.
    /// </summary>
    internal const int MaxCachedTrees = 4096;

    /// <summary>
    /// The number of per-tree entries currently cached. Exposed for unit testing
    /// that the cache honours <see cref="MaxCachedTrees"/>.
    /// </summary>
    internal int CachedTreeCount => _cache.Count;

    /// <inheritdoc />
    public async ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default)
        => (await ObserveAsync(treeId, cancellationToken).ConfigureAwait(false)).Paused;

    /// <inheritdoc />
    public async ValueTask<ReceiveFenceObservation> ObserveAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var now = DateTime.UtcNow;
        if (_cache.TryGetValue(treeId, out var cached) && now < cached.ExpiresAtUtc)
        {
            return cached.Observation;
        }

        var observation = await _grainFactory.GetGrain<ITreeReceiveFenceGrain>(treeId)
            .ObserveAsync().ConfigureAwait(false);

        StoreBounded(treeId, new CacheEntry(observation, now.Add(CacheWindow)), now);
        return observation;
    }

    /// <summary>
    /// Caches <paramref name="entry"/> without letting the map grow without bound.
    /// Refreshing a key already present never grows it, so that always proceeds; a
    /// new key at the cap first sweeps entries whose window has closed, and is
    /// dropped only when the map is still full of live entries.
    /// </summary>
    private void StoreBounded(string treeId, CacheEntry entry, DateTime now)
    {
        if (_cache.ContainsKey(treeId))
        {
            _cache[treeId] = entry;
            return;
        }

        if (_cache.Count >= MaxCachedTrees)
        {
            foreach (var pair in _cache)
            {
                if (now >= pair.Value.ExpiresAtUtc)
                {
                    _cache.TryRemove(pair.Key, out _);
                }
            }

            if (_cache.Count >= MaxCachedTrees)
            {
                return;
            }
        }

        _cache[treeId] = entry;
    }

    private readonly record struct CacheEntry(ReceiveFenceObservation Observation, DateTime ExpiresAtUtc);
}
