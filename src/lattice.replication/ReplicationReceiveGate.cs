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
/// not dropped, and retried); erring stale in the unpaused direction is bounded
/// by the window and covered by the shipper-side pause on the peer, which stays
/// engaged until global completion.
/// </para>
/// </summary>
internal sealed class ReplicationReceiveGate(IGrainFactory grainFactory) : IReplicationReceiveGate
{
    private static readonly TimeSpan CacheWindow = TimeSpan.FromMilliseconds(250);

    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly ConcurrentDictionary<string, CacheEntry> _cache = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public async ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();

        var now = DateTime.UtcNow;
        if (_cache.TryGetValue(treeId, out var cached) && now < cached.ExpiresAtUtc)
        {
            return cached.Paused;
        }

        var paused = await _grainFactory.GetGrain<ITreeReceiveFenceGrain>(treeId)
            .IsPausedAsync().ConfigureAwait(false);

        _cache[treeId] = new CacheEntry(paused, now.Add(CacheWindow));
        return paused;
    }

    private readonly record struct CacheEntry(bool Paused, DateTime ExpiresAtUtc);
}
