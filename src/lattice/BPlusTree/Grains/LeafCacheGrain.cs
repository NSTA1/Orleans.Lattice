using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A <see cref="StatelessWorkerAttribute"/>-based read-through cache that sits
/// in front of a <see cref="BPlusLeafGrain"/>. Each silo may have its own
/// activation, serving reads from a local LWW-map cache.
///
/// On a cache miss or when the cache is stale, the grain fetches a
/// <see cref="StateDelta"/> from the primary leaf and merges it into the
/// local cache using <see cref="LwwValue{T}.Merge"/>. Because the merge is
/// commutative and idempotent, stale entries are harmlessly overwritten
/// without an explicit invalidation protocol.
///
/// When <see cref="LatticeOptions.CacheTtl"/> is non-zero, the cache skips
/// the delta refresh if less than the configured duration has elapsed since
/// the last successful refresh, reducing RPC overhead at the cost of
/// potentially serving slightly stale data.
/// </summary>
[StatelessWorker]
internal sealed class LeafCacheGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor) : ILeafCacheGrain
{
    private readonly Dictionary<string, LwwValue<byte[]>> _cache = new(StringComparer.Ordinal);
    private VersionVector _version = new();
    private long _lastRefreshTicks;
    private string? _treeId;

    /// <summary>
    /// Keys this cache currently knows are covered by a pending-tx
    /// prepare on the primary leaf. Refreshed whenever we take the
    /// cross-grain refresh path in <see cref="RefreshAsync"/> by
    /// calling <see cref="IBPlusLeafGrain.GetPendingKeysAsync"/>.
    /// Reads that hit a key in this set are delegated to the primary
    /// leaf so the per-tree <see cref="ITxRegistryGrain"/> can apply
    /// the strict atomic-visibility verdict; the cache cannot make
    /// that decision itself because <see cref="_cache"/> only holds
    /// committed (post-merge) state. Empty in steady state — the vast
    /// majority of keys are never covered by an in-flight saga, so the
    /// per-read <see cref="HashSet{T}.Contains"/> probe is O(1) and
    /// allocation-free.
    /// </summary>
    private readonly HashSet<string> _pendingKeys = new(StringComparer.Ordinal);

    /// <summary>
    /// Cached resolved <see cref="GrainId"/> of the primary leaf. The
    /// activation key is immutable for the activation's lifetime, so the
    /// parsed value is invariant — caching it avoids re-running
    /// <c>GrainId.Parse(context.GrainId.Key.ToString())</c> on every
    /// <see cref="RefreshAsync"/>, which allocated a string + a re-parsed
    /// GrainId per call. Lazily initialised on first use because the
    /// grain context is set up before the constructor returns but reading
    /// it eagerly here would order-couple the field initialiser to the
    /// primary constructor.
    /// </summary>
    private GrainId? _cachedPrimaryLeafId;

    /// <summary>
    /// The most recent same-silo revision cookie this cache successfully
    /// observed and refreshed against. Used by <see cref="RefreshAsync"/>
    /// to skip the cross-grain <see cref="IBPlusLeafGrain.GetDeltaSinceAsync"/>
    /// call when the primary leaf is on the same silo and has not
    /// advanced since this cache last refreshed. <c>0</c> means "never
    /// successfully refreshed" — must take the cross-grain refresh path.
    /// </summary>
    private long _lastSeenPrimaryRevision;

    /// <summary>
    /// The <see cref="GrainId"/> string of the primary leaf grain this cache
    /// is associated with. Parsed from the grain's own string key.
    /// </summary>
    private GrainId PrimaryLeafId => _cachedPrimaryLeafId ??= GrainId.Parse(context.GrainId.Key.ToString()!);

    public async Task<byte[]?> GetAsync(string key)
    {
        // Always pull a delta from the primary. The VersionVector comparison
        // makes this cheap — if nothing changed, the primary returns an empty
        // delta without scanning entries.
        await RefreshAsync();

        // Strict atomic-visibility delegation: if this key is covered by
        // a pending-tx prepare on the primary, the cache has no way to
        // decide whether to surface the prepared value, hide the key,
        // or fall through to the pre-saga value — only the per-tree
        // TxRegistry holds the recorded outcome, and only the primary
        // leaf's read path consults it. Delegate so the saga's
        // linearization point applies uniformly across cache and leaf.
        if (_pendingKeys.Contains(key))
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);
            return await leaf.GetAsync(key);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        if (_cache.TryGetValue(key, out var cached) && !cached.IsTombstone
            && !cached.IsExpired(nowTicks))
        {
            LatticeMetrics.CacheHits.Add(1, CacheTreeTag());
            return cached.Value;
        }

        LatticeMetrics.CacheMisses.Add(1, CacheTreeTag());
        return null;
    }

    public async Task<bool> ExistsAsync(string key)
    {
        await RefreshAsync();

        // See GetAsync for the delegation rationale.
        if (_pendingKeys.Contains(key))
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);
            return await leaf.ExistsAsync(key);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var hit = _cache.TryGetValue(key, out var cached) && !cached.IsTombstone
            && !cached.IsExpired(nowTicks);
        (hit ? LatticeMetrics.CacheHits : LatticeMetrics.CacheMisses).Add(1, CacheTreeTag());
        return hit;
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {
        await RefreshAsync();

        // Partition the request into pending and non-pending keys. The
        // pending subset rounds-trips to the primary leaf so the
        // TxRegistry-driven verdict applies; the remainder is served
        // from _cache as before. In the steady state (_pendingKeys
        // empty) the partition collapses to a no-op and the legacy
        // cache path runs unchanged.
        List<string>? delegated = null;
        if (_pendingKeys.Count > 0)
        {
            foreach (var key in keys)
            {
                if (_pendingKeys.Contains(key))
                {
                    delegated ??= new List<string>();
                    delegated.Add(key);
                }
            }
        }

        Dictionary<string, byte[]>? delegatedResult = null;
        if (delegated is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);
            delegatedResult = await leaf.GetManyAsync(delegated);
        }

        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var result = new Dictionary<string, byte[]>(keys.Count);
        var hits = 0;
        var cacheLookups = 0;
        foreach (var key in keys)
        {
            if (delegatedResult is not null && delegatedResult.TryGetValue(key, out var delegatedValue))
            {
                result[key] = delegatedValue;
                continue;
            }
            // Skip cache scoring for delegated keys — they were not
            // served from this cache, so they should not count as a
            // hit or a miss against this cache's metrics. Use the
            // O(1) HashSet membership check on _pendingKeys rather
            // than O(n) List.Contains over `delegated` — `delegated`
            // is by construction a subset of `_pendingKeys`, so the
            // two predicates are equivalent for this branch.
            if (_pendingKeys.Contains(key))
                continue;

            cacheLookups++;
            if (_cache.TryGetValue(key, out var cached) && !cached.IsTombstone
                && !cached.IsExpired(nowTicks))
            {
                result[key] = cached.Value!;
                hits++;
            }
        }
        var tag = CacheTreeTag();
        if (hits > 0) LatticeMetrics.CacheHits.Add(hits, tag);
        var misses = cacheLookups - hits;
        if (misses > 0) LatticeMetrics.CacheMisses.Add(misses, tag);
        return result;
    }

    private KeyValuePair<string, object?> CacheTreeTag() =>
        new(LatticeMetrics.TagTree, _treeId ?? string.Empty);

    private async Task RefreshAsync()
    {
        var primaryId = PrimaryLeafId;

        // Same-silo revision-cookie short-circuit. When the primary
        // leaf is activated on this silo, every state-advancing
        // operation on it (Set / Delete / saga prepare / saga
        // terminal) bumps a process-wide cookie published via
        // BPlusLeafGrain.LeafRevisionRegistry. We check the cookie
        // BEFORE the TTL gate because saga-pending visibility relies
        // on every cache observing every revision change promptly:
        // if a saga's terminal mark drains the leaf's _pendingTx and
        // bumps the revision, the cache MUST refresh on the next read
        // even if its TTL has not yet elapsed — otherwise the cache
        // continues serving the pre-saga value from _cache while a
        // sibling cache (whose own leaf drained earlier in the same
        // window) has already merged the post-saga value, producing
        // a split observation across leaves that the lattice-level
        // double-checked TxRegistry snapshot retry cannot detect
        // (because the cache short-circuits the leaf RPC entirely).
        if (_lastSeenPrimaryRevision > 0
            && BPlusLeafGrain.TryGetLeafRevision(primaryId, out var sameSiloRev))
        {
            if (sameSiloRev == _lastSeenPrimaryRevision)
                return; // provably fresh; nothing has changed on the primary.
            // Revision changed: skip the TTL gate. The revision is the
            // source of truth on same-silo; the TTL exists only as a
            // bandwidth bound for cross-silo where we cannot make a
            // direct equality check.
        }
        else
        {
            // First refresh on this cache OR the primary leaf is on
            // another silo (no revision cookie published locally).
            // Fall back to the TTL gate to cap cross-silo RPC traffic
            // at the cost of bounded staleness — the cross-silo case
            // already has a separate non-linearizable-scan window
            // bounded by network round-trip time.
            var ttl = await GetCacheTtlAsync();
            if (ttl > TimeSpan.Zero && _lastRefreshTicks > 0)
            {
                var elapsed = Environment.TickCount64 - _lastRefreshTicks;
                if (elapsed < (long)ttl.TotalMilliseconds)
                    return;
            }
        }

        // Capture the revision cookie BEFORE issuing GetDeltaSinceAsync.
        // The cookie is monotonically increasing on every leaf state
        // change; recording it BEFORE the cross-grain RPC means
        // _lastSeenPrimaryRevision is a sound lower bound on the state
        // our forthcoming refresh will reflect. Capturing AFTER the
        // RPC returns leaves a window where the leaf can advance
        // between the delta's wall-clock observation moment and the
        // cookie read — the cache would then record a cookie that
        // matches the leaf's NEW state while _cache and _pendingKeys
        // only reflect the older state, and the same-silo fast path
        // would short-circuit indefinitely without observing the
        // missed advance. The cost of capturing early is at worst one
        // extra refresh per phantom advance, which is bounded by leaf
        // state-bumping cadence.
        var preFetchRevision = BPlusLeafGrain.TryGetLeafRevision(primaryId, out var preRev)
            ? preRev
            : 0L;

        var primaryLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(primaryId);

        // Refresh the pending-key set BEFORE fetching the delta. The
        // ordering matters: a saga drain (TxCommit/TxAbort) between
        // these two RPCs flips a leaf from "_pendingTx[txX]={K},
        // Entries[K]=pre" to "_pendingTx empty, Entries[K]=post". If
        // we fetched delta first, we could observe (pendingKeys=
        // empty, delta=pre) — the cache would then serve _cache[K]=
        // pre directly without delegating to the leaf, violating
        // strict per-tree atomic visibility against sibling caches
        // whose leaf drained in a different fan-out window. Fetching
        // pending first means the worst-case observation is
        // (pendingKeys=[K], delta=post): the cache delegates K to the
        // leaf (which is post-drain, returns Entries=post) and the
        // sibling caches' post values are consistent. Under-claiming
        // a key as pending is a correctness-preserving over-
        // approximation; over-claiming a key as fully cached is the
        // bug we are avoiding.
        var pendingKeys = await primaryLeaf.GetPendingKeysAsync();

        var delta = await primaryLeaf.GetDeltaSinceAsync(_version);

        // If the primary leaf has been split, prune any cached entries that
        // now belong to the new sibling (keys >= SplitKey). This is idempotent
        // and safe to apply on every refresh — pruning a key that doesn't
        // exist is a no-operation.
        if (delta.SplitKey is not null)
        {
            var splitKey = delta.SplitKey;
            var keysToRemove = new List<string>();
            foreach (var key in _cache.Keys)
            {
                if (string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                    keysToRemove.Add(key);
            }
            foreach (var key in keysToRemove)
            {
                _cache.Remove(key);
            }
        }

        // Stamp the pre-fetch cookie so subsequent same-silo reads can
        // short-circuit when no further state has accumulated. If the
        // cookie was absent (cross-silo primary), preFetchRevision is
        // 0 and the fast-path guard `_lastSeenPrimaryRevision > 0`
        // keeps us on the cross-grain refresh path — same behaviour as
        // before this fix.
        _lastSeenPrimaryRevision = preFetchRevision;

        _pendingKeys.Clear();
        if (pendingKeys is { Count: > 0 })
        {
            foreach (var k in pendingKeys)
                _pendingKeys.Add(k);
        }

        if (delta.IsEmpty)
            return;

        // Merge each entry using LWW semantics.
        foreach (var (key, lww) in delta.Entries)
        {
            if (_cache.TryGetValue(key, out var existing))
            {
                _cache[key] = LwwValue<byte[]>.Merge(existing, lww);
            }
            else
            {
                _cache[key] = lww;
            }
        }

        // Advance our version vector to reflect what we've received.
        _version = VersionVector.Merge(_version, delta.Version);
        _lastRefreshTicks = Environment.TickCount64;
    }

    private async Task<TimeSpan> GetCacheTtlAsync()
    {
        if (_treeId is null)
        {
            var primaryLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);
            _treeId = await primaryLeaf.GetTreeIdAsync() ?? string.Empty;
        }
        return optionsMonitor.Get(_treeId).CacheTtl;
    }
}
