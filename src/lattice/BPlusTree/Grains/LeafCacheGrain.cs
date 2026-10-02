using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A <see cref="Orleans.Concurrency.StatelessWorkerAttribute"/>-based read-through cache that sits
/// in front of a <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Each silo may have its own
/// activation, serving reads from a local LWW-map cache.
///
/// On a cache miss or when the cache is stale, the grain fetches a
/// <see cref="StateDelta"/> from the primary leaf and merges it into the
/// local cache using <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Merge"/>. Because the merge is
/// commutative and idempotent, stale entries are harmlessly overwritten
/// without an explicit invalidation protocol.
///
/// When <see cref="LatticeOptions.CacheTtl"/> is non-zero, the cache skips
/// the delta refresh if less than the configured duration has elapsed since
/// the last successful refresh, reducing RPC overhead at the cost of
/// potentially serving slightly stale data.
/// </summary>
// CS9113: 'originClusterIdResolver' is referenced only inside #if LATTICE_DIAG
// blocks (used by DiagSiloTag to disambiguate Site A vs Site B emissions in the
// shared file-based DiagSink log). Suppressed at the parameter list because in
// non-diag builds the parameter is genuinely unread, but removing it would break
// the activation-DI signature and the diag build's site-tagging behaviour.
#pragma warning disable CS9113
[StatelessWorker]
internal sealed class LeafCacheGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILatticeOriginClusterIdResolver originClusterIdResolver) : ILeafCacheGrain, IGrainBase
#pragma warning restore CS9113
{
    IGrainContext IGrainBase.GrainContext => context;

    async Task IGrainBase.OnActivateAsync(CancellationToken cancellationToken)
    {
        _treeId = await PrimaryLeaf.GetTreeIdAsync() ?? string.Empty;
        _metricTreeId = await optionsResolver.ResolveMetricTreeIdAsync(_treeId);
    }

    /// <summary>
    /// The read-through mirror of the primary leaf's rows.
    /// <para>
    /// <b>Complete-mirror invariant (issue #2412).</b> Once
    /// <see cref="RefreshAsync"/> has returned normally, every key the primary
    /// leaf would serve a read for has a row here: resident, tombstoned, or
    /// payload-evicted (the <c>Value is null &amp;&amp; !IsTombstone</c>
    /// sentinel, which the read paths delegate to the leaf). The read paths
    /// rely on it without consulting the leaf: a key that misses this mirror is
    /// answered as absent - <see cref="GetAsync"/> returns <c>null</c>,
    /// <see cref="ExistsAsync"/> returns <c>false</c>, and
    /// <see cref="GetManyAsync"/> omits it - which the caller cannot tell from a
    /// genuinely absent key. Every path that writes or removes a row preserves
    /// it, and each is the only thing standing between it and a silent miss:
    /// </para>
    /// <list type="bullet">
    ///   <item><b>Resync</b> (first refresh, leaf reactivation, a stale
    ///   cursor): <see cref="LeafPayloadCache.Clear"/> then a full snapshot that
    ///   the leaf builds from every row it holds
    ///   (<c>BPlusLeafGrain.GetDeltaSinceCursorAsync</c>).</item>
    ///   <item><b>Incremental delta</b>: every leaf row mutation funnels through
    ///   <c>StoreEntry</c> / <c>RemoveEntry</c>, which record a fresh per-key
    ///   delivery sequence, so everything newer than the cursor ships; a
    ///   seal lift re-records the reclaimed rows (issue #3524).</item>
    ///   <item><b>Payload eviction</b>: rewrites a row's payload to <c>null</c>
    ///   and never removes the row, so an evicted key delegates rather than
    ///   misses.</item>
    ///   <item><b>Tombstones and TTL</b>: a tombstone row is retained and expiry
    ///   is judged here with the same predicate the leaf uses, so both answer
    ///   absent together.</item>
    ///   <item><b>Split and moved-away prunes</b>: remove only keys the leaf is
    ///   giving up - at or above its published split key, which the division
    ///   hands to the new sibling, or in a sealed slot, which the read paths
    ///   reject with <see cref="StaleShardRoutingException"/> before ever
    ///   reaching the miss.</item>
    ///   <item><b>Interleaving</b>: the activation is non-reentrant, so no read
    ///   observes the gap between a resync's clear and its repopulation.</item>
    ///   <item><b>Failure mid-refresh</b>: every await in
    ///   <see cref="RefreshAsync"/> precedes the first mutation, and the
    ///   freshness markers are stamped last, so a fault leaves the prior mirror
    ///   intact and unstamped and the next read retries.</item>
    /// </list>
    /// <para>
    /// A new path that writes or removes rows here must preserve the invariant,
    /// or make the read paths delegate a miss to the leaf instead.
    /// </para>
    /// </summary>
    private readonly LeafPayloadCache _cache = new();

#if LATTICE_DIAG
    /// <summary>
    /// Cached cluster id of the silo hosting this cache activation; see
    /// <see cref="BPlusLeafGrain.DiagSiloTag"/> for the rationale.
    /// </summary>
    private string? _diagSiloTag;

    private string DiagSiloTag => _diagSiloTag
        ??= (originClusterIdResolver.Resolve(_treeId ?? string.Empty) is { Length: > 0 } id ? id : "(local)");
#endif
    private VersionVector _version = new();
    private long _lastRefreshTicks;
    private string? _treeId;
    private string? _metricTreeId;

    /// <summary>
    /// Activation-scoped delivery cursor obtained from the primary
    /// leaf on every refresh. Decouples cache delivery from LWW HLC
    /// ordering: an epoch mismatch forces a full snapshot, otherwise
    /// the leaf ships only entries whose per-key sequence is strictly
    /// greater than this value's
    /// <see cref="LeafDeliveryCursor.Sequence"/>. Starts at
    /// <see cref="LeafDeliveryCursor.Empty"/> so the first refresh
    /// trips the epoch-mismatch fast path.
    /// </summary>
    private LeafDeliveryCursor _deliveryCursor = LeafDeliveryCursor.Empty;

    /// <summary>
    /// Keys this cache currently knows are covered by a pending-tx
    /// prepare on the primary leaf. Refreshed whenever we take the
    /// cross-grain refresh path in <see cref="RefreshAsync"/> by
    /// calling <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetPendingKeysAsync"/>.
    /// Reads that hit a key in this set are delegated to the primary
    /// leaf so the per-tree <see cref="ITxRegistryGrain"/> can apply
    /// the strict atomic-visibility verdict; the cache cannot make
    /// that decision itself because <see cref="_cache"/> only holds
    /// committed (post-merge) state. Empty in steady state - the vast
    /// majority of keys are never covered by an in-flight saga, so the
    /// per-read <see cref="HashSet{T}.Contains"/> probe is O(1) and
    /// allocation-free.
    /// </summary>
    private readonly HashSet<string> _pendingKeys = new(StringComparer.Ordinal);

    /// <summary>
    /// Cached resolved <see cref="GrainId"/> of the primary leaf. The
    /// activation key is immutable for the activation's lifetime, so the
    /// parsed value is invariant - caching it avoids re-running
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
    /// to skip the cross-grain cursor-delta call when the primary leaf is on the same silo and has not
    /// advanced since this cache last refreshed, independent of the ordinary cache TTL. <c>0</c> means "never
    /// successfully refreshed" - must take the cross-grain refresh path.
    /// </summary>
    private long _lastSeenPrimaryRevision;

    /// <summary>
    /// The sorted, distinct set of virtual slots the primary leaf has
    /// reported as moved away during this activation's lifetime. Slots
    /// are sticky once moved (see
    /// <c>BPlusLeafGrain.MarkSlotsMovedAwayAsync</c>), so this set only
    /// grows. A key whose virtual slot falls in here is no longer
    /// authoritatively owned by the primary leaf, so the cache must
    /// surface a <see cref="StaleShardRoutingException"/> on read
    /// instead of silently returning <c>null</c> - otherwise the caller
    /// observes a phantom-deleted key during the reshard drain window,
    /// before the shard root's read gate (which activates at
    /// <c>ShardSplitPhase.Swap</c>) catches the misroute. <c>null</c>
    /// means "no slots have been reported moved away yet" (the steady
    /// state).
    /// </summary>
    private int[]? _movedAwaySlots;

    /// <summary>
    /// The virtual-shard count associated with <see cref="_movedAwaySlots"/>.
    /// Required to hash a request key back to a virtual slot in the same
    /// space the primary leaf used when it published the moved-away set.
    /// <c>0</c> means "no moved-away set recorded yet".
    /// </summary>
    private int _movedAwayVsc;

    /// <summary>
    /// The <see cref="GrainId"/> string of the primary leaf grain this cache
    /// is associated with. Parsed from the grain's own string key.
    /// </summary>
    private GrainId PrimaryLeafId => _cachedPrimaryLeafId ??= GrainId.Parse(context.GrainId.Key.ToString()!);

    private IBPlusLeafGrain? _primaryLeaf;

    /// <summary>
    /// Reference to the primary leaf, resolved once per activation. A bare
    /// <c>GetGrain</c> per call re-resolves the interface type and rebuilds the
    /// proxy on every read that delegates to the primary.
    /// </summary>
    private IBPlusLeafGrain PrimaryLeaf => _primaryLeaf ??= grainFactory.GetGrain<IBPlusLeafGrain>(PrimaryLeafId);

    /// <summary>
    /// Throws <see cref="StaleShardRoutingException"/> if <paramref name="key"/>
    /// hashes into a virtual slot the primary leaf has reported as moved
    /// away. This is the cache-side equivalent of the moved-away read
    /// gate the shard root activates at <c>ShardSplitPhase.Swap</c>:
    /// during the drain window the shard root has not yet rejected the
    /// read, but the primary leaf has already published its moved-away
    /// set in a <see cref="StateDelta"/> consumed by
    /// <see cref="RefreshAsync"/>. Surfacing the routing exception lets
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeGrain"/>'s retry loop invalidate the shard map
    /// and re-route to the new owner rather than letting the caller
    /// observe a phantom-absent key.
    /// </summary>
    private void ThrowIfKeyMovedAway(string key)
    {
        var slots = _movedAwaySlots;
        if (slots is null || slots.Length == 0 || _movedAwayVsc <= 0) return;
        var slot = ShardMap.GetVirtualSlot(key, _movedAwayVsc);
        if (Array.BinarySearch(slots, slot) >= 0)
        {
            // SourceShardIndex / TargetShardIndex are unknown to the
            // cache (the cache is layered below shard routing), so
            // pass sentinel -1 values. LatticeGrain.GetManyAsyncCore
            // catches StaleShardRoutingException unconditionally - it
            // does not inspect the indices, only invalidates its
            // ShardMap and retries.
            throw new StaleShardRoutingException(-1, -1, slot);
        }
    }

    /// <summary>
    /// Batched variant of <see cref="ThrowIfKeyMovedAway"/>. Throws on
    /// the first key in <paramref name="keys"/> that hashes into a
    /// moved-away virtual slot.
    /// </summary>
    private void ThrowIfAnyKeyMovedAway(IEnumerable<string> keys)
    {
        var slots = _movedAwaySlots;
        if (slots is null || slots.Length == 0 || _movedAwayVsc <= 0) return;
        var vsc = _movedAwayVsc;
        foreach (var key in keys)
        {
            var slot = ShardMap.GetVirtualSlot(key, vsc);
            if (Array.BinarySearch(slots, slot) >= 0)
                throw new StaleShardRoutingException(-1, -1, slot);
        }
    }

    /// <summary>
    /// Activates this cache and populates it from the primary leaf without
    /// returning a payload. Deliberately skips the moved-away and pending-tx
    /// gates that <see cref="GetAsync"/> applies: pre-warm answers no read, so
    /// it has no result to invalidate. A stale-routing failure here is the
    /// caller's cue to skip this leaf, and the caller treats every pre-warm
    /// failure as a no-op.
    /// </summary>
    public Task PreWarmAsync() => RefreshAsync();

    public async Task<byte[]?> GetAsync(string key)
    {
        // Always pull a delta from the primary. The delivery cursor makes the
        // no-change path cheap - if this cache is already at the primary's
        // cursor, the primary returns an empty delta without scanning entries.
        await RefreshAsync();

        // Moved-away gate: a key whose virtual slot has been migrated
        // away from the primary leaf must surface as a routing
        // exception so LatticeGrain re-routes to the new owner instead
        // of seeing a silent null.
        ThrowIfKeyMovedAway(key);

        // Strict atomic-visibility delegation: if this key is covered by
        // a pending-tx prepare on the primary, OR the cached entry has
        // IsMigrated=true (the row arrived via a cross-shard migration
        // saga and the destination's shadow-marker / TxRegistry guard
        // is the only place the saga's linearization point is honored),
        // the cache has no way to decide whether to surface the value,
        // hide the key, or fall through. Delegate to the primary leaf
        // so the saga's linearization point applies uniformly across
        // cache and leaf. Without the migrated-entry branch, a chaos
        // window can observe a split snapshot: the cache serves the
        // pre-saga value from _cache while a sibling cache (whose leaf
        // already saw the saga's terminal) serves the post-saga value.
        var found = _cache.TryPeek(key, out var cached);
        var live = found
            && !cached.IsTombstone
            && !cached.IsExpired(DateTimeOffset.UtcNow.Ticks);

        // Payload-evicted sentinel: the row's metadata is retained but its
        // value payload was reclaimed by the LRU budget. Value is null while
        // IsTombstone is false - a shape LwwValue.Create can never produce -
        // so this is an unambiguous "delegate to the leaf for the authoritative
        // payload" signal, reusing the same leaf RPC path as pending / migrated
        // keys. Without this branch the read below would return the null
        // payload as a false miss.
        var payloadEvicted = live && cached.Value is null;
        if (_pendingKeys.Contains(key) || (live && cached.IsMigrated) || payloadEvicted)
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG cache-delegate-get] silo={DiagSiloTag} cache-gid={context.GrainId} primary={PrimaryLeafId} key={key} reason={(_pendingKeys.Contains(key) ? "pending" : cached.IsMigrated ? "migrated" : "evicted")}");
#endif
            // A pure payload-eviction miss (not pending, not migrated) is a
            // capacity-driven miss - record it so the eviction budget's cost
            // is visible on the existing cache-miss instrument.
            if (payloadEvicted && !_pendingKeys.Contains(key) && !cached.IsMigrated)
                LatticeMetrics.CacheMisses.Add(1, CacheTreeTag(), CacheTenantTag());
            var leaf = PrimaryLeaf;
            return await leaf.GetAsync(key);
        }

        if (live)
        {
            _cache.RecordHit(key);
            LatticeMetrics.CacheHits.Add(1, CacheTreeTag(), CacheTenantTag());
            return cached.Value;
        }

        LatticeMetrics.CacheMisses.Add(1, CacheTreeTag(), CacheTenantTag());
        return null;
    }

    public async Task<bool> ExistsAsync(string key)
    {
        await RefreshAsync();

        // See GetAsync for the moved-away gate rationale.
        ThrowIfKeyMovedAway(key);

        // See GetAsync for the delegation rationale (pending OR
        // IsMigrated=true entries must delegate so the leaf's shadow
        // guard runs against the per-tree TxRegistry).
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var hasCached = _cache.TryPeek(key, out var cached)
            && !cached.IsTombstone
            && !cached.IsExpired(nowTicks);
        if (_pendingKeys.Contains(key) || (hasCached && cached.IsMigrated))
        {
            var leaf = PrimaryLeaf;
            return await leaf.ExistsAsync(key);
        }

        // Existence is answerable from the retained metadata envelope alone, so
        // a payload-evicted entry (Value == null, not a tombstone) still counts
        // as a live hit here without a leaf RPC - only value reads pay the
        // eviction delegation cost.
        if (hasCached) _cache.RecordHit(key);
        (hasCached ? LatticeMetrics.CacheHits : LatticeMetrics.CacheMisses).Add(1, CacheTreeTag(), CacheTenantTag());
        return hasCached;
    }

    public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)
    {

        await RefreshAsync();

        // See GetAsync for the moved-away gate rationale. Surface the
        // exception BEFORE partitioning so a single moved-away key in
        // the batch invalidates the whole call's routing - the
        // LatticeGrain retry loop will then re-route the entire batch
        // against the new owner, instead of returning a partial
        // dictionary that silently omits the moved keys.
        ThrowIfAnyKeyMovedAway(keys);

        // Partition the request into delegated and non-delegated keys.
        // A key delegates to the primary leaf when EITHER:
        //   1. It is in _pendingKeys (a saga prepare covers it on the
        //      primary leaf - only the leaf can consult the per-tree
        //      TxRegistry for the outcome), OR
        //   2. The cached entry has IsMigrated=true (the row arrived
        //      via a cross-shard migration saga; the destination
        //      leaf's shadow-marker guard is the only place the saga's
        //      linearization point is honored). Without the migrated
        //      branch, a chaos window can observe a split snapshot:
        //      this cache serves the pre-saga value from _cache while
        //      a sibling cache (whose leaf already drained the saga's
        //      terminal) serves the post-saga value.
        // In the steady state (no pending keys, no migrated entries)
        // the partition collapses to a no-op and the legacy cache
        // path runs unchanged.
        var nowTicks = DateTimeOffset.UtcNow.Ticks;
        var predicate = LatticePredicateContext.Current;
        // Fast-path eligibility is a property of the predicate tree alone, so it
        // is loop-invariant across every row this call folds. Resolve it once
        // here rather than let each row re-walk the whole tree to rediscover it.
        var predicateFastPath = predicate is not null && LatticePredicateEvaluator.IsFastPathEligible(predicate.Value);
        List<string>? delegated = null;
        HashSet<string>? delegatedSet = null;

        // Steady state (no pending keys, no migrated or payload-evicted
        // entries) delegates nothing, and then the partition pass below has
        // already established every key's answer: the serve pass used to
        // re-probe _cache for the same key a second time, doubling the
        // dictionary lookups on the hottest read-through batch path. So the
        // two passes are fused - the partition pass serves each
        // non-delegating key straight from the probe it just performed - and
        // the original two-pass shape is reinstated only once a key actually
        // delegates. That fallback matters for more than tidiness: the
        // delegation path awaits a leaf RPC, and re-probing _cache after that
        // turn boundary (rather than trusting a pre-await snapshot) is what
        // keeps the served values consistent with the delegated fetch.
        var result = new Dictionary<string, byte[]>(keys.Count);
        var hits = 0;
        var cacheLookups = 0;
        foreach (var key in keys)
        {
            var pending = _pendingKeys.Contains(key);
            bool mustDelegate;
            LwwValue<byte[]> probe = default;
            var probeLive = false;
            if (pending)
            {
                mustDelegate = true;
            }
            else if (_cache.TryPeek(key, out probe)
                && !probe.IsTombstone
                && !probe.IsExpired(nowTicks))
            {
                // Delegate a live entry when it is a cross-shard migration
                // import (shadow-guard must run on the leaf) OR its payload was
                // evicted by the LRU budget (Value == null; the leaf holds the
                // authoritative payload). A value read cannot be served from the
                // retained metadata alone, so payload eviction forces the same
                // delegation as migration.
                mustDelegate = probe.IsMigrated || probe.Value is null;
                probeLive = !mustDelegate;
            }
            else
            {
                mustDelegate = false;
            }

            if (mustDelegate)
            {
                delegated ??= new List<string>();
                delegatedSet ??= new HashSet<string>();
                if (delegatedSet.Add(key))
                    delegated.Add(key);
                continue;
            }

            // Fused serve. Discarded wholesale below if any key delegates.
            // A key that is not live here is omitted, with no leaf RPC: sound
            // only because _cache is a complete mirror once RefreshAsync has
            // returned (see the invariant on _cache, issue #2412).
            cacheLookups++;
            if (probeLive)
            {
                _cache.RecordHit(key);
                if (predicate is not null && !LatticePredicateEvaluator.Matches(probe.Value, predicate.Value, predicateFastPath))
                {
                    hits++;
                    continue;
                }
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG cache-hit-many] silo={DiagSiloTag} cache-gid={context.GrainId} primary={PrimaryLeafId} key={key} valRound={DiagSink.DecodeRound(probe.Value!)} hlc={probe.Timestamp} isMig={probe.IsMigrated}");
#endif
                result[key] = probe.Value!;
                hits++;
            }
        }

        if (delegated is null)
        {
            var fusedTag = CacheTreeTag();
            var fusedTenantTag = CacheTenantTag();
            if (hits > 0) LatticeMetrics.CacheHits.Add(hits, fusedTag, fusedTenantTag);
            var fusedMisses = cacheLookups - hits;
            if (fusedMisses > 0) LatticeMetrics.CacheMisses.Add(fusedMisses, fusedTag, fusedTenantTag);
            return result;
        }

        // A key delegates: abandon the fused serve and fall back to the
        // original probe-after-await shape.
        result.Clear();
        hits = 0;
        cacheLookups = 0;

        Dictionary<string, byte[]>? delegatedResult = null;
        {
#if LATTICE_DIAG
            DiagSink.Write($"[DIAG cache-delegate-many] silo={DiagSiloTag} cache-gid={context.GrainId} primary={PrimaryLeafId} keys=[{string.Join(',', delegated)}]");
#endif
            var leaf = PrimaryLeaf;
            delegatedResult = await leaf.GetManyAsync(delegated);
        }

        foreach (var key in keys)
        {
            if (delegatedSet is not null && delegatedSet.Contains(key))
            {
                // Skip cache scoring for delegated keys - they were not
                // served from this cache. If the leaf omitted the key
                // from delegatedResult (e.g. tombstoned destination,
                // aborted saga), the key is intentionally absent from
                // result - we must NOT fall back to _cache for it,
                // because that would recreate the bypass we are
                // fixing.
                if (delegatedResult is not null && delegatedResult.TryGetValue(key, out var delegatedValue))
                    result[key] = delegatedValue;
                continue;
            }

            cacheLookups++;
            // No else: a miss is omitted without a leaf RPC, relying on the
            // complete-mirror invariant on _cache (issue #2412).
            if (_cache.TryPeek(key, out var cached) && !cached.IsTombstone
                && !cached.IsExpired(nowTicks))
            {
                // Non-delegated live entries always carry a resident payload -
                // payload-evicted keys were routed to the delegation partition
                // above - so cached.Value is non-null here.
                _cache.RecordHit(key);
                if (predicate is not null && !LatticePredicateEvaluator.Matches(cached.Value, predicate.Value, predicateFastPath))
                {
                    hits++;
                    continue;
                }
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG cache-hit-many] silo={DiagSiloTag} cache-gid={context.GrainId} primary={PrimaryLeafId} key={key} valRound={DiagSink.DecodeRound(cached.Value!)} hlc={cached.Timestamp} isMig={cached.IsMigrated}");
#endif
                result[key] = cached.Value!;
                hits++;
            }
        }
        var tag = CacheTreeTag();
        var tenantTag = CacheTenantTag();
        if (hits > 0) LatticeMetrics.CacheHits.Add(hits, tag, tenantTag);
        var misses = cacheLookups - hits;
        if (misses > 0) LatticeMetrics.CacheMisses.Add(misses, tag, tenantTag);
        return result;
    }

    private KeyValuePair<string, object?> CacheTreeTag() =>
        new(LatticeMetrics.TagTree, _metricTreeId ?? _treeId ?? string.Empty);

    /// <summary>Derives the owning-tenant tag emitted beside <see cref="CacheTreeTag"/>.</summary>
    private KeyValuePair<string, object?> CacheTenantTag() =>
        LatticeTenantLabel.ForTree(_treeId);

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
        // even if its TTL has not yet elapsed - otherwise the cache
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
            {
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG refresh-skip-revision] silo={DiagSiloTag} cache-gid={context.GrainId} primary={primaryId} rev={sameSiloRev}");
#endif
                return; // provably fresh; nothing has changed on the primary.
            }
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
            // at the cost of bounded staleness - the cross-silo case
            // already has a separate non-linearizable-scan window
            // bounded by network round-trip time.
            var ttl = await GetCacheTtlAsync();
            if (ttl > TimeSpan.Zero && _lastRefreshTicks > 0)
            {
                var elapsed = Environment.TickCount64 - _lastRefreshTicks;
                if (elapsed < (long)ttl.TotalMilliseconds)
                {
#if LATTICE_DIAG
                    DiagSink.Write($"[DIAG refresh-skip-ttl] silo={DiagSiloTag} cache-gid={context.GrainId} primary={primaryId} elapsedMs={elapsed} ttlMs={(long)ttl.TotalMilliseconds} lastSeenRev={_lastSeenPrimaryRevision}");
#endif
                    return;
                }
            }
        }

        // Capture the revision cookie BEFORE issuing GetDeltaSinceCursorAsync.
        // The cookie is monotonically increasing on every leaf state
        // change; recording it BEFORE the cross-grain RPC means
        // _lastSeenPrimaryRevision is a sound lower bound on the state
        // our forthcoming refresh will reflect. Capturing AFTER the
        // RPC returns leaves a window where the leaf can advance
        // between the delta's wall-clock observation moment and the
        // cookie read - the cache would then record a cookie that
        // matches the leaf's NEW state while _cache and _pendingKeys
        // only reflect the older state, and the same-silo fast path
        // would short-circuit indefinitely without observing the
        // missed advance. The cost of capturing early is at worst one
        // extra refresh per phantom advance, which is bounded by leaf
        // state-bumping cadence.
        var preFetchRevision = BPlusLeafGrain.TryGetLeafRevision(primaryId, out var preRev)
            ? preRev
            : 0L;

        var primaryLeaf = PrimaryLeaf;

        // Refresh the pending-key set BEFORE fetching the delta. The
        // ordering matters: a saga drain (TxCommit/TxAbort) between
        // these two RPCs flips a leaf from "_pendingTx[txX]={K},
        // Entries[K]=pre" to "_pendingTx empty, Entries[K]=post". If
        // we fetched delta first, we could observe (pendingKeys=
        // empty, delta=pre) - the cache would then serve _cache[K]=
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

        // Cursor-based delivery: decouples cache delivery from LWW
        // HLC ordering so cross-cluster applies whose source HLC is
        // below the destination leaf's published clock are still
        // delivered correctly. An epoch mismatch (fresh cache or
        // stale leaf activation) trips a full-snapshot rebuild; an
        // epoch match scans the leaf's per-key sequence map and
        // ships only entries newer than _deliveryCursor.Sequence.
        var priorCursor = _deliveryCursor;
        var delta = await primaryLeaf.GetDeltaSinceCursorAsync(priorCursor);

        // Resolve the per-activation payload budget here, BEFORE the first
        // mutation of this activation's mirror state, so this await is the LAST
        // one in the method (issue #2412). The complete-mirror invariant on
        // _cache is only as strong as this refresh is all-or-nothing, and the
        // budget read is a registry round trip that can fault. Resolved after
        // the resync Clear() and the revision-cookie stamp - where it used to
        // sit - a transient registry fault left the mirror cleared or only
        // partly advanced while the cookie already claimed "provably fresh", so
        // every later same-silo read short-circuited the refresh and
        // GetManyAsync silently dropped keys the leaf holds. Resolved here, a
        // fault propagates with the mirror untouched, the cookie unstamped, and
        // the cursor unadopted, so the next read simply retries the refresh.
        // An empty delta never applies a budget, so it still pays no registry
        // read, exactly as before.
        var budgetBytes = delta.IsEmpty ? 0L : await ResolveCacheBudgetBytesAsync();

        // A full snapshot is signalled either by the epoch changing (the
        // ordinary re-activation case) or by our sequence having been
        // ahead of the leaf's, which the leaf treats as a stale cursor and
        // answers with a snapshot. The second arm matters because the epoch
        // is compared across processes: if two activations in different
        // silos ever mint the same epoch, the flip is suppressed, and
        // without this the cache would merge the leaf's snapshot into its
        // existing contents WITHOUT the eviction below - leaving keys the
        // leaf has since deleted visible in the cache's read view. Keep
        // this condition in lockstep with the leaf-side guard in
        // BPlusLeafGrain.GetDeltaSinceCursorAsync.
        var resynced = priorCursor.Epoch != delta.DeliveryCursor.Epoch
            || priorCursor.Sequence > delta.DeliveryCursor.Sequence;

#if LATTICE_DIAG
        // Compact one-line summary of cache._version vs delta.Version: for each
        // replica id appearing in either vector, print "id:ourClock/theirClock"
        // so the trace shows whether the cache's vector already dominates the
        // leaf's vector along every axis (which is the empty-delta short-circuit
        // condition in GetDeltaSinceAsync).
        string FormatVer(VersionVector us, VersionVector them)
        {
            var ids = new HashSet<string>(us.Entries.Keys);
            foreach (var k in them.Entries.Keys) ids.Add(k);
            var parts = new List<string>(ids.Count);
            foreach (var id in ids)
            {
                var u = us.GetClock(id);
                var t = them.GetClock(id);
                var marker = u >= t ? "=" : "<";
                parts.Add($"{id[..Math.Min(8, id.Length)]}:cache={u}{marker}leaf={t}");
            }
            return string.Join(",", parts);
        }
        DiagSink.Write($"[DIAG refresh-delta] silo={DiagSiloTag} cache-gid={context.GrainId} primary={primaryId} isEmpty={delta.IsEmpty} entryCount={delta.Entries.Count} splitKey={(delta.SplitKey ?? "<null>")} movedAwaySlotsCount={(delta.MovedAwaySlots?.Length ?? 0)} pendingCount={(pendingKeys?.Count ?? 0)} versions=[{FormatVer(_version, delta.Version)}] cursor=[ours.Ep={priorCursor.Epoch}.Sq={priorCursor.Sequence}->leaf.Ep={delta.DeliveryCursor.Epoch}.Sq={delta.DeliveryCursor.Sequence} resynced={resynced}]");
#endif

        // A resync (leaf reactivation, first refresh ever, or a cursor the
        // leaf rejected as stale) means the delta now carries a full
        // snapshot of every live entry. Clear the local cache so range-
        // deleted / migrated-away keys that no longer exist on the
        // leaf are evicted; the subsequent merge loop repopulates
        // _cache from the snapshot.
        if (resynced)
        {
            _cache.Clear();
        }

        // If the primary leaf has been split, prune any cached entries that
        // now belong to the new sibling (keys >= SplitKey). This is idempotent
        // and safe to apply on every refresh - pruning a key that doesn't
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

        // Moved-away prune: when the primary leaf has had one or more
        // virtual slots migrated to a different physical shard, drop any
        // cached entries whose key now hashes into one of those slots.
        // The cache must not continue to serve the source's pre-migration
        // snapshot once the destination has taken authoritative ownership.
        if (delta.MovedAwaySlots is { Length: > 0 } movedSlots && delta.MovedAwayVsc is int movedVsc && movedVsc > 0)
        {
            // Record the moved-away set so subsequent reads can surface
            // StaleShardRoutingException for keys hashing into one of
            // these slots. The leaf publishes the full cumulative set in
            // every delta, so replacement is equivalent to monotonic
            // merge here. Guard against a Vsc change (which would
            // indicate the virtual-shard fan-out was re-configured)
            // by resetting the slot set when the new Vsc differs.
            if (_movedAwayVsc != movedVsc)
            {
                _movedAwaySlots = movedSlots;
                _movedAwayVsc = movedVsc;
            }
            else
            {
                _movedAwaySlots = movedSlots;
            }

            // Lazy-allocate: leaves with no cached keys hashing into a
            // moved slot allocate zero. On a typical refresh after a
            // split, exactly one virtual-slot stripe migrates per leaf,
            // so most refreshes hit this fast path.
            List<string>? keysToRemoveMoved = null;
            foreach (var key in _cache.Keys)
            {
                var slot = ShardMap.GetVirtualSlot(key, movedVsc);
                if (Array.BinarySearch(movedSlots, slot) >= 0)
                    (keysToRemoveMoved ??= new List<string>()).Add(key);
            }
            if (keysToRemoveMoved is { Count: > 0 })
            {
#if LATTICE_DIAG
                DiagSink.Write($"[DIAG cache-prune-moved-away] silo={DiagSiloTag} cache-gid={context.GrainId} primary={PrimaryLeafId} pruneCount={keysToRemoveMoved.Count} pruneKeys=[{string.Join(',', keysToRemoveMoved)}]");
#endif
                foreach (var key in keysToRemoveMoved)
                {
                    _cache.Remove(key);
                }
            }
        }
        else if (delta.MovedAwayVsc is int liftedVsc && liftedVsc > 0
                 && _movedAwaySlots is { Length: > 0 })
        {
            // Seal lift. The primary leaf still reports a slot-space stamp but
            // no sealed slot at all - the one shape only
            // BPlusLeafGrain.UnmarkSlotsMovedAwayAsync produces, when an online
            // consolidation folds virtual slots back onto this shard. A leaf
            // that never sealed anything carries no stamp either, so an
            // ordinary refresh can never trip this branch.
            //
            // Without it the cache would keep throwing
            // StaleShardRoutingException at keys the shard has legitimately
            // reclaimed, and every reader would ping-pong between a routing
            // map that says "this shard" and a cache that says "the retired
            // one" - the reclaimed keys would be permanently unreachable.
            _movedAwaySlots = null;
            _movedAwayVsc = 0;
        }

        _pendingKeys.Clear();
        if (pendingKeys is { Count: > 0 })
        {
            foreach (var k in pendingKeys)
                _pendingKeys.Add(k);
        }

        if (delta.IsEmpty)
        {
            // Even when no entries shipped, adopt the leaf's cursor
            // and refresh the wall-clock so subsequent refreshes
            // observe the up-to-date epoch / sequence and the TTL
            // gate has a meaningful starting point.
            _deliveryCursor = delta.DeliveryCursor;
            _lastRefreshTicks = Environment.TickCount64;
            StampRevisionCookie(preFetchRevision);
            return;
        }

        // Apply the per-activation payload budget before merging new entries.
        // Re-resolve the effective cap each refresh so a live reconfiguration -
        // whether a silo-wide IOptionsMonitor change or a per-tree registry
        // runtime override - takes effect on a warm activation; a null cap (the
        // default) leaves the mirror unbounded and the store keeps its
        // zero-overhead path. Eviction runs inside LeafPayloadCache.Set as
        // entries are merged below. The resolved cap is applied once per refresh
        // (not per cache operation): LeafPayloadCache holds the budget internally
        // so subsequent per-key Set calls perform no override lookup. It was
        // resolved above, ahead of every mutation (issue #2412).
        _cache.SetBudget(budgetBytes);

        // Merge each entry using LWW semantics.
        foreach (var (key, lww) in delta.Entries)
        {
            if (_cache.TryPeek(key, out var existing))
            {
                _cache.Set(key, LwwValue<byte[]>.Merge(existing, lww));
            }
            else
            {
                _cache.Set(key, lww);
            }
        }

        // Advance our version vector to reflect what we've received.
        _version = VersionVector.Merge(_version, delta.Version);
        // Adopt the leaf's current delivery cursor so the next
        // refresh ships only the strictly-newer per-key sequences.
        _deliveryCursor = delta.DeliveryCursor;
        _lastRefreshTicks = Environment.TickCount64;
        StampRevisionCookie(preFetchRevision);
    }

    /// <summary>
    /// Stamps the pre-fetch revision cookie so subsequent same-silo reads can
    /// short-circuit when no further state has accumulated on the primary.
    /// <para>
    /// Called only at the two points where a refresh has fully committed, and
    /// never earlier (issue #2412). The cookie is a claim that the mirror
    /// reflects the primary as of <paramref name="preFetchRevision"/>; a stamp
    /// taken before the mirror was actually advanced would let a refresh that
    /// fails part-way leave that claim standing over a cleared or half-advanced
    /// mirror, and the same-silo fast path in <see cref="RefreshAsync"/> would
    /// then never refresh again until the primary happened to write.
    /// </para>
    /// <para>
    /// <paramref name="preFetchRevision"/> is 0 when the registry held no entry
    /// for the primary at the moment it was captured. That has TWO causes, not
    /// one: the primary is activated on another silo (permanent for a
    /// cross-silo cache), or it was not activated anywhere just then (transient
    /// - the next activation publishes a cookie during OnActivateAsync).
    /// Recording 0 in either case keeps the fast-path guard
    /// <c>_lastSeenPrimaryRevision &gt; 0</c> false, so this cache stays on the
    /// TTL gate until its next refresh restamps a real cookie. That is bounded:
    /// the stamp is unconditional on every committed refresh and
    /// <c>_lastRefreshTicks</c> is restamped with it, so the cache self-heals
    /// after one TTL rather than being pinned indefinitely.
    /// </para>
    /// </summary>
    private void StampRevisionCookie(long preFetchRevision) => _lastSeenPrimaryRevision = preFetchRevision;

    private async Task<TimeSpan> GetCacheTtlAsync()
    {
        if (_treeId is null)
        {
            var primaryLeaf = PrimaryLeaf;
            _treeId = await primaryLeaf.GetTreeIdAsync() ?? string.Empty;
        }
        return optionsMonitor.Get(_treeId).CacheTtl;
    }

    /// <summary>
    /// Resolves the effective read-through-cache payload budget in bytes for
    /// this activation's tree, honouring the per-tree runtime
    /// <see cref="State.TreeRegistryEntry.MaxCacheValueBytes"/> override when one
    /// is pinned and falling back to the silo-wide static
    /// <see cref="LatticeOptions.MaxCacheValueBytes"/> otherwise. Returns
    /// <c>0</c> (unbounded) when neither is set. Called once per cache refresh
    /// (never per cache operation), so the registry read it may incur is
    /// amortised against the delta-shipping RPCs that same refresh already made;
    /// the resolved cap is then held inside <see cref="LeafPayloadCache"/> so
    /// per-key merges pay no override lookup.
    /// </summary>
    private async ValueTask<long> ResolveCacheBudgetBytesAsync()
    {
        var treeId = _treeId;
        if (string.IsNullOrEmpty(treeId))
        {
            // No tree id resolved yet (the same-silo revision fast path can
            // reach here before GetCacheTtlAsync has populated _treeId): fall
            // back to the silo-wide static option exactly as the pre-override
            // behaviour did, rather than consulting the registry with an empty
            // key.
            return optionsMonitor.Get(string.Empty).MaxCacheValueBytes ?? 0;
        }
        return (await optionsResolver.GetMaxCacheValueBytesAsync(treeId)) ?? 0;
    }

    /// <summary>
    /// Diagnostic-only footprint snapshot of the live read-through cache
    /// mirror. This is <em>not</em> part of any grain interface or wire
    /// contract - it is an internal seam consumed only by the
    /// <c>Bench.LeafCacheGrowth</c> probe (and its unit test) to measure the
    /// unbounded cache's per-activation memory cost as a function of entry
    /// count and value size, and by the future regression test that pins the
    /// per-activation budget so a refactor cannot silently re-introduce
    /// unbounded growth. Enumerates the cache once; O(n) in the live entry
    /// count. The reported <see cref="LeafCacheFootprint.ValueBytes"/> sums
    /// only the non-null <c>Value</c> payload lengths - the dominant,
    /// unbounded memory dimension the eviction investigation targets - and
    /// excludes the bounded per-row LWW-envelope metadata.
    /// </summary>
    internal LeafCacheFootprint DebugFootprint()
    {
        long valueBytes = 0;
        foreach (var lww in _cache.Values)
        {
            if (lww.Value is { } payload)
                valueBytes += payload.Length;
        }
        return new LeafCacheFootprint(_cache.Count, valueBytes);
    }
}
