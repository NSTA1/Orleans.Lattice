using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The per-silo resolution cache. Stores resolved subjects keyed by the whole
/// caller credential (see <see cref="MembershipCacheKey"/>) and serves a warm
/// entry without re-authenticating or touching the
/// directory. Only a resolved identity is cached: an anonymous verdict is never
/// stored, so an unauthenticated caller cannot populate the map, and the map is
/// capped at <see cref="MaxCachedSubjects"/> live entries so a rotating-token
/// population cannot grow it without bound. Freshness is enforced three ways:
/// every entry is bounded by the
/// minimum of the configured cache lifetime and the credential's own expiry (so
/// a subject is never served past its token's <c>exp</c>), the cache is
/// flushed whenever this silo observes a <c>sys-membership-*</c> mutation
/// (through the core <see cref="IMutationObserver"/> seam), so a membership change
/// is reflected without a process restart, and it is flushed whenever
/// <see cref="LatticeMembershipOptions"/> changes, so a subject mapped under the
/// previous group merge rules is not served after them. The observer runs on the silo that
/// commits the write, so on its own it would leave another silo's cache serving a
/// pre-change entry until that entry expired; the
/// <see cref="MembershipEpochSynchronizer"/> closes that gap by flushing every
/// other silo within about a second through a cluster-wide epoch, and once it is in
/// charge a warm entry is served only while its last poll confirmed the cache
/// current (see <see cref="RequireConfirmedCurrency"/>). Cache hits and misses are counted on the
/// membership-owned meter through <see cref="LatticeMembershipMetrics"/>.
/// </summary>
internal sealed class MembershipResolutionCache : IMutationObserver, IDisposable
{
    /// <summary>
    /// The hard ceiling on cached subjects. The cache key is derived from the
    /// caller-supplied credential, so without a ceiling a population that
    /// rotates tokens grows the map without bound: entries expire logically but
    /// nothing removes
    /// them until a membership tree happens to mutate. Refusing an insert at the
    /// ceiling is safe because a miss re-authenticates and re-resolves, so the
    /// bound costs latency and never correctness.
    /// </summary>
    internal const int MaxCachedSubjects = 4096;

    private readonly ConcurrentDictionary<MembershipCacheKey, Entry> _entries = new();
    private readonly TimeProvider _timeProvider;
    private readonly IOptionsMonitor<LatticeMembershipOptions> _options;
    private IDisposable? _optionsChange;

    // Bumped by every flush. A resolution captures it before resolving and keeps
    // its result only if no flush happened meanwhile, so a subject resolved under
    // the pre-flush rules is never cached after the flush that was meant to
    // evict it.
    private long _generation;

    // Cross-silo currency (#5518). Zero until a MembershipEpochSynchronizer takes
    // charge of this cache; from then on a warm entry is served only while
    // _currentUntilUtcTicks - set by each successful epoch poll - lies ahead, so a
    // silo that cannot learn of membership writes committed elsewhere stops
    // serving cached resolutions instead of serving pre-change groups.
    private int _confirmedCurrencyRequired;
    private long _currentUntilUtcTicks;

    /// <summary>
    /// Initializes the cache and subscribes it to option changes, each of which
    /// flushes it.
    /// </summary>
    /// <param name="timeProvider">The clock that bounds entry lifetimes.</param>
    /// <param name="options">The membership options; a change flushes the cache.</param>
    public MembershipResolutionCache(
        TimeProvider timeProvider,
        IOptionsMonitor<LatticeMembershipOptions> options)
    {
        _timeProvider = timeProvider;
        _options = options;
        _optionsChange = options.OnChange(_ => Clear());
    }

    /// <summary>The number of live cache entries. Exposed for tests.</summary>
    internal int Count => _entries.Count;

    /// <summary>
    /// Runs after a resolution has passed its flush check and before it stores
    /// its entry, so a test can flush inside that window.
    /// </summary>
    internal Action? BeforeStoreForTesting { get; set; }

    /// <summary>
    /// Attempts to serve a warm entry for <paramref name="cacheKey"/> without
    /// allocating a resolver closure. Lets the caller skip building the
    /// cache-miss delegate on the common warm-hit path.
    /// </summary>
    /// <param name="cacheKey">The key covering the whole caller credential.</param>
    /// <param name="subject">The cached subject when warm; otherwise <c>default</c>.</param>
    /// <returns><c>true</c> when a live entry was served.</returns>
    public bool TryGetCached(in MembershipCacheKey cacheKey, out LatticeSubject subject)
    {
        if (_options.CurrentValue.ResolutionCacheTtl > TimeSpan.Zero
            && _entries.TryGetValue(cacheKey, out var entry)
            && _timeProvider.GetUtcNow() is var now
            && now < entry.ExpiresAt
            && IsConfirmedCurrent(now))
        {
            subject = entry.Subject;
            LatticeMembershipMetrics.RecordResolutionCacheHit();
            return true;
        }

        // A warm-path miss is not counted here: it is always followed by a
        // ResolveAsync that records the miss when it actually resolves, so
        // counting it in both places would double-count a single lookup.
        subject = default;
        return false;
    }

    /// <summary>
    /// Returns the cached subject for <paramref name="cacheKey"/> when warm,
    /// otherwise invokes <paramref name="resolver"/> and caches the result
    /// bounded by the cache lifetime and the token's expiry.
    /// </summary>
    /// <param name="cacheKey">The key covering the whole caller credential.</param>
    /// <param name="resolver">Resolves the subject on a cache miss.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    public async ValueTask<LatticeSubject> ResolveAsync(
        MembershipCacheKey cacheKey,
        Func<CancellationToken, ValueTask<ResolvedSubject>> resolver,
        CancellationToken cancellationToken)
    {
        var ttl = _options.CurrentValue.ResolutionCacheTtl;
        var now = _timeProvider.GetUtcNow();

        if (ttl > TimeSpan.Zero && _entries.TryGetValue(cacheKey, out var entry) && now < entry.ExpiresAt && IsConfirmedCurrent(now))
        {
            LatticeMembershipMetrics.RecordResolutionCacheHit();
            return entry.Subject;
        }

        // No live entry: this is the authoritative cache miss - the resolver is
        // about to run - so it is counted here rather than in TryGetCached.
        LatticeMembershipMetrics.RecordResolutionCacheMiss();

        var generation = Interlocked.Read(ref _generation);
        var resolved = await resolver(cancellationToken).ConfigureAwait(false);

        // Only a resolved identity is cached. An unrecognised, invalid, or
        // expired credential resolves to the anonymous subject, and caching that
        // verdict would key an entry on a token no authenticator accepted -
        // letting an unauthenticated caller mint an unbounded number of distinct
        // cache keys before proving anything. A positive-only cache is bounded
        // by the real subject population, and re-resolving an anonymous verdict
        // is cheap because it never reaches the directory.
        if (ttl > TimeSpan.Zero && !resolved.Subject.IsAnonymous)
        {
            var expiresAt = now + ttl;
            if (resolved.TokenExpiry is { } tokenExpiry && tokenExpiry < expiresAt)
            {
                expiresAt = tokenExpiry;
            }

            if (expiresAt > now && Interlocked.Read(ref _generation) == generation)
            {
                var stored = new Entry(resolved.Subject, expiresAt);
                BeforeStoreForTesting?.Invoke();
                StoreBounded(cacheKey, stored, now);

                // A flush that landed between the check above and the store
                // cleared the map before this entry reached it. Re-check and
                // withdraw exactly this entry, so a concurrent resolution's
                // newer entry is never dropped.
                if (Interlocked.Read(ref _generation) != generation)
                {
                    _entries.TryRemove(new KeyValuePair<MembershipCacheKey, Entry>(cacheKey, stored));
                }
            }
        }

        return resolved.Subject;
    }

    /// <summary>
    /// Stores <paramref name="entry"/> without letting the map exceed
    /// <see cref="MaxCachedSubjects"/> live keys. Refreshing a key that is
    /// already present cannot grow the map, so it is always allowed and an
    /// in-use entry is never dropped from under the warm path. Admitting a new
    /// key at the ceiling first drops whatever has already expired, and skips
    /// the insert only when the map is genuinely full of live entries.
    /// </summary>
    private void StoreBounded(in MembershipCacheKey cacheKey, Entry entry, DateTimeOffset now)
    {
        if (_entries.ContainsKey(cacheKey))
        {
            _entries[cacheKey] = entry;
            return;
        }

        if (_entries.Count >= MaxCachedSubjects)
        {
            foreach (var pair in _entries)
            {
                if (now >= pair.Value.ExpiresAt)
                {
                    _entries.TryRemove(pair.Key, out _);
                }
            }

            if (_entries.Count >= MaxCachedSubjects)
            {
                return;
            }
        }

        _entries[cacheKey] = entry;
    }

    /// <summary>
    /// Drops every cached entry, and stops any resolution already in flight from
    /// caching its result. Used by tests, by membership mutations, by a change to
    /// <see cref="LatticeMembershipOptions"/>, and by the
    /// tenancy add-on when its delegated tenant access administration flag
    /// changes, so no subject resolved before the flip outlives it.
    /// </summary>
    internal void Clear()
    {
        Interlocked.Increment(ref _generation);
        _entries.Clear();
    }

    /// <summary>
    /// Makes the cache serve a warm entry only while a confirmation from
    /// <see cref="ConfirmCurrentUntil"/> holds. Called once by the
    /// <see cref="MembershipEpochSynchronizer"/> that takes charge of keeping this
    /// cache current with membership writes committed on other silos; a cache
    /// nobody keeps current (a host without the synchronizer) is unaffected.
    /// </summary>
    internal void RequireConfirmedCurrency() => Volatile.Write(ref _confirmedCurrencyRequired, 1);

    /// <summary>
    /// Records that a read of the cluster-wide membership epoch found nothing this
    /// cache has not already reflected, so warm entries may be served until
    /// <paramref name="until"/>.
    /// </summary>
    /// <param name="until">The instant the confirmation lapses.</param>
    internal void ConfirmCurrentUntil(DateTimeOffset until) =>
        Volatile.Write(ref _currentUntilUtcTicks, until.UtcTicks);

    /// <summary>
    /// Whether a warm entry may be served at <paramref name="now"/>: always when no
    /// synchronizer keeps the cache current, otherwise only while its last
    /// confirmation holds.
    /// </summary>
    /// <param name="now">The current instant.</param>
    /// <returns><c>true</c> when a warm entry may be served.</returns>
    internal bool IsConfirmedCurrent(DateTimeOffset now) =>
        Volatile.Read(ref _confirmedCurrencyRequired) == 0
        || now.UtcTicks < Volatile.Read(ref _currentUntilUtcTicks);

    /// <summary>Stops flushing on option changes.</summary>
    public void Dispose() => Interlocked.Exchange(ref _optionsChange, null)?.Dispose();

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (mutation.TreeId is { } treeId
            && treeId.StartsWith(MembershipConstants.TreePrefix, StringComparison.Ordinal))
        {
            Clear();
        }

        return Task.CompletedTask;
    }

    private readonly record struct Entry(LatticeSubject Subject, DateTimeOffset ExpiresAt);
}

/// <summary>
/// The outcome of an uncached resolution: the resolved subject and the optional
/// token expiry that bounds how long it may be cached.
/// </summary>
/// <param name="Subject">The resolved subject.</param>
/// <param name="TokenExpiry">The credential's expiry, or <c>null</c> when it carries none.</param>
internal readonly record struct ResolvedSubject(LatticeSubject Subject, DateTimeOffset? TokenExpiry);
