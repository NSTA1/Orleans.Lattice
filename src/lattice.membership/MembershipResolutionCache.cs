using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The per-silo resolution cache. Stores resolved subjects keyed by credential
/// token and serves a warm entry without re-authenticating or touching the
/// directory. Freshness is enforced two ways: every entry is bounded by the
/// minimum of the configured cache lifetime and the credential's own expiry (so
/// a subject is never served past its token's <c>exp</c>), and the cache is
/// flushed whenever a <c>sys-membership-*</c> tree mutates (observed through the
/// core <see cref="IMutationObserver"/> seam), so a membership change is
/// reflected without a process restart.
/// </summary>
internal sealed class MembershipResolutionCache(
    TimeProvider timeProvider,
    IOptionsMonitor<LatticeMembershipOptions> options) : IMutationObserver
{
    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);

    /// <summary>The number of live cache entries. Exposed for tests.</summary>
    internal int Count => _entries.Count;

    /// <summary>
    /// Attempts to serve a warm entry for <paramref name="cacheKey"/> without
    /// allocating a resolver closure. Lets the caller skip building the
    /// cache-miss delegate on the common warm-hit path.
    /// </summary>
    /// <param name="cacheKey">The credential token used as the cache key.</param>
    /// <param name="subject">The cached subject when warm; otherwise <c>default</c>.</param>
    /// <returns><c>true</c> when a live entry was served.</returns>
    public bool TryGetCached(string cacheKey, out LatticeSubject subject)
    {
        if (options.CurrentValue.ResolutionCacheTtl > TimeSpan.Zero
            && _entries.TryGetValue(cacheKey, out var entry)
            && timeProvider.GetUtcNow() < entry.ExpiresAt)
        {
            subject = entry.Subject;
            return true;
        }

        subject = default;
        return false;
    }

    /// <summary>
    /// Returns the cached subject for <paramref name="cacheKey"/> when warm,
    /// otherwise invokes <paramref name="resolver"/> and caches the result
    /// bounded by the cache lifetime and the token's expiry.
    /// </summary>
    /// <param name="cacheKey">The credential token used as the cache key.</param>
    /// <param name="resolver">Resolves the subject on a cache miss.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    public async ValueTask<LatticeSubject> ResolveAsync(
        string cacheKey,
        Func<CancellationToken, ValueTask<ResolvedSubject>> resolver,
        CancellationToken cancellationToken)
    {
        var ttl = options.CurrentValue.ResolutionCacheTtl;
        var now = timeProvider.GetUtcNow();

        if (ttl > TimeSpan.Zero && _entries.TryGetValue(cacheKey, out var entry) && now < entry.ExpiresAt)
        {
            return entry.Subject;
        }

        var resolved = await resolver(cancellationToken).ConfigureAwait(false);

        if (ttl > TimeSpan.Zero)
        {
            var expiresAt = now + ttl;
            if (resolved.TokenExpiry is { } tokenExpiry && tokenExpiry < expiresAt)
            {
                expiresAt = tokenExpiry;
            }

            if (expiresAt > now)
            {
                _entries[cacheKey] = new Entry(resolved.Subject, expiresAt);
            }
        }

        return resolved.Subject;
    }

    /// <summary>Drops every cached entry. Exposed for tests.</summary>
    internal void Clear() => _entries.Clear();

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        if (mutation.TreeId is { } treeId
            && treeId.StartsWith(MembershipConstants.TreePrefix, StringComparison.Ordinal))
        {
            _entries.Clear();
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
