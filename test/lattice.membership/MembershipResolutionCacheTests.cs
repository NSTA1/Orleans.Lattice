using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="MembershipResolutionCache"/>: warm-hit reuse,
/// token-expiry bounding, the two disabling conditions (zero TTL), the
/// positive-only admission and capped size that keep an unauthenticated or
/// token-rotating caller from growing the map, and change-feed invalidation via
/// the <see cref="IMutationObserver"/> seam.
/// </summary>
public class MembershipResolutionCacheTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static (MembershipResolutionCache Cache, ManualTimeProvider Time) CreateCache(
        TimeSpan? ttl = null)
    {
        var time = new ManualTimeProvider(Start);
        var options = new LatticeMembershipOptions
        {
            ResolutionCacheTtl = ttl ?? TimeSpan.FromMinutes(5),
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeMembershipOptions>>();
        monitor.CurrentValue.Returns(options);
        return (new MembershipResolutionCache(time, monitor), time);
    }

    private static Func<CancellationToken, ValueTask<ResolvedSubject>> Resolver(
        LatticeSubject subject,
        DateTimeOffset? expiry,
        Action onCall) =>
        _ =>
        {
            onCall();
            return new ValueTask<ResolvedSubject>(new ResolvedSubject(subject, expiry));
        };

    [Test]
    public async Task ResolveAsync_warm_entry_is_served_without_re_resolving()
    {
        var (cache, _) = CreateCache();
        var calls = 0;
        var subject = new LatticeSubject("alice");

        var first = await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);
        var second = await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);

        Assert.That(first, Is.EqualTo(subject));
        Assert.That(second, Is.EqualTo(subject));
        Assert.That(calls, Is.EqualTo(1), "the warm second resolution must not re-run the resolver");
    }

    [Test]
    public async Task ResolveAsync_re_resolves_after_the_cache_ttl_elapses()
    {
        var (cache, time) = CreateCache(ttl: TimeSpan.FromMinutes(5));
        var calls = 0;
        var subject = new LatticeSubject("alice");

        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);
        time.Advance(TimeSpan.FromMinutes(6));
        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);

        Assert.That(calls, Is.EqualTo(2));
    }

    [Test]
    public async Task ResolveAsync_never_serves_a_subject_past_its_token_expiry()
    {
        var (cache, time) = CreateCache(ttl: TimeSpan.FromMinutes(30));
        var calls = 0;
        var subject = new LatticeSubject("alice");
        var tokenExpiry = Start + TimeSpan.FromMinutes(2);

        // Cache TTL is 30 min but the token expires in 2 min: the entry must be
        // bounded by the token expiry, not the TTL.
        await cache.ResolveAsync("tok", Resolver(subject, tokenExpiry, () => calls++), default);
        time.Advance(TimeSpan.FromMinutes(3));
        await cache.ResolveAsync("tok", Resolver(subject, tokenExpiry, () => calls++), default);

        Assert.That(calls, Is.EqualTo(2), "an entry must not outlive the token's exp even within the cache TTL");
    }

    [Test]
    public async Task ResolveAsync_zero_ttl_disables_caching()
    {
        var (cache, _) = CreateCache(ttl: TimeSpan.Zero);
        var calls = 0;
        var subject = new LatticeSubject("alice");

        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);
        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);

        Assert.That(calls, Is.EqualTo(2));
        Assert.That(cache.Count, Is.Zero);
    }

    [Test]
    public async Task OnMutationAsync_membership_tree_mutation_flushes_the_cache()
    {
        var (cache, _) = CreateCache();
        var calls = 0;
        var subject = new LatticeSubject("alice");

        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);
        await cache.OnMutationAsync(new LatticeMutation { TreeId = MembershipConstants.EdgesTree }, default);
        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);

        Assert.That(calls, Is.EqualTo(2), "a sys-membership-* mutation must invalidate the cache");
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task OnMutationAsync_unrelated_tree_mutation_leaves_the_cache_warm()
    {
        var (cache, _) = CreateCache();
        var calls = 0;
        var subject = new LatticeSubject("alice");

        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);
        await cache.OnMutationAsync(new LatticeMutation { TreeId = "orders" }, default);
        await cache.ResolveAsync("tok", Resolver(subject, null, () => calls++), default);

        Assert.That(calls, Is.EqualTo(1), "a mutation on a non-membership tree must not flush the cache");
    }

    [Test]
    public void OnMutationAsync_null_tree_id_is_a_no_op()
    {
        var (cache, _) = CreateCache();

        Assert.That(
            async () => await cache.OnMutationAsync(new LatticeMutation(), default),
            Throws.Nothing);
    }

    [Test]
    public async Task Clear_drops_every_entry()
    {
        var (cache, _) = CreateCache();
        var subject = new LatticeSubject("alice");

        _ = await cache.ResolveAsync("tok", Resolver(subject, null, () => { }), default);
        Assert.That(cache.Count, Is.EqualTo(1));

        cache.Clear();

        Assert.That(cache.Count, Is.Zero);
    }

    [Test]
    public async Task ResolveAsync_an_anonymous_verdict_is_never_cached()
    {
        var (cache, _) = CreateCache();
        var calls = 0;

        var first = await cache.ResolveAsync(
            "bogus", Resolver(LatticeSubject.Anonymous, null, () => calls++), default);
        var second = await cache.ResolveAsync(
            "bogus", Resolver(LatticeSubject.Anonymous, null, () => calls++), default);

        Assert.That(first.IsAnonymous, Is.True);
        Assert.That(second.IsAnonymous, Is.True);
        Assert.That(cache.Count, Is.Zero, "an unresolvable credential must not occupy a cache entry");
        Assert.That(calls, Is.EqualTo(2), "an anonymous verdict must be re-resolved, never served warm");
    }

    [Test]
    public async Task ResolveAsync_distinct_unresolvable_tokens_do_not_grow_the_cache()
    {
        var (cache, _) = CreateCache();

        // Every distinct token an unauthenticated caller presents used to mint a
        // permanent entry. The cache must stay empty no matter how many are tried.
        for (var i = 0; i < 10_000; i++)
        {
            _ = await cache.ResolveAsync(
                $"forged-{i}", Resolver(LatticeSubject.Anonymous, null, () => { }), default);
        }

        Assert.That(cache.Count, Is.Zero);
    }

    [Test]
    public async Task ResolveAsync_bounds_the_number_of_cached_subjects()
    {
        var (cache, _) = CreateCache();

        // A resolved population that rotates its tokens mints a new key per
        // token; the map must stay capped rather than growing with the churn.
        for (var i = 0; i < MembershipResolutionCache.MaxCachedSubjects + 500; i++)
        {
            _ = await cache.ResolveAsync(
                $"tok-{i}", Resolver(new LatticeSubject($"user-{i}"), null, () => { }), default);
        }

        Assert.That(cache.Count, Is.LessThanOrEqualTo(MembershipResolutionCache.MaxCachedSubjects));
    }

    [Test]
    public async Task ResolveAsync_still_resolves_correctly_once_the_cache_is_full()
    {
        var (cache, _) = CreateCache();

        for (var i = 0; i < MembershipResolutionCache.MaxCachedSubjects + 500; i++)
        {
            _ = await cache.ResolveAsync(
                $"tok-{i}", Resolver(new LatticeSubject($"user-{i}"), null, () => { }), default);
        }

        var expected = new LatticeSubject("late-arrival");
        var actual = await cache.ResolveAsync("late-tok", Resolver(expected, null, () => { }), default);

        Assert.That(
            actual,
            Is.EqualTo(expected),
            "a full cache must still return the authoritative subject; the bound may cost a lookup, never correctness");
    }

    [Test]
    public async Task ResolveAsync_reclaims_expired_entries_when_the_cache_is_full()
    {
        var (cache, time) = CreateCache(ttl: TimeSpan.FromMinutes(5));

        for (var i = 0; i < MembershipResolutionCache.MaxCachedSubjects; i++)
        {
            _ = await cache.ResolveAsync(
                $"tok-{i}", Resolver(new LatticeSubject($"user-{i}"), null, () => { }), default);
        }

        Assert.That(cache.Count, Is.EqualTo(MembershipResolutionCache.MaxCachedSubjects));

        // Every entry is now stale. Admitting a new key must drop them rather
        // than refuse the insert, so a full-but-dead cache does not stay cold.
        time.Advance(TimeSpan.FromMinutes(6));
        var subject = new LatticeSubject("fresh");
        var calls = 0;

        _ = await cache.ResolveAsync("fresh-tok", Resolver(subject, null, () => calls++), default);
        var warm = await cache.ResolveAsync("fresh-tok", Resolver(subject, null, () => calls++), default);

        Assert.That(cache.Count, Is.EqualTo(1), "the expired entries must have been reclaimed");
        Assert.That(warm, Is.EqualTo(subject));
        Assert.That(calls, Is.EqualTo(1), "the newly admitted entry must be served warm");
    }
}
