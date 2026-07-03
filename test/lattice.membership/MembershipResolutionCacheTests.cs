using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Unit tests for <see cref="MembershipResolutionCache"/>: warm-hit reuse,
/// token-expiry bounding, the two disabling conditions (zero TTL), and
/// change-feed invalidation via the <see cref="IMutationObserver"/> seam.
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
        await cache.OnMutationAsync(new LatticeMutation { TreeId = MembershipConstants.UsersTree }, default);
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
    public void Clear_drops_every_entry()
    {
        var (cache, _) = CreateCache();
        var subject = new LatticeSubject("alice");

        _ = cache.ResolveAsync("tok", Resolver(subject, null, () => { }), default).AsTask().Result;
        Assert.That(cache.Count, Is.EqualTo(1));

        cache.Clear();

        Assert.That(cache.Count, Is.Zero);
    }
}
