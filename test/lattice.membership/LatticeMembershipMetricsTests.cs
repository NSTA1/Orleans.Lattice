using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Tests for the <see cref="LatticeMembershipMetrics"/> resolution-cache
/// hit / miss counters on the <c>orleans.lattice.membership</c> meter, asserting
/// they emit <b>real</b> values sourced at <see cref="MembershipResolutionCache"/>:
/// a cold resolve counts one miss, subsequent same-subject resolves count hits,
/// and a resolve after the entry's freshness bound elapses counts a fresh miss.
/// This is the positive counterpart of the removed reserved-seam guard - the
/// signal now lives where the cache lives, with no dependency on the
/// authorization meter above membership in the package graph.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class LatticeMembershipMetricsTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static (MembershipResolutionCache Cache, ManualTimeProvider Time) CreateCache(TimeSpan? ttl = null)
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
        DateTimeOffset? expiry = null) =>
        _ => new ValueTask<ResolvedSubject>(new ResolvedSubject(subject, expiry));

    [Test]
    public void Meter_name_is_the_canonical_membership_meter()
    {
        Assert.That(LatticeMembershipMetrics.MeterName, Is.EqualTo("orleans.lattice.membership"));
        Assert.That(LatticeMembershipMetrics.Meter.Name, Is.EqualTo("orleans.lattice.membership"));
    }

    [Test]
    public void RecordResolutionCacheHit_increments_the_hits_counter()
    {
        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.ResolutionCacheHitsName);

        LatticeMembershipMetrics.RecordResolutionCacheHit();

        Assert.That(hits.Sum(), Is.EqualTo(1));
    }

    [Test]
    public void RecordResolutionCacheMiss_increments_the_misses_counter()
    {
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.ResolutionCacheMissesName);

        LatticeMembershipMetrics.RecordResolutionCacheMiss();

        Assert.That(misses.Sum(), Is.EqualTo(1));
    }

    [Test]
    public void RecordDirectorySearch_matched_records_duration_and_one_hit()
    {
        using var duration = new MeterCollector<double>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchDurationName);
        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchHitsName);
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchMissesName);

        LatticeMembershipMetrics.RecordDirectorySearch(12.5, matched: true);

        Assert.Multiple(() =>
        {
            Assert.That(duration.Measurements.Select(m => m.Value), Is.EqualTo(new[] { 12.5 }));
            Assert.That(hits.Sum(), Is.EqualTo(1), "a matched search counts one hit");
            Assert.That(misses.Sum(), Is.Zero, "a matched search counts no miss");
        });
    }

    [Test]
    public void RecordDirectorySearch_unmatched_records_duration_and_one_miss()
    {
        using var duration = new MeterCollector<double>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchDurationName);
        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchHitsName);
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.DirectorySearchMissesName);

        LatticeMembershipMetrics.RecordDirectorySearch(3.0, matched: false);

        Assert.Multiple(() =>
        {
            Assert.That(duration.Measurements.Select(m => m.Value), Is.EqualTo(new[] { 3.0 }));
            Assert.That(misses.Sum(), Is.EqualTo(1), "an unmatched search counts one miss");
            Assert.That(hits.Sum(), Is.Zero, "an unmatched search counts no hit");
        });
    }

    [Test]
    public async Task Cold_resolve_counts_one_miss_and_warm_re_resolves_count_hits()
    {
        var (cache, _) = CreateCache();
        var subject = new LatticeSubject("alice");

        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.ResolutionCacheHitsName);
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.ResolutionCacheMissesName);

        // First resolve is cold (one miss); the next three are warm (three hits).
        await cache.ResolveAsync("tok", Resolver(subject), default);
        await cache.ResolveAsync("tok", Resolver(subject), default);
        await cache.ResolveAsync("tok", Resolver(subject), default);
        await cache.ResolveAsync("tok", Resolver(subject), default);

        Assert.That(misses.Sum(), Is.EqualTo(1), "only the cold resolve is a miss");
        Assert.That(hits.Sum(), Is.EqualTo(3), "each subsequent same-subject resolve is a hit");
    }

    [Test]
    public async Task Resolve_after_the_ttl_elapses_counts_a_fresh_miss()
    {
        var (cache, time) = CreateCache(ttl: TimeSpan.FromMinutes(5));
        var subject = new LatticeSubject("alice");

        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.ResolutionCacheMissesName);

        await cache.ResolveAsync("tok", Resolver(subject), default);   // cold miss
        time.Advance(TimeSpan.FromMinutes(6));                          // entry expires
        await cache.ResolveAsync("tok", Resolver(subject), default);   // fresh miss

        Assert.That(misses.Sum(), Is.EqualTo(2), "an expired entry re-resolves and counts a second miss");
    }

    [Test]
    public async Task TryGetCached_warm_serve_counts_a_hit_and_a_miss_serves_no_double_count()
    {
        var (cache, _) = CreateCache();
        var subject = new LatticeSubject("alice");

        // Warm the cache (one miss recorded by the resolve).
        await cache.ResolveAsync("tok", Resolver(subject), default);

        using var hits = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.ResolutionCacheHitsName);
        using var misses = new MeterCollector<long>(
            LatticeMembershipMetrics.MeterName, LatticeMembershipMetrics.ResolutionCacheMissesName);

        Assert.That(cache.TryGetCached("tok", out _), Is.True);
        Assert.That(hits.Sum(), Is.EqualTo(1), "a warm TryGetCached serve is a hit");

        // A TryGetCached miss records nothing: the miss is counted by the
        // ResolveAsync that necessarily follows it, so the warm fast path never
        // double-counts a single lookup.
        Assert.That(cache.TryGetCached("cold", out _), Is.False);
        Assert.That(misses.Sum(), Is.Zero, "a TryGetCached miss is not counted on its own");
    }
}
