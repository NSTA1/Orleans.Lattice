using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Coverage for the caching and degradation behaviour of
/// <see cref="ManagementClusterRuntimeStatisticsSource"/>: the time-to-live that
/// keeps the management-grain query off the sampling timer's hot path, the
/// double-check that stops two racing callers both querying the cluster, the
/// fallback to the last known snapshot when a query fails, and the interval
/// resolution that substitutes the default for a non-positive configured value.
/// </summary>
[TestFixture]
public sealed class ManagementClusterRuntimeStatisticsSourceCacheTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// A clock that returns a scripted sequence of instants, one per read. It
    /// models the interleaving a real race produces - the caller reads a stale
    /// instant before taking the refresh lock and a fresh one after another
    /// caller has already refreshed - without any threading in the test.
    /// </summary>
    private sealed class ScriptedTimeProvider(params DateTimeOffset[] readings) : TimeProvider
    {
        private int _index;

        public override DateTimeOffset GetUtcNow()
        {
            var value = readings[Math.Min(_index, readings.Length - 1)];
            _index++;
            return value;
        }
    }

    private static ManagementClusterRuntimeStatisticsSource Create(
        TimeProvider clock,
        IGrainFactory? grainFactory,
        Action<LatticeScalingSignalOptions>? configure = null,
        ILogger<ManagementClusterRuntimeStatisticsSource>? logger = null)
    {
        var options = new LatticeScalingSignalOptions();
        configure?.Invoke(options);
        return new ManagementClusterRuntimeStatisticsSource(
            Options.Create(options),
            clock,
            grainFactory,
            logger);
    }

    [Test]
    public async Task No_grain_factory_reports_the_single_silo_fallback()
    {
        var source = Create(new MutableTimeProvider(T0), grainFactory: null);

        var snapshot = await source.SampleAsync(CancellationToken.None);

        Assert.That(snapshot.ActiveSiloCount, Is.EqualTo(1));
    }

    [Test]
    public async Task GetActiveReplicaCountAsync_never_reports_fewer_than_one_replica()
    {
        var source = Create(new MutableTimeProvider(T0), grainFactory: null);

        var count = await source.GetActiveReplicaCountAsync(CancellationToken.None);

        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task A_second_sample_inside_the_ttl_is_served_from_cache()
    {
        var clock = new MutableTimeProvider(T0);
        var source = Create(
            clock,
            grainFactory: null,
            configure: o => o.SampleInterval = TimeSpan.FromMinutes(1));

        await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(5));
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.That(second.ActiveSiloCount, Is.EqualTo(1));
    }

    [Test]
    public async Task A_refresh_that_lands_while_waiting_for_the_lock_is_not_repeated()
    {
        // Reading order: [0] outer check on the first call, [1] inner check on
        // the first call, [2] outer check on the second call (past the TTL, so
        // it takes the lock), [3] inner re-check on the second call - which now
        // observes an instant inside the TTL, exactly as it would if a racing
        // caller had refreshed while this one waited. The cached snapshot must
        // be returned without a second cluster query.
        var clock = new ScriptedTimeProvider(
            T0,
            T0,
            T0 + TimeSpan.FromMinutes(10),
            T0 + TimeSpan.FromSeconds(1));

        var source = Create(
            clock,
            grainFactory: null,
            configure: o => o.SampleInterval = TimeSpan.FromMinutes(1));

        await source.SampleAsync(CancellationToken.None);
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.That(second.ActiveSiloCount, Is.EqualTo(1));
    }

    [Test]
    public async Task A_failing_query_falls_back_to_the_last_known_snapshot()
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IManagementGrain>(0).Returns(_ => throw new InvalidOperationException("boom"));

        var clock = new MutableTimeProvider(T0);
        var source = Create(
            clock,
            factory,
            configure: o => o.SampleInterval = TimeSpan.FromMinutes(1),
            logger: NullLogger<ManagementClusterRuntimeStatisticsSource>.Instance);

        var first = await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(10));
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.ActiveSiloCount, Is.EqualTo(1), "The first failure degrades to the fallback.");
            Assert.That(second.ActiveSiloCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_non_positive_sample_interval_falls_back_to_the_default_ttl()
    {
        var clock = new MutableTimeProvider(T0);
        var source = Create(
            clock,
            grainFactory: null,
            configure: o => o.SampleInterval = TimeSpan.Zero);

        await source.SampleAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromMilliseconds(1));
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.That(
            second.ActiveSiloCount,
            Is.EqualTo(1),
            "A zero interval must not disable caching; the default TTL applies.");
    }

    [Test]
    public async Task A_failing_query_after_a_successful_one_reuses_the_cached_snapshot()
    {
        // The first sample succeeds and populates the cache; the second fails
        // past the TTL and must degrade to the last known snapshot rather than
        // to the single-silo fallback.
        var management = Substitute.For<IManagementGrain>();
        var hosts = new Dictionary<SiloAddress, SiloStatus>
        {
            [SiloAddress.New(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 11111), 0)] = SiloStatus.Active,
            [SiloAddress.New(new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 11112), 0)] = SiloStatus.Active,
        };

        var fail = false;
        management.GetHosts(true).Returns(_ => fail
            ? throw new InvalidOperationException("cluster unreachable")
            : hosts);
        management.GetDetailedGrainStatistics().Returns(_ => Array.Empty<DetailedGrainStatistic>());
        management.GetRuntimeStatistics(Arg.Any<SiloAddress[]>())
            .Returns(_ => Array.Empty<SiloRuntimeStatistics>());

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IManagementGrain>(0).Returns(management);

        var clock = new MutableTimeProvider(T0);
        var source = Create(
            clock,
            factory,
            configure: o => o.SampleInterval = TimeSpan.FromMinutes(1),
            logger: NullLogger<ManagementClusterRuntimeStatisticsSource>.Instance);

        var first = await source.SampleAsync(CancellationToken.None);

        fail = true;
        clock.Advance(TimeSpan.FromMinutes(10));
        var second = await source.SampleAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.ActiveSiloCount, Is.EqualTo(2));
            Assert.That(
                second.ActiveSiloCount,
                Is.EqualTo(2),
                "The failed refresh must reuse the last good snapshot, not reset to 1.");
        });
    }

    [Test]
    public void A_cancelled_token_propagates()
    {
        var source = Create(new MutableTimeProvider(T0), grainFactory: null);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            async () => await source.SampleAsync(cts.Token));
    }
}
