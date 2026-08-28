using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="TenantRateBudgetCoordinatorHostedService"/>.</summary>
public sealed class TenantRateBudgetCoordinatorHostedServiceTests
{
    private static IOptionsMonitor<LatticeTenantRateLimiterOptions> Options(LatticeTenantRateLimiterOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantRateLimiterOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static (TenantRateBudgetCoordinatorHostedService Service, SiloLocalTenantRateLimiter Limiter, TenantId Tenant) Build(
        IOptionsMonitor<LatticeTenantRateLimiterOptions> options)
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var coordinator = new TenantRateBudgetCoordinator(
            new FakeRateProvider(new TenantRateSpec(tenant, OpsPerSecond: 1000, BurstPercent: 0)),
            new FakeSiloCountProvider(1),
            new FakeDemandExchange(null),
            limiter,
            clock,
            options);
        var service = new TenantRateBudgetCoordinatorHostedService(
            coordinator, clock, options, Substitute.For<ILogger<TenantRateBudgetCoordinatorHostedService>>());
        return (service, limiter, tenant);
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var options = Options(new LatticeTenantRateLimiterOptions());
        var coordinator = new TenantRateBudgetCoordinator(
            new FakeRateProvider(), new FakeSiloCountProvider(1), new FakeDemandExchange(null), limiter, clock, options);
        var logger = Substitute.For<ILogger<TenantRateBudgetCoordinatorHostedService>>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantRateBudgetCoordinatorHostedService(null!, clock, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinatorHostedService(coordinator, null!, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinatorHostedService(coordinator, clock, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinatorHostedService(coordinator, clock, options, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task StartAsync_runs_a_bootstrap_lease_cycle_that_configures_the_buckets()
    {
        var options = new LatticeTenantRateLimiterOptions
        {
            Apportionment = TenantRateApportionmentStrategy.StaticEven,
            LeaseInterval = TimeSpan.FromHours(1),
        };
        var (service, limiter, _) = Build(Options(options));

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        Assert.That(limiter.BucketCount, Is.EqualTo(1), "bootstrap cycle apportioned the tenant's rate");
    }

    [Test]
    public async Task StartAsync_exposes_the_loop_task()
    {
        var options = new LatticeTenantRateLimiterOptions { LeaseInterval = TimeSpan.FromHours(1) };
        var (service, _, _) = Build(Options(options));

        Assert.That(service.Loop, Is.Null, "no loop before start");

        await service.StartAsync(CancellationToken.None);
        Assert.That(service.Loop, Is.Not.Null);

        await service.StopAsync(CancellationToken.None);
        Assert.That(service.Loop!.IsCompleted, Is.True, "loop completes after stop");
    }

    [Test]
    public async Task StopAsync_without_a_prior_start_is_a_no_op()
    {
        var options = new LatticeTenantRateLimiterOptions();
        var (service, _, _) = Build(Options(options));

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing);
    }

    [Test]
    public void NextPeriod_returns_the_plain_interval_while_cycles_succeed()
    {
        var (service, _, _) = Build(Options(new LatticeTenantRateLimiterOptions()));

        Assert.That(service.NextPeriod(TimeSpan.FromSeconds(30), 0), Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void NextPeriod_doubles_once_per_consecutive_failure()
    {
        var options = new LatticeTenantRateLimiterOptions { MaxLeaseBackoff = TimeSpan.FromHours(1) };
        var (service, _, _) = Build(Options(options));
        var interval = TimeSpan.FromSeconds(30);

        Assert.Multiple(() =>
        {
            Assert.That(service.NextPeriod(interval, 1), Is.EqualTo(TimeSpan.FromMinutes(1)));
            Assert.That(service.NextPeriod(interval, 2), Is.EqualTo(TimeSpan.FromMinutes(2)));
            Assert.That(service.NextPeriod(interval, 3), Is.EqualTo(TimeSpan.FromMinutes(4)));
        });
    }

    [Test]
    public void NextPeriod_clamps_to_the_configured_ceiling()
    {
        var options = new LatticeTenantRateLimiterOptions { MaxLeaseBackoff = TimeSpan.FromMinutes(5) };
        var (service, _, _) = Build(Options(options));
        var interval = TimeSpan.FromSeconds(30);

        Assert.Multiple(() =>
        {
            Assert.That(service.NextPeriod(interval, 20), Is.EqualTo(TimeSpan.FromMinutes(5)));
            // The exponent cap must hold independently of the ceiling, so a very long
            // outage can never overflow the shift into a negative period.
            Assert.That(service.NextPeriod(interval, int.MaxValue), Is.EqualTo(TimeSpan.FromMinutes(5)));
        });
    }

    [Test]
    public void NextPeriod_treats_a_ceiling_at_or_below_the_interval_as_backoff_disabled()
    {
        var options = new LatticeTenantRateLimiterOptions { MaxLeaseBackoff = TimeSpan.FromSeconds(10) };
        var (service, _, _) = Build(Options(options));

        Assert.That(service.NextPeriod(TimeSpan.FromSeconds(30), 5), Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void NextPeriod_falls_back_to_the_default_ceiling_when_it_is_non_positive()
    {
        var options = new LatticeTenantRateLimiterOptions { MaxLeaseBackoff = TimeSpan.Zero };
        var (service, _, _) = Build(Options(options));

        Assert.That(
            service.NextPeriod(TimeSpan.FromSeconds(30), 30),
            Is.EqualTo(LatticeTenantRateLimiterOptions.DefaultMaxLeaseBackoff));
    }

    [Test]
    public void ResolveCycleTimeout_never_exceeds_one_lease_interval()
    {
        // A cycle that could outlast its own tick is the livelock: the loop would
        // spend 100% of its wall clock inside a single stalled registry scan.
        var options = new LatticeTenantRateLimiterOptions { LeaseCycleTimeout = TimeSpan.FromMinutes(10) };
        var (service, _, _) = Build(Options(options));

        Assert.That(service.ResolveCycleTimeout(TimeSpan.FromSeconds(30)), Is.EqualTo(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void ResolveCycleTimeout_honours_a_timeout_shorter_than_the_interval()
    {
        var options = new LatticeTenantRateLimiterOptions { LeaseCycleTimeout = TimeSpan.FromSeconds(5) };
        var (service, _, _) = Build(Options(options));

        Assert.That(service.ResolveCycleTimeout(TimeSpan.FromSeconds(30)), Is.EqualTo(TimeSpan.FromSeconds(5)));
    }

    [Test]
    public void ResolveCycleTimeout_falls_back_to_the_default_when_non_positive()
    {
        var options = new LatticeTenantRateLimiterOptions { LeaseCycleTimeout = TimeSpan.Zero };
        var (service, _, _) = Build(Options(options));

        Assert.That(
            service.ResolveCycleTimeout(TimeSpan.FromHours(1)),
            Is.EqualTo(LatticeTenantRateLimiterOptions.DefaultLeaseCycleTimeout));
    }

    [Test]
    public async Task A_cycle_that_never_completes_is_cancelled_and_the_loop_survives()
    {
        // The observed failure: every shard-root read timing out at the 30s Orleans
        // deadline with no per-cycle bound, so the loop occupied the silo until it
        // was restarted. The cycle is now cancelled at its own bound and the service
        // still shuts down cleanly.
        var clock = new ManualTimeProvider();
        var options = Options(new LatticeTenantRateLimiterOptions
        {
            // The lease interval is long enough that the follow-up tick never fires
            // during the test; only the (deliberately tiny) cycle bound does.
            LeaseInterval = TimeSpan.FromHours(1),
            LeaseCycleTimeout = TimeSpan.FromMilliseconds(200),
        });
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var stalled = new StallingRateProvider();
        var coordinator = new TenantRateBudgetCoordinator(
            stalled,
            new FakeSiloCountProvider(1),
            new FakeDemandExchange(null),
            limiter,
            clock,
            options);
        var service = new TenantRateBudgetCoordinatorHostedService(
            coordinator, clock, options, Substitute.For<ILogger<TenantRateBudgetCoordinatorHostedService>>());

        await service.StartAsync(CancellationToken.None);
        Assert.That(await stalled.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10)), Is.True);

        // The bootstrap cycle is parked inside the registry scan. Without a per-cycle
        // bound it would park forever; the bound cancels it, the loop logs and moves
        // on, and shutdown completes promptly instead of hanging.
        await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(service.Loop!.IsCompleted, Is.True, "the loop is not wedged on the stalled cycle");
    }

    /// <summary>
    /// An <see cref="ITenantRateProvider"/> that parks forever inside its scan,
    /// standing in for a registry read that never drains.
    /// </summary>
    private sealed class StallingRateProvider : ITenantRateProvider
    {
        /// <summary>Completes once the scan has actually been entered.</summary>
        public TaskCompletionSource<bool> Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async IAsyncEnumerable<TenantRateSpec> GetConfiguredRatesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            _ = Entered.TrySetResult(true);
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            yield break;
        }
    }
}
