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
}
