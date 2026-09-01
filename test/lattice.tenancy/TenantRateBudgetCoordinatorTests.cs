using Microsoft.Extensions.Options;
using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="TenantRateBudgetCoordinator"/>.</summary>
public sealed class TenantRateBudgetCoordinatorTests
{
    private static IOptionsMonitor<LatticeTenantRateLimiterOptions> Options(LatticeTenantRateLimiterOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantRateLimiterOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static TenantRateBudgetCoordinator Create(
        SiloLocalTenantRateLimiter limiter,
        ManualTimeProvider clock,
        LatticeTenantRateLimiterOptions options,
        ITenantRateProvider rateProvider,
        ILiveSiloCountProvider siloCountProvider,
        ITenantClusterDemandExchange demandExchange) =>
        new(rateProvider, siloCountProvider, demandExchange, limiter, clock, Options(options));

    /// <summary>Counts how many ops the tenant's bucket admits immediately (time held fixed).</summary>
    private static int CountImmediateAdmits(SiloLocalTenantRateLimiter limiter, TenantId tenant, int probes)
    {
        var admitted = 0;
        for (var i = 0; i < probes; i++)
        {
            if (limiter.TryAcquire(tenant))
            {
                admitted++;
            }
        }

        return admitted;
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var options = Options(new LatticeTenantRateLimiterOptions());
        var rate = new FakeRateProvider();
        var silos = new FakeSiloCountProvider(1);
        var exchange = new FakeDemandExchange(null);

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantRateBudgetCoordinator(null!, silos, exchange, limiter, clock, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinator(rate, null!, exchange, limiter, clock, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinator(rate, silos, null!, limiter, clock, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinator(rate, silos, exchange, null!, clock, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinator(rate, silos, exchange, limiter, null!, options), Throws.ArgumentNullException);
            Assert.That(() => new TenantRateBudgetCoordinator(rate, silos, exchange, limiter, clock, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task RunLeaseCycleAsync_static_even_configures_a_bucket_at_the_apportioned_share()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var options = new LatticeTenantRateLimiterOptions { Apportionment = TenantRateApportionmentStrategy.StaticEven };
        var coordinator = Create(
            limiter, clock, options,
            new FakeRateProvider(new TenantRateSpec(tenant, OpsPerSecond: 4000, BurstPercent: 10)),
            new FakeSiloCountProvider(4),
            new FakeDemandExchange(null));

        await coordinator.RunLeaseCycleAsync();

        // 4000 / 4 silos = 1000 share; 10% burst => 100 burst tokens => 101 immediate.
        Assert.Multiple(() =>
        {
            Assert.That(limiter.BucketCount, Is.EqualTo(1));
            Assert.That(CountImmediateAdmits(limiter, tenant, 200), Is.EqualTo(101));
        });
    }

    [Test]
    public async Task RunLeaseCycleAsync_demand_strategy_falls_back_to_static_even_when_no_cluster_total_is_available()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var options = new LatticeTenantRateLimiterOptions { Apportionment = TenantRateApportionmentStrategy.Demand };
        var coordinator = Create(
            limiter, clock, options,
            new FakeRateProvider(new TenantRateSpec(tenant, OpsPerSecond: 4000, BurstPercent: 10)),
            new FakeSiloCountProvider(4),
            new FakeDemandExchange(clusterTotal: null));

        await coordinator.RunLeaseCycleAsync();

        // Null cluster total => static-even 1000 share => 101 immediate.
        Assert.That(CountImmediateAdmits(limiter, tenant, 200), Is.EqualTo(101));
    }

    [Test]
    public async Task RunLeaseCycleAsync_demand_strategy_engages_when_a_cluster_total_is_supplied()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var options = new LatticeTenantRateLimiterOptions
        {
            Apportionment = TenantRateApportionmentStrategy.Demand,
            DemandReserveFraction = 0.2,
        };

        // First cycle bootstraps static-even (2 silos => 500 share, 10% => 51 immediate).
        var bootstrapExchange = new FakeDemandExchange(clusterTotal: null);
        var coordinator = Create(
            limiter, clock, options,
            new FakeRateProvider(new TenantRateSpec(tenant, OpsPerSecond: 1000, BurstPercent: 10)),
            new FakeSiloCountProvider(2),
            bootstrapExchange);
        await coordinator.RunLeaseCycleAsync();

        // Register local demand by exhausting the bootstrap allowance (51 admits).
        var localDemand = CountImmediateAdmits(limiter, tenant, 200);
        Assert.That(localDemand, Is.EqualTo(51), "bootstrap static-even allowance");

        // Second cycle: this silo is the sole demander, so demand-proportional lifts
        // the share to reserve floor (100) + whole pool (800) = 900 => 91 immediate.
        var demandExchange = new FakeDemandExchange(clusterTotal: localDemand);
        var busyCoordinator = Create(
            limiter, clock, options,
            new FakeRateProvider(new TenantRateSpec(tenant, OpsPerSecond: 1000, BurstPercent: 10)),
            new FakeSiloCountProvider(2),
            demandExchange);
        await busyCoordinator.RunLeaseCycleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(demandExchange.LastLocalDemand, Is.EqualTo(localDemand), "coordinator handed local demand to the exchange");
            Assert.That(CountImmediateAdmits(limiter, tenant, 200), Is.EqualTo(91), "demand-proportional share of 900");
        });
    }

    [Test]
    public async Task RunLeaseCycleAsync_floors_a_sub_unit_share_to_one_op_per_second()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var options = new LatticeTenantRateLimiterOptions { Apportionment = TenantRateApportionmentStrategy.StaticEven };

        // clusterRate 1 across 4 silos => share 0, floored to 1 (no div-by-zero,
        // a working bucket that admits at least one op).
        var coordinator = Create(
            limiter, clock, options,
            new FakeRateProvider(new TenantRateSpec(tenant, OpsPerSecond: 1, BurstPercent: 0)),
            new FakeSiloCountProvider(4),
            new FakeDemandExchange(null));

        await coordinator.RunLeaseCycleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(limiter.BucketCount, Is.EqualTo(1));
            Assert.That(limiter.TryAcquire(tenant), Is.True);
        });
    }

    [Test]
    public async Task RunLeaseCycleAsync_prunes_a_tenant_that_no_longer_has_a_configured_rate()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var acme = TenantId.Parse("acme");
        var globex = TenantId.Parse("globex");
        var options = new LatticeTenantRateLimiterOptions { Apportionment = TenantRateApportionmentStrategy.StaticEven };

        var first = Create(
            limiter, clock, options,
            new FakeRateProvider(
                new TenantRateSpec(acme, 1000, 0),
                new TenantRateSpec(globex, 1000, 0)),
            new FakeSiloCountProvider(1),
            new FakeDemandExchange(null));
        await first.RunLeaseCycleAsync();
        Assert.That(limiter.BucketCount, Is.EqualTo(2));

        var second = Create(
            limiter, clock, options,
            new FakeRateProvider(new TenantRateSpec(acme, 1000, 0)),
            new FakeSiloCountProvider(1),
            new FakeDemandExchange(null));
        await second.RunLeaseCycleAsync();

        Assert.Multiple(() =>
        {
            Assert.That(limiter.BucketCount, Is.EqualTo(1));
            Assert.That(limiter.TryAcquire(globex), Is.True, "pruned tenant is inert again");
        });
    }

    [Test]
    public async Task RunLeaseCycleAsync_with_no_configured_tenants_leaves_the_limiter_empty()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var options = new LatticeTenantRateLimiterOptions();
        var coordinator = Create(
            limiter, clock, options,
            new FakeRateProvider(),
            new FakeSiloCountProvider(3),
            new FakeDemandExchange(null));

        await coordinator.RunLeaseCycleAsync();

        Assert.That(limiter.BucketCount, Is.EqualTo(0));
    }

    [Test]
    public async Task RunLeaseCycleAsync_clamps_a_non_positive_silo_count_to_one()
    {
        // Covers line 82: when ILiveSiloCountProvider returns 0 (or negative) the
        // coordinator falls back to 1 so no division-by-zero occurs.
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var options = new LatticeTenantRateLimiterOptions { Apportionment = TenantRateApportionmentStrategy.StaticEven };
        var coordinator = Create(
            limiter, clock, options,
            new FakeRateProvider(new TenantRateSpec(tenant, OpsPerSecond: 1000, BurstPercent: 0)),
            new FakeSiloCountProvider(0),
            new FakeDemandExchange(null));

        await coordinator.RunLeaseCycleAsync();

        // 1000 ops / 1 silo (clamped from 0) = 1000; 0% burst => 1 immediate.
        Assert.Multiple(() =>
        {
            Assert.That(limiter.BucketCount, Is.EqualTo(1));
            Assert.That(limiter.TryAcquire(tenant), Is.True, "bucket was configured at full rate");
        });
    }
}
