using Microsoft.Extensions.Options;
using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Executable production seams used by the rate-budget refinement mapping.</summary>
public sealed class TenantRateBudgetRefinementTests
{
    [Test]
    public void Apportionment_matches_every_bounded_model_allocation_input()
    {
        foreach (var rate in new long[] { 1, 2 })
        {
            foreach (var silos in new[] { 0, 1, 2 })
            {
                foreach (var local in new long[] { 0, 1, 2 })
                {
                    foreach (var total in new long[] { 0, 2 })
                    {
                        var expected = total == 0
                            ? rate / Math.Max(1, silos)
                            : local >= total ? rate : rate * local / total;
                        var actual = TenantBudgetApportionment.DemandProportionalShare(
                            rate, silos, local, total, reserveFraction: 0);
                        Assert.That(Math.Max(1, actual), Is.EqualTo(Math.Max(1, expected)),
                            $"rate={rate}, silos={silos}, local={local}, total={total}");
                    }
                }
            }
        }
    }

    [Test]
    public async Task RunLeaseCycleAsync_membership_change_is_observed_only_on_the_next_cycle()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var silos = Substitute.For<ILiveSiloCountProvider>();
        silos.GetLiveSiloCountAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<int>(1));
        var coordinator = Create(limiter, clock, tenant, 2, silos);
        await coordinator.RunLeaseCycleAsync();
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        Assert.That(limiter.TryAcquire(tenant), Is.False);

        silos.GetLiveSiloCountAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<int>(2));
        clock.Advance(Frequency / 2);
        Assert.That(limiter.TryAcquire(tenant), Is.True, "old share is retained before refresh");
        await coordinator.RunLeaseCycleAsync();
        Assert.That(limiter.TryAcquire(tenant), Is.True, "changed share installs a fresh bucket");
        clock.Advance(Frequency / 2);
        Assert.That(limiter.TryAcquire(tenant), Is.False, "new live count halves the share");

        silos.GetLiveSiloCountAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<int>(1));
        await coordinator.RunLeaseCycleAsync();
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        clock.Advance(Frequency / 2);
        Assert.That(limiter.TryAcquire(tenant), Is.True, "departed silo's share returns on refresh");
    }

    [Test]
    public async Task RunLeaseCycleAsync_recreated_coordinator_does_not_reset_unchanged_bucket()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var silos = new FakeSiloCountProvider(1);
        await Create(limiter, clock, tenant, 1, silos).RunLeaseCycleAsync();
        Assert.That(limiter.TryAcquire(tenant), Is.True);

        await Create(limiter, clock, tenant, 1, silos).RunLeaseCycleAsync();
        Assert.That(limiter.TryAcquire(tenant), Is.False, "coordinator recreation retains singleton debt");
    }

    [Test]
    public async Task RunLeaseCycleAsync_failed_refresh_after_cadence_retains_the_previous_bucket()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var silos = new FakeSiloCountProvider(1);
        await Create(limiter, clock, tenant, 1, silos).RunLeaseCycleAsync();
        var exchange = Substitute.For<ITenantClusterDemandExchange>();
        exchange.ExchangeAsync(Arg.Any<TenantId>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<long?>(Task.FromException<long?>(new InvalidOperationException("exchange failed"))));
        var options = Substitute.For<IOptionsMonitor<LatticeTenantRateLimiterOptions>>();
        options.CurrentValue.Returns(new LatticeTenantRateLimiterOptions());
        var failed = new TenantRateBudgetCoordinator(
            new FakeRateProvider(new TenantRateSpec(tenant, 100, 0)), silos, exchange, limiter, clock, options);
        clock.AdvanceSeconds(100);
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        Assert.That(async () => await failed.RunLeaseCycleAsync(), Throws.InvalidOperationException);
        Assert.That(limiter.TryAcquire(tenant), Is.False, "cadence and failure do not expire enforcing debt");
        clock.Advance(Frequency / 100);
        Assert.That(limiter.TryAcquire(tenant), Is.False, "failed grant does not replace the old rate");
    }

    [Test]
    public async Task RunLeaseCycleAsync_budget_change_is_deferred_until_refresh()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        var silos = new FakeSiloCountProvider(1);
        await Create(limiter, clock, tenant, 1, silos).RunLeaseCycleAsync();
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        var changed = Create(limiter, clock, tenant, 2, silos);
        Assert.That(limiter.TryAcquire(tenant), Is.False, "new registry spec alone does not update the bucket");
        await changed.RunLeaseCycleAsync();
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        clock.Advance(Frequency / 2);
        Assert.That(limiter.TryAcquire(tenant), Is.True);
    }

    [TestCase(1, 2)]
    [TestCase(2, 2)]
    [TestCase(10, 3)]
    public void Apportionment_floored_shares_and_GCRA_bursts_are_not_a_strict_cluster_ceiling(long rate, int silos)
    {
        var share = Math.Max(1, TenantBudgetApportionment.StaticEvenShare(rate, silos));
        var emission = TenantTokenBucket.ComputeEmissionIntervalTicks(share, Frequency);
        var tolerance = TenantTokenBucket.ComputeBurstToleranceTicks(share, 50, Frequency);
        long clusterAdmits = 0;
        for (var silo = 0; silo < silos; silo++)
        {
            var bucket = new TenantTokenBucket(emission, tolerance);
            for (var probe = 0; probe < 10; probe++)
            {
                if (bucket.TryAcquire(StartTimestamp))
                {
                    clusterAdmits++;
                }
            }
        }

        Assert.That(clusterAdmits, Is.EqualTo(silos * (tolerance / emission + 1)));
        Assert.That(share, Is.GreaterThanOrEqualTo(1));
    }

    private static TenantRateBudgetCoordinator Create(
        SiloLocalTenantRateLimiter limiter, ManualTimeProvider clock, TenantId tenant,
        long rate, ILiveSiloCountProvider silos)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeTenantRateLimiterOptions>>();
        options.CurrentValue.Returns(new LatticeTenantRateLimiterOptions
        {
            Apportionment = TenantRateApportionmentStrategy.StaticEven,
        });
        return new TenantRateBudgetCoordinator(
            new FakeRateProvider(new TenantRateSpec(tenant, rate, 0)), silos,
            new FakeDemandExchange(null), limiter, clock, options);
    }
}
