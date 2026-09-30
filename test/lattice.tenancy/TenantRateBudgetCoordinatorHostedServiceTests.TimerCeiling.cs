namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Tests that the lease loop's per-cycle timeout is held to the longest delay a
/// <see cref="CancellationTokenSource"/> accepts, as the tick period already is.
/// </summary>
public sealed partial class TenantRateBudgetCoordinatorHostedServiceTests
{
    [Test]
    public void ResolveCycleTimeout_clamps_a_timeout_beyond_the_timer_ceiling()
    {
        // Regression: the cycle timeout was clamped only to the lease interval, which
        // is itself unclamped, so an out-of-range interval let an out-of-range timeout
        // through to new CancellationTokenSource(timeout, clock) - which throws for a
        // finite delay above ~49.71 days, exactly as the PeriodicTimer period does.
        var options = new LatticeTenantRateLimiterOptions { LeaseCycleTimeout = TimeSpan.FromDays(60) };
        var (service, _, _) = Build(Options(options));

        Assert.Multiple(() =>
        {
            Assert.That(service.ResolveCycleTimeout(TimeSpan.FromDays(60)), Is.EqualTo(TimerCeiling));
            Assert.That(
                () => new CancellationTokenSource(TimeSpan.FromDays(60), TimeProvider.System),
                Throws.InstanceOf<ArgumentOutOfRangeException>(),
                "anti-vacuity: the unclamped timeout is one CancellationTokenSource itself refuses");
            Assert.That(() => new CancellationTokenSource(TimerCeiling, TimeProvider.System).Dispose(), Throws.Nothing);
        });
    }

    [Test]
    public async Task A_lease_interval_and_cycle_timeout_beyond_the_timer_ceiling_do_not_kill_the_loop()
    {
        // Both beyond the ceiling: the interval no longer caps the timeout below it, so
        // before the fix the bootstrap cycle's deadline threw outside the cycle's fault
        // handler, the loop task faulted before apportioning anything, and StopAsync
        // rethrew the ArgumentOutOfRangeException at shutdown.
        var options = new LatticeTenantRateLimiterOptions
        {
            Apportionment = TenantRateApportionmentStrategy.StaticEven,
            LeaseInterval = TimeSpan.FromDays(60),
            LeaseCycleTimeout = TimeSpan.FromDays(60),
        };
        var (service, limiter, _) = Build(Options(options));

        await service.StartAsync(CancellationToken.None);
        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing);

        Assert.Multiple(() =>
        {
            Assert.That(limiter.BucketCount, Is.EqualTo(1), "the bootstrap cycle ran and apportioned the rate");
            Assert.That(service.Loop!.IsFaulted, Is.False, "the lease loop must not die on an out-of-range timeout");
        });
    }
}
