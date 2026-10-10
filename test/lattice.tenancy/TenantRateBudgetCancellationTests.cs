using Microsoft.Extensions.Options;
using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Cancellation fences on delayed lease-cycle collaborators.</summary>
public sealed class TenantRateBudgetCancellationTests
{
    [Test]
    public async Task RunLeaseCycleAsync_cancelled_delayed_exchange_does_not_replace_the_bucket()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        limiter.Configure(tenant, Frequency, 0);
        Assert.That(limiter.TryAcquire(tenant), Is.True);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var response = new TaskCompletionSource<long?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var exchange = Substitute.For<ITenantClusterDemandExchange>();
        exchange.ExchangeAsync(tenant, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                entered.TrySetResult();
                return new ValueTask<long?>(response.Task);
            });
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantRateLimiterOptions>>();
        monitor.CurrentValue.Returns(new LatticeTenantRateLimiterOptions());
        var coordinator = new TenantRateBudgetCoordinator(
            new FakeRateProvider(new TenantRateSpec(tenant, 100, 0)),
            new FakeSiloCountProvider(1), exchange, limiter, clock, monitor);
        using var cancellation = new CancellationTokenSource();

        var cycle = coordinator.RunLeaseCycleAsync(cancellation.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        cancellation.Cancel();
        response.SetResult(1);
        try
        {
            await cycle;
        }
        catch (OperationCanceledException)
        {
        }

        Assert.That(limiter.TryAcquire(tenant), Is.False, "cancelled grant must not reset the exhausted bucket");
        clock.Advance(Frequency / 100);
        Assert.That(limiter.TryAcquire(tenant), Is.False, "cancelled grant must not increase the old rate");
        Assert.That(async () => await cycle, Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task RunLeaseCycleAsync_cancelled_enumeration_does_not_prune_existing_buckets()
    {
        var clock = new ManualTimeProvider();
        var limiter = new SiloLocalTenantRateLimiter(clock);
        var tenant = TenantId.Parse("acme");
        limiter.Configure(tenant, Frequency, 0);
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        using var cancellation = new CancellationTokenSource();
        var provider = Substitute.For<ITenantRateProvider>();
        provider.GetConfiguredRatesAsync(Arg.Any<CancellationToken>())
            .Returns(_ => CancelAtEnd(cancellation));
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantRateLimiterOptions>>();
        monitor.CurrentValue.Returns(new LatticeTenantRateLimiterOptions());
        var coordinator = new TenantRateBudgetCoordinator(
            provider, new FakeSiloCountProvider(1), new FakeDemandExchange(null), limiter, clock, monitor);

        var cycle = coordinator.RunLeaseCycleAsync(cancellation.Token);
        try
        {
            await cycle;
        }
        catch (OperationCanceledException)
        {
        }

        Assert.That(limiter.BucketCount, Is.EqualTo(1));
        Assert.That(limiter.TryAcquire(tenant), Is.False);
        Assert.That(async () => await cycle, Throws.InstanceOf<OperationCanceledException>());
    }

    private static async IAsyncEnumerable<TenantRateSpec> CancelAtEnd(CancellationTokenSource cancellation)
    {
        await Task.CompletedTask;
        cancellation.Cancel();
        yield break;
    }
}
