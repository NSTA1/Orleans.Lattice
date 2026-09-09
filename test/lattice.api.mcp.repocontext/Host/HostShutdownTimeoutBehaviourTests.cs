using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Pins what the generic host actually does when
/// <see cref="HostOptions.ShutdownTimeout"/> expires before every hosted service
/// has stopped, because the whole design of the abandoned-drain signal rests on
/// it and it was previously an inference rather than an observation.
/// </summary>
/// <remarks>
/// <para>
/// The question that matters: when the budget expires, does the host still raise
/// <see cref="IHostApplicationLifetime.ApplicationStopped"/>? If it does, then a
/// signal bound to that event reports "drain complete" for a drain that was in
/// fact abandoned mid-deactivation, which is worse than silence - it is a
/// confident false positive. If it does not, the failure is merely silent.
/// </para>
/// <para>
/// These tests use a plain <see cref="HostBuilder"/> and sub-second budgets, so
/// they cost milliseconds and need no container.
/// </para>
/// </remarks>
[TestFixture]
public sealed class HostShutdownTimeoutBehaviourTests
{
    /// <summary>
    /// A hosted service whose stop takes longer than the budget. It observes the
    /// cancellation token, which is what a well-behaved service does and what
    /// Orleans' silo does: the budget cancels the token, the service abandons its
    /// remaining work, and deactivation is left incomplete.
    /// </summary>
    private sealed class SlowStoppingService(TimeSpan stopDuration) : IHostedService
    {
        public bool StopObservedCancellation { get; private set; }

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(stopDuration, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                StopObservedCancellation = true;
                throw;
            }
        }
    }

    private static IHost BuildHost(TimeSpan budget, SlowStoppingService service) =>
        new HostBuilder()
            .ConfigureServices(services =>
            {
                services.Configure<HostOptions>(options => options.ShutdownTimeout = budget);
                services.AddSingleton<IHostedService>(service);
            })
            .Build();

    [Test]
    public async Task The_host_raises_ApplicationStopped_even_when_the_shutdown_budget_expires()
    {
        var budget = TimeSpan.FromMilliseconds(200);
        var service = new SlowStoppingService(TimeSpan.FromSeconds(30));
        using var host = BuildHost(budget, service);

        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        var stoppedFired = false;
        lifetime.ApplicationStopped.Register(() => stoppedFired = true);

        await host.StartAsync();

        var startedAt = Stopwatch.GetTimestamp();
        try
        {
            await host.StopAsync();
        }
        catch (OperationCanceledException)
        {
            // Whether the abandonment also surfaces as a throw is asserted separately.
        }

        var elapsed = Stopwatch.GetElapsedTime(startedAt);

        Assert.Multiple(() =>
        {
            Assert.That(service.StopObservedCancellation, Is.True,
                "the budget must have cancelled the service's stop token, otherwise this test is not exercising an abandoned drain");
            Assert.That(elapsed, Is.LessThan(TimeSpan.FromSeconds(10)),
                "the host must have stopped waiting at the budget rather than awaiting the full 30s stop");
            Assert.That(stoppedFired, Is.True,
                "ApplicationStopped is raised even though the drain was abandoned, which is why a signal bound to it cannot by itself distinguish a completed drain from an abandoned one");
        });
    }

    [Test]
    public async Task The_host_propagates_a_hosted_services_stop_exception_rather_than_swallowing_it()
    {
        var budget = TimeSpan.FromMilliseconds(200);
        var service = new SlowStoppingService(TimeSpan.FromSeconds(30));
        using var host = BuildHost(budget, service);

        await host.StartAsync();

        Exception? thrown = null;
        try
        {
            await host.StopAsync();
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        // Deliberately narrow: this says the host PROPAGATES what a hosted service
        // throws on stop. It does NOT say every abandoned drain throws, because that
        // depends on whether the service rethrows the cancellation - this test's
        // service does, whereas a service that force-stops and returns normally
        // would leave the host stopping cleanly and the process exiting zero. Which
        // of the two the silo does is not settled here.
        Assert.That(thrown, Is.Not.Null,
            "a hosted service that throws on stop must not have its exception swallowed by the host");
    }

    [Test]
    public async Task A_drain_that_finishes_inside_the_budget_neither_cancels_nor_throws()
    {
        var budget = TimeSpan.FromSeconds(30);
        var service = new SlowStoppingService(TimeSpan.FromMilliseconds(50));
        using var host = BuildHost(budget, service);

        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        var stoppedFired = false;
        lifetime.ApplicationStopped.Register(() => stoppedFired = true);

        await host.StartAsync();
        await host.StopAsync();

        Assert.Multiple(() =>
        {
            Assert.That(service.StopObservedCancellation, Is.False);
            Assert.That(stoppedFired, Is.True);
        });
    }
}
