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

    private static IHost BuildHost(TimeSpan budget, IHostedService service) =>
        new HostBuilder()
            .ConfigureServices(services =>
            {
                services.Configure<HostOptions>(options => options.ShutdownTimeout = budget);
                services.AddSingleton(service);
            })
            .Build();

    /// <summary>
    /// The other half of the abandoned-drain case: a service that observes the
    /// cancellation and returns normally instead of rethrowing, which is what a
    /// service that force-stops its remaining work does.
    /// </summary>
    private sealed class ForceStoppingService(TimeSpan stopDuration) : IHostedService
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
            }
        }
    }

    private static async Task WaitForStartedAsync(IHostApplicationLifetime lifetime)
    {
        var started = new TaskCompletionSource();
        using var registration = lifetime.ApplicationStarted.Register(started.SetResult);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(30));
    }

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
        // depends on whether the service rethrows the cancellation. Both arms are
        // now pinned - see the two process-shell tests below, where an absorbed
        // abandonment returns from RunAsync normally and a rethrown one faults it -
        // so the pre-#2401 exit code was undetermined between success and a
        // crash-shaped abort. Which arm the Orleans silo takes is still not settled
        // here, and deliberately no longer needs to be: the drain signal assigns the
        // code at the moment of overrun, so the reported outcome is the same either
        // way.
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

    [Test]
    public async Task An_abandoned_drain_whose_service_absorbs_the_cancellation_stops_the_host_with_no_error_at_all()
    {
        // Settles the half that the exception test above deliberately left open, and
        // it is the half that matters: a service which force-stops and returns
        // normally leaves the host stopping ENTIRELY cleanly. No throw, no signal of
        // any kind - the abandonment is invisible above the log.
        var budget = TimeSpan.FromMilliseconds(200);
        var service = new ForceStoppingService(TimeSpan.FromSeconds(30));
        using var host = BuildHost(budget, service);

        await host.StartAsync();

        var startedAt = Stopwatch.GetTimestamp();
        Exception? thrown = null;
        try
        {
            await host.StopAsync();
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        var elapsed = Stopwatch.GetElapsedTime(startedAt);

        Assert.Multiple(() =>
        {
            Assert.That(service.StopObservedCancellation, Is.True,
                "the budget must have cancelled the service's stop token, otherwise this is not an abandoned drain and the rest asserts nothing");
            Assert.That(elapsed, Is.LessThan(TimeSpan.FromSeconds(10)),
                "the host must have stopped waiting at the budget rather than awaiting the full 30s stop");
            Assert.That(thrown, Is.Null,
                "an abandoned drain absorbed by the service raises nothing, so nothing downstream can make the process report failure");
        });
    }

    [Test]
    public async Task The_process_shell_completes_normally_when_an_abandoned_drain_absorbs_the_cancellation()
    {
        // The determination issue #2401 asks for, taken to the layer Program.cs
        // actually uses: `await app.RunAsync()`. With the abandonment absorbed,
        // RunAsync returns normally and Environment.ExitCode is never assigned, so
        // the process reports success for a drain that did not finish. That is the
        // defect - not a missing log line, but an orchestrator being told the
        // container stopped cleanly.
        var budget = TimeSpan.FromMilliseconds(200);
        var service = new ForceStoppingService(TimeSpan.FromSeconds(30));
        using var host = BuildHost(budget, service);

        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        var exitCodeBefore = Environment.ExitCode;

        var run = host.RunAsync();
        await WaitForStartedAsync(lifetime);
        lifetime.StopApplication();

        Exception? thrown = null;
        try
        {
            await run.WaitAsync(TimeSpan.FromSeconds(30));
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        Assert.Multiple(() =>
        {
            Assert.That(service.StopObservedCancellation, Is.True,
                "the drain must actually have been abandoned, otherwise this measures an ordinary clean stop");
            Assert.That(thrown, Is.Null, "RunAsync returns normally, exactly as it does for a drain that finished");
            Assert.That(Environment.ExitCode, Is.EqualTo(exitCodeBefore),
                "nothing in the host assigns an exit code, which is why RepoContextDrainSignal has to (issue #2401)");
        });
    }

    [Test]
    public async Task The_process_shell_faults_when_an_abandoned_drain_rethrows_the_cancellation()
    {
        // The opposite arm, and why the pre-#2401 exit code was not merely wrong but
        // UNDETERMINED: with a rethrowing service the same abandonment escapes
        // RunAsync, and an unhandled exception in Program.cs aborts the process
        // instead. So the abandoned drain reported either success or a crash
        // depending on a hosted service's internal choice - and a crash-shaped abort
        // is itself indistinguishable from a real crash. Assigning the code
        // deliberately replaces both with one meaningful value.
        var budget = TimeSpan.FromMilliseconds(200);
        var service = new SlowStoppingService(TimeSpan.FromSeconds(30));
        using var host = BuildHost(budget, service);

        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

        var run = host.RunAsync();
        await WaitForStartedAsync(lifetime);
        lifetime.StopApplication();

        Exception? thrown = null;
        try
        {
            await run.WaitAsync(TimeSpan.FromSeconds(30));
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        Assert.Multiple(() =>
        {
            Assert.That(service.StopObservedCancellation, Is.True,
                "the drain must actually have been abandoned for the two arms to differ only in how the service responds");
            Assert.That(thrown, Is.Not.Null,
                "the same abandonment escapes RunAsync here, which is the second of the two undetermined outcomes");
        });
    }
}
