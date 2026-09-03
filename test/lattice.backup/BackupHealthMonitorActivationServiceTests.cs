using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Runtime;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="BackupHealthMonitorActivationService"/> that do not
/// require a live silo. Exercises the disabled-options early-exit path (lines
/// 31-32), the successful-on-first-attempt path (lines 40-44), the retry-then-
/// succeed path (lines 46-51, 58), and cancellation during the retry delay (lines
/// 53-55), using a fake <see cref="IGrainFactory"/> that controls whether
/// <see cref="ILatticeBackupHealthMonitorGrain.EnsureStartedAsync"/> succeeds or
/// throws.
/// </summary>
[TestFixture]
public sealed class BackupHealthMonitorActivationServiceTests
{
    private static BackupHealthMonitorActivationService CreateService(
        IGrainFactory grainFactory,
        bool enabled = true)
    {
        var options = new LatticeBackupHealthOptions { Enabled = enabled };
        var monitor = Substitute.For<IOptionsMonitor<LatticeBackupHealthOptions>>();
        monitor.CurrentValue.Returns(options);
        return new BackupHealthMonitorActivationService(
            grainFactory,
            monitor,
            NullLogger<BackupHealthMonitorActivationService>.Instance);
    }

    [Test]
    public async Task ExecuteAsync_disabled_options_returns_without_calling_grain()
    {
        // Lines 31-32: when Enabled is false the service logs and returns immediately.
        var grainFactory = Substitute.For<IGrainFactory>();
        using var service = CreateService(grainFactory, enabled: false);

        await service.StartAsync(CancellationToken.None);

        // Await the background execute task so lines 31-32 are reached before the
        // assertion runs. ExecuteTask is exposed by BackgroundService (.NET 6+).
        var executeTask = service.ExecuteTask;
        if (executeTask is not null)
        {
            await executeTask.WaitAsync(TimeSpan.FromSeconds(10));
        }

        grainFactory.DidNotReceive().GetGrain<ILatticeBackupHealthMonitorGrain>(Arg.Any<string>());
    }

    [Test]
    public async Task ExecuteAsync_enabled_calls_grain_once_when_it_succeeds_immediately()
    {
        // Lines 40-44: the happy path - EnsureStartedAsync returns, service exits.
        var monitorGrain = Substitute.For<ILatticeBackupHealthMonitorGrain>();
        monitorGrain.EnsureStartedAsync().Returns(Task.CompletedTask);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeBackupHealthMonitorGrain>(Arg.Any<string>())
            .Returns(monitorGrain);

        using var service = CreateService(grainFactory, enabled: true);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await service.StartAsync(cts.Token);
        // Give the background task a moment to complete.
        await Task.Delay(200, cts.Token);

        await monitorGrain.Received(1).EnsureStartedAsync();
    }

    [Test]
    public async Task ExecuteAsync_retries_after_one_transient_failure_then_succeeds()
    {
        // Lines 46-48, 51, 58: the first EnsureStartedAsync call throws, the service
        // logs and waits (Task.Delay), then the second call succeeds.
        var callCount = 0;
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var monitorGrain = Substitute.For<ILatticeBackupHealthMonitorGrain>();
        monitorGrain.EnsureStartedAsync().Returns(_ =>
        {
            callCount++;
            if (callCount == 1)
                throw new InvalidOperationException("silo not ready");
            tcs.TrySetResult();
            return Task.CompletedTask;
        });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeBackupHealthMonitorGrain>(Arg.Any<string>())
            .Returns(monitorGrain);

        using var service = CreateService(grainFactory, enabled: true);
        await service.StartAsync(CancellationToken.None);

        // Wait until the second call has returned (or time out so the test fails).
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(callCount, Is.EqualTo(2));
    }

    [Test]
    public async Task ExecuteAsync_cancellation_during_delay_returns_cleanly()
    {
        // Lines 53-55: OperationCanceledException thrown by Task.Delay(stoppingToken) is
        // caught and the method returns. Strategy: the first EnsureStartedAsync throws,
        // which puts the service into Task.Delay(250ms). We cancel the stopping token via
        // StopAsync, which cancels that delay, triggering the OperationCanceledException
        // catch block. A TCS signals that the first attempt is complete so StopAsync is
        // not called until the retry delay is in flight (preventing cancellation before
        // the delay even starts).
        //
        // The TCS is completed in a finally, i.e. AFTER the throw, not before it. Setting
        // it before the throw did not establish what the comment above claims: the test
        // could resume and cancel while the service was still unwinding, so the run being
        // exercised was sometimes the throw-racing-cancellation path rather than the
        // delay-cancellation path this case is about. That path now has its own test.
        var firstAttemptThrew = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var monitorGrain = Substitute.For<ILatticeBackupHealthMonitorGrain>();
        monitorGrain.EnsureStartedAsync().Returns(_ =>
        {
            try
            {
                throw new InvalidOperationException("silo not ready");
            }
            finally
            {
                firstAttemptThrew.TrySetResult();
            }
        });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeBackupHealthMonitorGrain>(Arg.Any<string>())
            .Returns(monitorGrain);

        using var service = CreateService(grainFactory, enabled: true);
        await service.StartAsync(CancellationToken.None);

        // Wait until the first EnsureStartedAsync has thrown and the service has entered
        // Task.Delay. Then stop the service; StopAsync cancels the stoppingToken, which
        // cancels the pending delay and exercises the OperationCanceledException branch.
        await firstAttemptThrew.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await service.StopAsync(CancellationToken.None);

        var executeTask = service.ExecuteTask;
        if (executeTask is not null)
        {
            Assert.That(executeTask.IsCompleted, Is.True);
            Assert.That(executeTask.IsFaulted, Is.False);
        }
    }

    /// <summary>
    /// A shutdown that lands between the grain call failing and the exception being
    /// handled must still stop cleanly, not fault the background service.
    /// </summary>
    /// <remarks>
    /// The previous handler was
    /// <c>catch (Exception) when (!stoppingToken.IsCancellationRequested)</c>. An
    /// exception filter is evaluated at throw time, so cancelling in that window made the
    /// filter false and left the exception uncaught: it escaped <c>ExecuteAsync</c> and
    /// faulted the task. That matters beyond tidiness, because
    /// <see cref="BackgroundServiceExceptionBehavior"/> defaults to
    /// <see cref="BackgroundServiceExceptionBehavior.StopHost"/> - so a benign "silo not
    /// ready" arriving during shutdown could take the whole host down.
    /// <para>
    /// This drives that window deterministically rather than hoping to hit it: the fake
    /// grain cancels the service's own stopping token <b>first</b> and only then throws,
    /// which is exactly the interleaving the filter mishandled. It failed reliably against
    /// the old handler and passes against the current one.
    /// </para>
    /// </remarks>
    [Test]
    public async Task ExecuteAsync_cancelled_while_the_grain_call_is_failing_returns_cleanly()
    {
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        BackupHealthMonitorActivationService? service = null;

        var monitorGrain = Substitute.For<ILatticeBackupHealthMonitorGrain>();
        monitorGrain.EnsureStartedAsync().Returns<Task>(call =>
        {
            // Request cancellation BEFORE throwing, so the exception is raised with the
            // stopping token already cancelled - the exact ordering that made the old
            // filter decline to catch. StopAsync cancels its token synchronously before
            // its first await, so the token is already cancelled when the throw happens
            // even though the returned task is not awaited here (awaiting it would
            // deadlock: it waits on the very execute task this call is running inside).
            _ = call;
            _ = service!.StopAsync(CancellationToken.None);
            stopping.TrySetResult();
            throw new InvalidOperationException("silo not ready");
        });

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILatticeBackupHealthMonitorGrain>(Arg.Any<string>())
            .Returns(monitorGrain);

        using var created = CreateService(grainFactory, enabled: true);
        service = created;

        await created.StartAsync(CancellationToken.None);
        await stopping.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var executeTask = created.ExecuteTask;
        if (executeTask is not null)
        {
            await executeTask.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.That(executeTask.IsFaulted, Is.False,
                "A shutdown racing a retryable startup failure faulted the background service. "
                + "With the default StopHost behaviour that takes the host down over a benign "
                + "'not ready yet' on the way out.");
        }
    }
}
