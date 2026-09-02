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
        var firstAttemptThrew = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var monitorGrain = Substitute.For<ILatticeBackupHealthMonitorGrain>();
        monitorGrain.EnsureStartedAsync().Returns(_ =>
        {
            // Signal that we are about to throw so the test can cancel via StopAsync.
            firstAttemptThrew.TrySetResult();
            throw new InvalidOperationException("silo not ready");
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
}
