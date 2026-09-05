using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the <see cref="AutoSharedDictionaryTrainingService"/> pump's
/// cadence clamp and its two fault arms. The pump is a
/// <see cref="Microsoft.Extensions.Hosting.BackgroundService"/> whose whole job
/// is to keep a best-effort training pass off the ship path, so the arms that
/// matter are the ones that decide whether a bad pass stops the pump (provider
/// disposed during shutdown) or is absorbed and retried (anything else).
/// </summary>
[TestFixture]
public sealed class AutoSharedDictionaryTrainingServicePumpTests
{
    /// <summary>
    /// A manual clock that records the due time of every timer the pump asks
    /// for, so the poll-cadence clamp is directly observable, and fires them on
    /// demand.
    /// </summary>
    private sealed class RecordingClock : TimeProvider
    {
        private readonly List<(DateTimeOffset Due, ManualTimer Timer)> _timers = [];
        private DateTimeOffset _now = DateTimeOffset.UnixEpoch;

        public List<TimeSpan> RequestedDueTimes { get; } = [];

        public override DateTimeOffset GetUtcNow() => _now;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(callback, state);
            lock (_timers)
            {
                RequestedDueTimes.Add(dueTime);
                _timers.Add((_now + dueTime, timer));
            }

            return timer;
        }

        public void Advance(TimeSpan delta)
        {
            _now += delta;
            (DateTimeOffset Due, ManualTimer Timer)[] due;
            lock (_timers)
            {
                due = _timers.Where(t => t.Due <= _now).ToArray();
                _timers.RemoveAll(t => t.Due <= _now);
            }

            foreach (var t in due)
            {
                t.Timer.Fire();
            }
        }

        private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
        {
            public void Fire() => callback(state);

            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            public void Dispose()
            {
            }

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// A clock that can be armed to fault, standing in for any unexpected
    /// failure inside a training pass. The provider reads the clock inside its
    /// training critical section, so arming this is the cleanest way to make an
    /// otherwise-sealed provider's <c>TryTrain</c> throw something that is not
    /// an <see cref="ObjectDisposedException"/>.
    /// </summary>
    private sealed class ArmableFaultingClock : TimeProvider
    {
        public bool Armed { get; set; }

        public override DateTimeOffset GetUtcNow() =>
            Armed
                ? throw new InvalidOperationException("clock unavailable mid-training")
                : DateTimeOffset.UnixEpoch;
    }

    private static CompressionDictionaryTrainingOptions TrainingOptions(TimeSpan minInterval) => new()
    {
        Enabled = true,
        MinSamplesToTrain = 8,
        MinTrainingInterval = minInterval,
        DictionaryCapacityBytes = 4096,
        MaxSampleCount = 4096,
        MaxReservoirBytes = 16L * 1024 * 1024,
    };

    private static void Seed(AutoTrainingCompressionDictionaryProvider provider)
    {
        for (var i = 0; i < 64; i++)
        {
            provider.Observe(System.Text.Encoding.UTF8.GetBytes(
                $"user:{i % 50}|order:{i}|status:shipped|region:eu-west-1|sku:widget-{i % 13}|"));
        }
    }

    private static async Task<bool> WaitFor(Func<bool> condition, int timeoutMs = 10000)
    {
        var start = Environment.TickCount64;
        while (Environment.TickCount64 - start < timeoutMs)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(20);
        }

        return condition();
    }

    // ---- Poll-cadence clamp ----------------------------------------------

    [Test]
    public async Task Pump_raises_a_sub_second_training_interval_to_the_poll_floor()
    {
        // A provider configured to train very often must not turn the pump into
        // a busy-spin: the poll cadence is floored at one second regardless.
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            TrainingOptions(TimeSpan.FromMilliseconds(1)));
        var clock = new RecordingClock();
        var service = new AutoSharedDictionaryTrainingService(
            provider, NullLogger<AutoSharedDictionaryTrainingService>.Instance, clock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        Assert.That(await WaitFor(() => clock.RequestedDueTimes.Count > 0), Is.True);
        await cts.CancelAsync();
        await service.StopAsync(default);

        Assert.That(clock.RequestedDueTimes[0], Is.EqualTo(TimeSpan.FromSeconds(1)));
    }

    [Test]
    public async Task Pump_lowers_a_long_training_interval_to_the_poll_ceiling()
    {
        // The other end of the clamp: a very long training interval must still
        // leave the pump responsive to the provider's own cadence re-check.
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            TrainingOptions(TimeSpan.FromHours(1)));
        var clock = new RecordingClock();
        var service = new AutoSharedDictionaryTrainingService(
            provider, NullLogger<AutoSharedDictionaryTrainingService>.Instance, clock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        Assert.That(await WaitFor(() => clock.RequestedDueTimes.Count > 0), Is.True);
        await cts.CancelAsync();
        await service.StopAsync(default);

        Assert.That(clock.RequestedDueTimes[0], Is.EqualTo(TimeSpan.FromMinutes(1)));
    }

    [Test]
    public async Task Pump_polls_at_the_provider_interval_when_it_is_inside_the_bounds()
    {
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            TrainingOptions(TimeSpan.FromSeconds(17)));
        var clock = new RecordingClock();
        var service = new AutoSharedDictionaryTrainingService(
            provider, NullLogger<AutoSharedDictionaryTrainingService>.Instance, clock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        Assert.That(await WaitFor(() => clock.RequestedDueTimes.Count > 0), Is.True);
        await cts.CancelAsync();
        await service.StopAsync(default);

        Assert.That(clock.RequestedDueTimes[0], Is.EqualTo(TimeSpan.FromSeconds(17)));
    }

    // ---- Fault arms -------------------------------------------------------

    [Test]
    public async Task Pump_stops_quietly_once_the_provider_is_disposed()
    {
        // Provider disposal is the host-shutdown signal reaching the pump from
        // the other direction: it must end the loop rather than log a warning
        // on every remaining tick.
        var provider = new AutoTrainingCompressionDictionaryProvider(TrainingOptions(TimeSpan.FromSeconds(1)));
        Seed(provider);
        var clock = new RecordingClock();
        var logger = new CapturingLogger<AutoSharedDictionaryTrainingService>();
        var service = new AutoSharedDictionaryTrainingService(provider, logger, clock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        Assert.That(await WaitFor(() => clock.RequestedDueTimes.Count > 0), Is.True);

        provider.Dispose();
        clock.Advance(TimeSpan.FromSeconds(2));

        var pump = service.ExecuteTask;
        Assert.That(pump, Is.Not.Null);
        await pump!.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(pump.IsCompletedSuccessfully, Is.True, "a disposed provider must end the pump cleanly");
            Assert.That(logger.Warnings, Is.Empty, "shutdown is not a fault and must not be logged as one");
        });

        await cts.CancelAsync();
        await service.StopAsync(default);
    }

    [Test]
    public async Task Pump_absorbs_a_failed_training_pass_and_keeps_pumping()
    {
        // Training is best-effort. An unexpected fault inside a pass must be
        // logged and retried on the next tick, never allowed to crash the host
        // or silently end the pump.
        var providerClock = new ArmableFaultingClock();
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            TrainingOptions(TimeSpan.FromSeconds(1)), providerClock);
        Seed(provider);

        var pumpClock = new RecordingClock();
        var logger = new CapturingLogger<AutoSharedDictionaryTrainingService>();
        var service = new AutoSharedDictionaryTrainingService(provider, logger, pumpClock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        Assert.That(await WaitFor(() => pumpClock.RequestedDueTimes.Count > 0), Is.True);

        providerClock.Armed = true;
        pumpClock.Advance(TimeSpan.FromSeconds(2));

        Assert.That(await WaitFor(() => logger.Warnings.Count > 0), Is.True,
            "a failed training pass must be logged");
        var warning = logger.Warnings[0];

        // The pump must still be running and must schedule the next poll.
        Assert.That(await WaitFor(() => pumpClock.RequestedDueTimes.Count > 1), Is.True,
            "the pump must schedule another poll after absorbing the fault");

        Assert.Multiple(() =>
        {
            Assert.That(warning.Message, Does.Contain("will retry on the next tick"));
            Assert.That(warning.Exception, Is.TypeOf<InvalidOperationException>());
            Assert.That(service.ExecuteTask!.IsCompleted, Is.False, "the pump must not end on a training fault");
        });

        await cts.CancelAsync();
        await service.StopAsync(default);
    }

    [Test]
    public async Task Pump_recovers_and_trains_after_a_failed_pass()
    {
        // The point of absorbing the fault: the very next tick must be able to
        // publish a dictionary.
        var providerClock = new ArmableFaultingClock { Armed = false };
        using var provider = new AutoTrainingCompressionDictionaryProvider(
            TrainingOptions(TimeSpan.FromSeconds(1)), providerClock);
        Seed(provider);

        var pumpClock = new RecordingClock();
        var logger = new CapturingLogger<AutoSharedDictionaryTrainingService>();
        var service = new AutoSharedDictionaryTrainingService(provider, logger, pumpClock);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        Assert.That(await WaitFor(() => pumpClock.RequestedDueTimes.Count > 0), Is.True);

        providerClock.Armed = true;
        pumpClock.Advance(TimeSpan.FromSeconds(2));
        Assert.That(await WaitFor(() => logger.Warnings.Count > 0), Is.True);
        Assert.That(provider.CurrentDictionaryId, Is.EqualTo(0u));

        providerClock.Armed = false;
        for (var i = 0; i < 5 && provider.CurrentDictionaryId == 0u; i++)
        {
            pumpClock.Advance(TimeSpan.FromSeconds(2));
            await Task.Delay(50);
        }

        Assert.That(provider.CurrentDictionaryId, Is.Not.EqualTo(0u),
            "the pump must train normally once the transient fault clears");

        await cts.CancelAsync();
        await service.StopAsync(default);
    }
}
