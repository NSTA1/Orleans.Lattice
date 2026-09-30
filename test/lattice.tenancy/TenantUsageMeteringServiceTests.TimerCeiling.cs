using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// The metering loop's cadence edges. <see cref="TenantUsageAccountingOptions.MeterInterval"/>
/// was handed straight to <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>,
/// whose only caught exception was cancellation: an interval above the timer
/// ceiling threw <see cref="ArgumentOutOfRangeException"/> out of the loop and
/// metering stopped for the life of the silo (then rethrew from
/// <see cref="TenantUsageMeteringService.StopAsync"/>), and a live reload to zero
/// re-metered every tenant back to back with no pause.
/// </summary>
public sealed partial class TenantUsageMeteringServiceTests
{
    private static (TenantUsageMeteringService Service, TenantUsageAccountingOptions Options) CreateOnClock(
        ITenantRegistry registry,
        TimeProvider clock,
        TimeSpan interval)
    {
        var accounting = new TenantUsageAccountingOptions
        {
            MeterInterval = interval,
            PublishMinAbsoluteDelta = 0,
            PublishMinRelativeDelta = 0,
        };

        // One mutable instance behind CurrentValue, so a test can reload the
        // cadence mid-run exactly as an options-monitor change would.
        var options = Substitute.For<IOptionsMonitor<TenantUsageAccountingOptions>>();
        options.CurrentValue.Returns(accounting);

        var cluster = Options.Create(new Orleans.Configuration.ClusterOptions { ClusterId = "cluster-a" });
        var service = new TenantUsageMeteringService(
            registry,
            new TenantUsagePublisher(new RecordingStore(), cluster, options),
            new TenantOverageMeter(new OverageTestData.FakeTenantOverageStore(), cluster),
            GrainFactoryWith([]),
            clock,
            options,
            NullLogger<TenantUsageMeteringService>.Instance);

        return (service, accounting);
    }

    /// <summary>A registry whose every enumeration counts, and releases a waiter on the first.</summary>
    private static ITenantRegistry CountingRegistry(Action onCycle)
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            onCycle();
            return EmptyStream();
        });

        return registry;
    }

    [Test]
    public void ResolveMeterDelay_clamps_an_interval_above_the_timer_ceiling()
    {
        var (service, options) = CreateOnClock(CountingRegistry(() => { }), new ManualTimeProvider(), TimeSpan.MaxValue);

        Assert.That(service.ResolveMeterDelay(), Is.EqualTo(TenantUsageMeteringService.MaxMeterDelay));

        options.MeterInterval = TimeSpan.FromDays(60);
        Assert.That(service.ResolveMeterDelay(), Is.EqualTo(TenantUsageMeteringService.MaxMeterDelay));

        options.MeterInterval = TenantUsageMeteringService.MaxMeterDelay;
        Assert.That(service.ResolveMeterDelay(), Is.EqualTo(TenantUsageMeteringService.MaxMeterDelay));
    }

    [Test]
    public void ResolveMeterDelay_keeps_an_in_range_interval_and_disables_on_a_non_positive_one()
    {
        var (service, options) = CreateOnClock(CountingRegistry(() => { }), new ManualTimeProvider(), TimeSpan.FromSeconds(30));

        Assert.That(service.ResolveMeterDelay(), Is.EqualTo(TimeSpan.FromSeconds(30)));

        options.MeterInterval = TimeSpan.Zero;
        Assert.That(service.ResolveMeterDelay(), Is.Null);

        options.MeterInterval = TimeSpan.FromSeconds(-5);
        Assert.That(service.ResolveMeterDelay(), Is.Null);

        options.MeterInterval = Timeout.InfiniteTimeSpan;
        Assert.That(service.ResolveMeterDelay(), Is.Null);
    }

    [Test]
    public void TenantUsageMeteringService_MaxMeterDelay_is_the_timer_ceiling()
    {
        Assert.That(
            TenantUsageMeteringService.MaxMeterDelay,
            Is.EqualTo(TimeSpan.FromMilliseconds(uint.MaxValue - 1)));
    }

    /// <summary>
    /// The regression. A sixty-day cadence used to fault the loop synchronously on
    /// its first delay, so no tenant was ever metered and quota admission stayed in
    /// its fail-open state. It must now wait the clamped ceiling and then meter.
    /// </summary>
    [Test]
    public async Task A_meter_interval_above_the_timer_ceiling_still_meters_at_the_ceiling()
    {
        var firstCycle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new ManualTimeProvider();
        var (service, _) = CreateOnClock(CountingRegistry(() => firstCycle.TrySetResult()), clock, TimeSpan.FromDays(60));

        await service.StartAsync(CancellationToken.None);
        try
        {
            Assert.That(service.Loop, Is.Not.Null);
            Assert.That(service.Loop!.IsFaulted, Is.False, "an out-of-range cadence must not fault the loop");

            clock.Advance(TenantUsageMeteringService.MaxMeterDelay - TimeSpan.FromMilliseconds(1));
            Assert.That(firstCycle.Task.IsCompleted, Is.False, "the first cycle waits the whole clamped delay");

            clock.Advance(TimeSpan.FromMilliseconds(1));
            await firstCycle.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.Multiple(() =>
        {
            Assert.That(service.Loop!.IsCompleted, Is.True);
            Assert.That(service.Loop!.IsFaulted, Is.False, "stopping must not surface a delay fault");
        });
    }

    /// <summary>
    /// A reload to zero means "metering disabled", as it does at start-up. It used
    /// to become <c>Task.Delay(TimeSpan.Zero)</c>, which completes immediately, so
    /// the loop re-metered every tenant back to back without ever pausing.
    /// </summary>
    [Test]
    public async Task A_reload_to_a_zero_interval_stops_the_running_loop()
    {
        var cycles = 0;
        var firstCycle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new ManualTimeProvider();
        var (service, options) = CreateOnClock(
            CountingRegistry(() =>
            {
                Interlocked.Increment(ref cycles);
                firstCycle.TrySetResult();
            }),
            clock,
            TimeSpan.FromSeconds(30));

        await service.StartAsync(CancellationToken.None);
        try
        {
            options.MeterInterval = TimeSpan.Zero;
            clock.Advance(TimeSpan.FromSeconds(30));

            await firstCycle.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await service.Loop!.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.Multiple(() =>
        {
            Assert.That(service.Loop!.IsFaulted, Is.False);
            Assert.That(Volatile.Read(ref cycles), Is.EqualTo(1), "the cycle already due runs once, then the loop stops");
        });
    }

    /// <summary>A reload to a negative interval also disables, rather than throwing out of the delay.</summary>
    [Test]
    public async Task A_reload_to_a_negative_interval_stops_the_running_loop_without_faulting()
    {
        var firstCycle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new ManualTimeProvider();
        var (service, options) = CreateOnClock(CountingRegistry(() => firstCycle.TrySetResult()), clock, TimeSpan.FromSeconds(30));

        await service.StartAsync(CancellationToken.None);
        try
        {
            options.MeterInterval = TimeSpan.FromSeconds(-5);
            clock.Advance(TimeSpan.FromSeconds(30));

            await firstCycle.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await service.Loop!.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.That(service.Loop!.IsFaulted, Is.False);
    }
}
