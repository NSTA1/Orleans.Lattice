using NSubstitute;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Pins that <see cref="RepoContextSelfIndexGrain"/> arms its scan timer with a due
/// time and period the Orleans runtime accepts, whatever tick interval is configured.
/// <para>
/// Orleans' grain timer rejects a due time or period above <c>0xFFFFFFFE</c>
/// milliseconds (about 49.7 days) with an <see cref="ArgumentOutOfRangeException"/>.
/// The grain used to pass <c>LATTICE_SELFINDEX_TICK_SECONDS</c> straight through as
/// the period and add up to another interval of jitter to the due time, so a tick
/// above the ceiling failed every onboarding and keep-alive re-arm, and one just
/// under it failed whenever the jitter pushed the first tick past the ceiling. The
/// harness substitutes the timer registry, so these tests assert the arguments
/// against the runtime's own bound rather than letting a real timer throw.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextSelfIndexGrainTimerCeilingTests
{
    /// <summary>The largest due time or period, in milliseconds, Orleans' grain timer accepts.</summary>
    private const long MaxSupportedTimeoutMs = 0xFFFFFFFE;

    private const string KeepaliveReminderName = "repo-context-self-index-keepalive";

    [TestCase(60)]
    [TestCase(36_500)]
    public async Task EnsureRunningAsync_with_a_tick_interval_beyond_the_timer_ceiling_arms_a_timer_the_runtime_accepts(int tickDays)
    {
        var harness = new SelfIndexGrainHarness(
            options: new RepoContextIndexingOptions { TickInterval = TimeSpan.FromDays(tickDays) });

        await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());

        var armed = ArmedOptions(harness);
        Assert.Multiple(() =>
        {
            AssertAccepted(armed.DueTime, "due time");
            AssertAccepted(armed.Period, "period");
            Assert.That(armed.Period, Is.EqualTo(TimeSpan.FromMilliseconds(MaxSupportedTimeoutMs)),
                "A tick longer than the timer can wait runs at the longest period it can express.");
        });
    }

    [Test]
    public async Task Keepalive_rearm_with_a_tick_interval_just_under_the_ceiling_never_jitters_the_due_time_past_it()
    {
        var tick = TimeSpan.FromMilliseconds(MaxSupportedTimeoutMs) - TimeSpan.FromSeconds(1);

        // The first-tick jitter is random, so draw it repeatedly: before the fix all
        // but a one-second sliver of the jitter range overshot the ceiling.
        for (var draw = 0; draw < 32; draw++)
        {
            var harness = new SelfIndexGrainHarness(options: new RepoContextIndexingOptions { TickInterval = tick });

            await harness.CreateGrain().ReceiveReminder(KeepaliveReminderName, new TickStatus());

            var armed = ArmedOptions(harness);
            Assert.Multiple(() =>
            {
                AssertAccepted(armed.DueTime, $"due time (draw {draw})");
                Assert.That(armed.Period, Is.EqualTo(tick), "A period under the ceiling is kept as configured.");
            });
        }
    }

    [Test]
    public async Task EnsureRunningAsync_with_the_default_tick_interval_keeps_its_period_and_jitters_within_one_interval()
    {
        var harness = new SelfIndexGrainHarness();

        await harness.CreateGrain().EnsureRunningAsync(SelfIndexGrainHarness.Request());

        var armed = ArmedOptions(harness);
        Assert.Multiple(() =>
        {
            Assert.That(armed.Period, Is.EqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(armed.DueTime, Is.GreaterThanOrEqualTo(TimeSpan.FromSeconds(15)));
            Assert.That(armed.DueTime, Is.LessThan(TimeSpan.FromSeconds(30)));
        });
    }

    [Test]
    public void MaxTimerDuration_is_the_runtime_timer_ceiling()
    {
        Assert.That(RepoContextSelfIndexGrain.MaxTimerDuration,
            Is.EqualTo(TimeSpan.FromMilliseconds(MaxSupportedTimeoutMs)));
    }

    private static GrainTimerCreationOptions ArmedOptions(SelfIndexGrainHarness harness)
    {
        var registration = harness.TimerRegistry.ReceivedCalls()
            .Single(static call => call.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return registration.GetArguments().OfType<GrainTimerCreationOptions>().Single();
    }

    /// <summary>Mirrors the argument validation Orleans' grain timer applies.</summary>
    private static void AssertAccepted(TimeSpan value, string what)
    {
        var milliseconds = (long)value.TotalMilliseconds;
        Assert.That(milliseconds, Is.InRange(-1L, MaxSupportedTimeoutMs),
            $"The scan timer's {what} ({value}) is outside the range a grain timer accepts, "
            + "so arming it throws ArgumentOutOfRangeException.");
    }
}
