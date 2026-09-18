using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Testing;

/// <summary>
/// Unit coverage for <see cref="ManualTimeProvider"/>, the shared hand-driven
/// clock that replaces the private per-fixture copies.
/// <para>
/// The behaviour that matters is the part the private copies left out. Almost
/// all of them override <see cref="TimeProvider.GetUtcNow"/> alone, which is
/// enough for a fixture that only reads the time and silently useless for one
/// that waits on it: <see cref="CancellationTokenSource"/> and
/// <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/> schedule
/// through <see cref="TimeProvider.CreateTimer"/>, and
/// <see cref="TimeProvider.GetElapsedTime(long)"/> reads
/// <see cref="TimeProvider.GetTimestamp"/>. A clock-only fake therefore leaves
/// the first two never firing - so the fixture HANGS rather than failing - and
/// leaves the third measuring the machine's real elapsed time, so the test
/// passes or fails on how fast the agent ran.
/// </para>
/// <para>
/// The tests below pin those three couplings first, because they are the ones a
/// reimplementation would omit and none of them are visible in a test that only
/// asserts a returned timestamp.
/// </para>
/// </summary>
[TestFixture]
public sealed class ManualTimeProviderTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Test]
    public async Task A_clock_that_is_not_advanced_does_not_move()
    {
        var clock = new ManualTimeProvider(Start);

        // Real time genuinely passes here, which is the whole assertion: the
        // provider must be driven by Advance and by nothing else.
        await Task.Delay(TimeSpan.FromMilliseconds(30), TestContext.CurrentContext.CancellationToken);

        Assert.That(clock.GetUtcNow(), Is.EqualTo(Start),
            "A hand-driven clock that drifts with the wall clock reintroduces exactly the timing "
            + "sensitivity it exists to remove.");
    }

    [Test]
    public void Advancing_moves_the_clock_by_exactly_the_delta()
    {
        var clock = new ManualTimeProvider(Start);

        clock.Advance(TimeSpan.FromMinutes(90));

        Assert.That(clock.GetUtcNow(), Is.EqualTo(Start + TimeSpan.FromMinutes(90)));
    }

    [Test]
    public void Advancing_backwards_is_refused()
    {
        var clock = new ManualTimeProvider(Start);

        Assert.That(
            () => clock.Advance(TimeSpan.FromSeconds(-1)),
            Throws.InstanceOf<ArgumentOutOfRangeException>(),
            "Time running backwards is never what a test means, and accepting it silently would let a "
            + "fixture assert against a state no deployment can reach.");
    }

    [Test]
    public void Elapsed_time_is_measured_against_this_clock_and_not_the_machine()
    {
        var clock = new ManualTimeProvider(Start);
        var stamp = clock.GetTimestamp();

        clock.Advance(TimeSpan.FromHours(3));

        Assert.That(clock.GetElapsedTime(stamp), Is.EqualTo(TimeSpan.FromHours(3)),
            "GetElapsedTime reads GetTimestamp, which defaults to Stopwatch. Leaving that default in "
            + "place makes an allocation-free wall-clock budget - the preferred shape - entirely "
            + "unaffected by Advance, so its test would pass or fail on real elapsed time.");
    }

    [Test]
    public void The_timestamp_frequency_matches_the_units_the_timestamp_is_reported_in()
    {
        var clock = new ManualTimeProvider(Start);

        Assert.That(clock.TimestampFrequency, Is.EqualTo(TimeSpan.TicksPerSecond),
            "A frequency that disagreed with GetTimestamp's units would make GetElapsedTime scale every "
            + "reading by a constant factor, which is a wrong answer rather than a missing one.");
    }

    [Test]
    public void The_local_time_zone_is_utc()
    {
        var clock = new ManualTimeProvider(Start);

        Assert.That(clock.LocalTimeZone, Is.EqualTo(TimeZoneInfo.Utc),
            "Inheriting the machine's zone would make any local-time assertion read differently on an "
            + "agent in another region.");
    }

    [Test]
    public void The_clock_starts_where_it_was_told_to()
    {
        var moment = new DateTimeOffset(1999, 12, 31, 23, 59, 59, TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(new ManualTimeProvider(moment).GetUtcNow(), Is.EqualTo(moment));
            Assert.That(new ManualTimeProvider().GetUtcNow(), Is.EqualTo(Start),
                "The default start is fixed rather than 'now', so an unparameterised clock is as "
                + "deterministic as a parameterised one.");
        });
    }

    [Test]
    public void A_cancellation_token_source_expires_only_when_the_clock_is_advanced()
    {
        var clock = new ManualTimeProvider(Start);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5), clock);

        clock.Advance(TimeSpan.FromSeconds(4));
        Assert.That(cts.IsCancellationRequested, Is.False,
            "Asserted before the expiry so the reading after it is known to have been caused by the "
            + "advance rather than by the source having fired immediately.");

        clock.Advance(TimeSpan.FromSeconds(1));

        Assert.That(cts.IsCancellationRequested, Is.True,
            "This is the capability the private copies lack. A CancellationTokenSource schedules its "
            + "expiry through CreateTimer, so against a clock-only fake it never fires and the waiting "
            + "test hangs instead of failing.");
    }

    [Test]
    public async Task A_delay_scheduled_on_this_clock_completes_when_the_clock_is_advanced()
    {
        var clock = new ManualTimeProvider(Start);
        var delay = Task.Delay(
            TimeSpan.FromSeconds(30), clock, TestContext.CurrentContext.CancellationToken);

        Assert.That(delay.IsCompleted, Is.False);

        clock.Advance(TimeSpan.FromSeconds(30));
        await delay;

        Assert.That(delay.IsCompletedSuccessfully, Is.True);
    }

    [Test]
    public void A_periodic_timer_fires_once_per_elapsed_period()
    {
        var clock = new ManualTimeProvider(Start);
        var fired = 0;
        using var timer = clock.CreateTimer(
            _ => Interlocked.Increment(ref fired),
            state: null,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1));

        clock.Advance(TimeSpan.FromSeconds(1));
        clock.Advance(TimeSpan.FromSeconds(1));

        Assert.That(Volatile.Read(ref fired), Is.EqualTo(2),
            "A periodic timer that rearmed only once would make any cadence assertion stop after the "
            + "first tick, which reads as the code under test having stopped working.");
    }

    [Test]
    public void A_timer_with_an_infinite_due_time_never_fires()
    {
        var clock = new ManualTimeProvider(Start);
        var fired = 0;
        using var timer = clock.CreateTimer(
            _ => Interlocked.Increment(ref fired),
            state: null,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromDays(365));

        Assert.That(Volatile.Read(ref fired), Is.Zero,
            "An infinite due time means disarmed. Firing it would make every disarmed timer in a "
            + "fixture go off the moment the clock moved.");
    }

    [Test]
    public void A_rearmed_timer_fires_at_its_new_due_time()
    {
        var clock = new ManualTimeProvider(Start);
        var fired = 0;
        using var timer = clock.CreateTimer(
            _ => Interlocked.Increment(ref fired),
            state: null,
            TimeSpan.FromSeconds(10),
            Timeout.InfiniteTimeSpan);

        timer.Change(TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
        clock.Advance(TimeSpan.FromSeconds(1));

        Assert.That(Volatile.Read(ref fired), Is.EqualTo(1),
            "Change must re-base the due time on the current instant. Retaining the original would "
            + "make a shortened deadline fire late, which is the failure a budget test is trying to "
            + "detect in production code.");
    }

    [Test]
    public void A_disposed_timer_does_not_fire()
    {
        var clock = new ManualTimeProvider(Start);
        var fired = 0;
        var timer = clock.CreateTimer(
            _ => Interlocked.Increment(ref fired),
            state: null,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1));

        timer.Dispose();
        clock.Advance(TimeSpan.FromSeconds(10));

        Assert.That(Volatile.Read(ref fired), Is.Zero,
            "A disposed timer that still fires would run a callback over state the fixture has already "
            + "torn down.");
    }

    [Test]
    public async Task A_timer_disposed_asynchronously_does_not_fire()
    {
        var clock = new ManualTimeProvider(Start);
        var fired = 0;
        var timer = clock.CreateTimer(
            _ => Interlocked.Increment(ref fired),
            state: null,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1));

        await timer.DisposeAsync();
        clock.Advance(TimeSpan.FromSeconds(10));

        Assert.That(Volatile.Read(ref fired), Is.Zero);
    }

    [Test]
    public void The_callback_receives_the_state_it_was_created_with()
    {
        var clock = new ManualTimeProvider(Start);
        object? observed = null;
        var expected = new object();
        using var timer = clock.CreateTimer(
            s => observed = s, expected, TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(1));

        Assert.That(observed, Is.SameAs(expected));
    }

    [Test]
    public void A_timer_cannot_be_created_without_a_callback()
    {
        var clock = new ManualTimeProvider(Start);

        Assert.That(
            () => clock.CreateTimer(null!, state: null, TimeSpan.Zero, Timeout.InfiniteTimeSpan),
            Throws.ArgumentNullException);
    }

    [Test]
    public void A_callback_that_disposes_its_own_timer_does_not_deadlock()
    {
        var clock = new ManualTimeProvider(Start);
        ITimer? timer = null;
        var fired = 0;
        timer = clock.CreateTimer(
            _ =>
            {
                Interlocked.Increment(ref fired);
                timer!.Dispose();
            },
            state: null,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1));

        // Disposing from inside the callback re-enters the provider's own lock,
        // which is the shape that deadlocks if callbacks are invoked while it is
        // held. A regression here hangs the whole run rather than failing one test.
        clock.Advance(TimeSpan.FromSeconds(1));
        clock.Advance(TimeSpan.FromSeconds(1));

        Assert.That(Volatile.Read(ref fired), Is.EqualTo(1));
    }
}
