using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.Testing;

public sealed partial class ManualTimeProviderTests
{
    [Test]
    public async Task CreateTimer_observed_registration_is_armed_before_concurrent_advance()
    {
        const int attempts = 20_000;
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.CurrentContext.CancellationToken);
        bound.CancelAfter(TimeSpan.FromSeconds(30));
        using var phases = new Barrier(2);
        var clock = new ManualTimeProvider(Start);
        var missed = new List<int>();
        var fired = 0;

        // Two dedicated participants repeatedly race the actual public seams, without
        // a timing sleep or a hook that could itself make publication atomic.
        var observer = Task.Factory.StartNew(() =>
        {
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                phases.SignalAndWait(bound.Token);
                while (clock.PendingTimerCount == 0)
                {
                    bound.Token.ThrowIfCancellationRequested();
                    Thread.SpinWait(1);
                }

                clock.Advance(TimeSpan.FromSeconds(5));
                phases.SignalAndWait(bound.Token);
            }
        }, bound.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        try
        {
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                clock = new ManualTimeProvider(Start);
                fired = 0;
                phases.SignalAndWait(bound.Token);
                using var timer = clock.CreateTimer(
                    _ => Interlocked.Increment(ref fired), null,
                    TimeSpan.FromSeconds(5), Timeout.InfiniteTimeSpan);
                phases.SignalAndWait(bound.Token);
                if (Volatile.Read(ref fired) != 1)
                {
                    missed.Add(attempt);
                }
            }
        }
        finally
        {
            await bound.CancelAsync();
            await observer;
        }

        Assert.That(missed, Is.Empty,
            $"An observed registration must fire on the first advance; missed attempts: "
            + string.Join(", ", missed.Take(20)));
    }

    [Test]
    public void CreateTimer_initialization_failure_does_not_publish_a_timer()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.MaxValue);

        Assert.That(
            () => clock.CreateTimer(
                _ => { }, null, TimeSpan.FromTicks(1), Timeout.InfiniteTimeSpan),
            Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(clock.PendingTimerCount, Is.Zero);
    }

    [Test]
    public void Change_after_disposal_returns_false_without_registering_again()
    {
        var clock = new ManualTimeProvider(Start);
        var fired = 0;
        var timer = clock.CreateTimer(
            _ => fired++, null, TimeSpan.Zero, TimeSpan.FromSeconds(1));
        timer.Dispose();

        Assert.That(timer.Change(TimeSpan.Zero, TimeSpan.FromSeconds(1)), Is.False);
        clock.Advance(TimeSpan.FromSeconds(1));

        Assert.Multiple(() =>
        {
            Assert.That(fired, Is.Zero);
            Assert.That(clock.PendingTimerCount, Is.Zero);
        });
    }

    [Test]
    public void Change_can_arm_and_disarm_an_initially_infinite_timer()
    {
        var clock = new ManualTimeProvider(Start);
        var fired = 0;
        using var timer = clock.CreateTimer(
            _ => fired++, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        Assert.That(timer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1)), Is.True);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.That(fired, Is.EqualTo(1));

        Assert.That(timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan), Is.True);
        clock.Advance(TimeSpan.FromDays(1));
        Assert.That(fired, Is.EqualTo(1));
    }

    [Test]
    public void A_callback_can_change_itself_and_create_another_timer()
    {
        var clock = new ManualTimeProvider(Start);
        ITimer? first = null;
        ITimer? second = null;
        var fired = 0;
        first = clock.CreateTimer(_ =>
        {
            fired++;
            if (fired == 1)
            {
                first!.Change(TimeSpan.FromSeconds(2), Timeout.InfiniteTimeSpan);
                second = clock.CreateTimer(
                    _ => fired++, null, TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
            }
        }, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        using (first)
        {
            try
            {
                clock.Advance(TimeSpan.FromSeconds(1));
                Assert.That(fired, Is.EqualTo(1));
                clock.Advance(TimeSpan.FromSeconds(1));
                Assert.That(fired, Is.EqualTo(2), "Only the callback-created timer is due.");
                clock.Advance(TimeSpan.FromSeconds(1));
                Assert.That(fired, Is.EqualTo(3), "The callback's Change replaces periodic rearming.");
                clock.Advance(TimeSpan.FromSeconds(10));
                Assert.That(fired, Is.EqualTo(3));
            }
            finally
            {
                second?.Dispose();
            }
        }
    }
}
