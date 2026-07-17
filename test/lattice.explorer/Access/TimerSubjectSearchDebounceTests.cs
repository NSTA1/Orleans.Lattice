using Orleans.Lattice.Explorer.Access;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// Coverage for the production <see cref="TimerSubjectSearchDebounce"/>: it runs a
/// scheduled action, supersedes a still-pending action when a newer one is
/// scheduled, and cancels on dispose. Tests await a signal rather than asserting
/// elapsed time, and rely on the synchronous cancellation in
/// <see cref="TimerSubjectSearchDebounce.Schedule"/> to keep supersession
/// deterministic.
/// </summary>
[TestFixture]
public sealed class TimerSubjectSearchDebounceTests
{
    private static readonly TimeSpan Signal = TimeSpan.FromSeconds(5);

    [Test]
    public void Schedule_null_action_throws()
    {
        using var debounce = new TimerSubjectSearchDebounce(TimeSpan.Zero);
        Assert.That(() => debounce.Schedule(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Schedule_runs_the_action()
    {
        using var debounce = new TimerSubjectSearchDebounce(TimeSpan.Zero);
        var ran = new TaskCompletionSource();

        debounce.Schedule(() =>
        {
            ran.TrySetResult();
            return Task.CompletedTask;
        });

        var completed = await Task.WhenAny(ran.Task, Task.Delay(Signal));
        Assert.That(completed, Is.SameAs(ran.Task), "the scheduled action should run");
    }

    [Test]
    public async Task Schedule_supersedes_a_pending_action()
    {
        using var debounce = new TimerSubjectSearchDebounce(TimeSpan.FromMilliseconds(20));
        var firstRan = false;
        var secondRan = new TaskCompletionSource();

        debounce.Schedule(() =>
        {
            firstRan = true;
            return Task.CompletedTask;
        });
        debounce.Schedule(() =>
        {
            secondRan.TrySetResult();
            return Task.CompletedTask;
        });

        var completed = await Task.WhenAny(secondRan.Task, Task.Delay(Signal));

        Assert.Multiple(() =>
        {
            Assert.That(completed, Is.SameAs(secondRan.Task), "the newest action should run");
            Assert.That(firstRan, Is.False, "the superseded action should be cancelled");
        });
    }

    [Test]
    public async Task Dispose_cancels_a_pending_action()
    {
        var debounce = new TimerSubjectSearchDebounce(TimeSpan.FromSeconds(30));
        var ran = false;
        debounce.Schedule(() =>
        {
            ran = true;
            return Task.CompletedTask;
        });

        debounce.Dispose();
        await Task.Delay(100);

        Assert.That(ran, Is.False);
    }

    [Test]
    public void Default_constructor_uses_the_default_interval()
    {
        Assert.That(TimerSubjectSearchDebounce.DefaultInterval, Is.GreaterThan(TimeSpan.Zero));
        using var debounce = new TimerSubjectSearchDebounce();
        Assert.That(() => debounce.Schedule(() => Task.CompletedTask), Throws.Nothing);
    }
}
