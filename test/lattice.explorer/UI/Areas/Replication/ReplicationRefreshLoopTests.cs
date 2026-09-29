using Orleans.Lattice.Explorer.UI.Areas.Replication;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Replication;

/// <summary>
/// The refresh cadence: it ticks only while the page is visible, stops the moment
/// the page is hidden, refreshes at once on return, never overlaps a refresh still
/// in flight, and stops for good on dispose. Driven entirely by a manual clock.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ReplicationRefreshLoopTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);

    private readonly ManualTimeProvider _time = new();
    private readonly FakePageVisibility _visibility = new();
    private int _refreshes;

    [Test]
    public async Task It_ticks_on_the_interval_while_the_page_is_visible()
    {
        using var loop = Create();
        await loop.StartAsync();

        _time.Advance(Interval);
        _time.Advance(Interval);

        Assert.Multiple(() =>
        {
            Assert.That(loop.IsRunning, Is.True);
            Assert.That(_visibility.Starts, Is.EqualTo(1));
            Assert.That(_refreshes, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Hiding_the_page_stops_the_cadence_and_showing_it_refreshes_at_once_and_resumes()
    {
        using var loop = Create();
        await loop.StartAsync();
        _time.Advance(Interval);

        _visibility.Set(false);
        _time.Advance(Interval * 10);
        var whileHidden = _refreshes;
        var armedWhileHidden = _time.ArmedTimers;

        _visibility.Set(true);
        var onReturn = _refreshes;
        _time.Advance(Interval);

        Assert.Multiple(() =>
        {
            Assert.That(whileHidden, Is.EqualTo(1), "no refresh while hidden");
            Assert.That(armedWhileHidden, Is.Zero, "the timer is disarmed, not merely ignored");
            Assert.That(onReturn, Is.EqualTo(2), "the page refreshes the moment it is shown again");
            Assert.That(_refreshes, Is.EqualTo(3));
            Assert.That(loop.IsRunning, Is.True);
        });
    }

    [Test]
    public async Task A_page_hidden_when_the_cadence_starts_does_not_tick()
    {
        _visibility.Set(false);
        using var loop = Create();
        await loop.StartAsync();

        _time.Advance(Interval * 3);

        Assert.Multiple(() =>
        {
            Assert.That(loop.IsRunning, Is.False);
            Assert.That(_refreshes, Is.Zero);
        });
    }

    [Test]
    public async Task A_tick_during_a_refresh_in_flight_is_skipped()
    {
        var gate = new TaskCompletionSource();
        var started = 0;
        using var loop = new ReplicationRefreshLoop(_time, _visibility, Interval, work => work(), async () =>
        {
            started++;
            await gate.Task.ConfigureAwait(false);
        });
        await loop.StartAsync();

        _time.Advance(Interval);
        _time.Advance(Interval);
        await Task.Run(gate.SetResult);
        _time.Advance(Interval);

        Assert.That(started, Is.EqualTo(2), "the second tick was skipped, the third ran");
    }

    [Test]
    public async Task Dispose_stops_the_cadence_and_unsubscribes()
    {
        var loop = Create();
        await loop.StartAsync();

        loop.Dispose();
        loop.Dispose();
        _time.Advance(Interval * 3);
        _visibility.Set(false);
        _visibility.Set(true);

        Assert.Multiple(() =>
        {
            Assert.That(_refreshes, Is.Zero);
            Assert.That(loop.IsRunning, Is.False);
            Assert.That(_visibility.Subscribers, Is.Zero);
            Assert.That(async () => await loop.StartAsync(), Throws.InstanceOf<ObjectDisposedException>());
        });
    }

    [Test]
    public async Task Starting_twice_starts_once_and_a_refresh_that_faults_on_teardown_is_absorbed()
    {
        using var loop = new ReplicationRefreshLoop(_time, _visibility, Interval, _ => throw new ObjectDisposedException("renderer"), () => Task.CompletedTask);
        await loop.StartAsync();
        await loop.StartAsync();

        Assert.Multiple(() =>
        {
            Assert.That(() => _time.Advance(Interval), Throws.Nothing);
            Assert.That(_visibility.Starts, Is.EqualTo(1));
        });
    }

    [Test]
    public void It_rejects_bad_arguments()
    {
        Func<Func<Task>, Task> dispatch = work => work();
        Func<Task> refresh = () => Task.CompletedTask;

        Assert.Multiple(() =>
        {
            Assert.That(() => new ReplicationRefreshLoop(null!, _visibility, Interval, dispatch, refresh), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationRefreshLoop(_time, null!, Interval, dispatch, refresh), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationRefreshLoop(_time, _visibility, Interval, null!, refresh), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationRefreshLoop(_time, _visibility, Interval, dispatch, null!), Throws.ArgumentNullException);
            Assert.That(() => new ReplicationRefreshLoop(_time, _visibility, TimeSpan.Zero, dispatch, refresh), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    private ReplicationRefreshLoop Create() =>
        new(_time, _visibility, Interval, work => work(), () =>
        {
            _refreshes++;
            return Task.CompletedTask;
        });
}
