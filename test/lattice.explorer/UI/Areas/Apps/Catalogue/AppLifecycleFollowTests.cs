using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>Following a lifecycle change the cluster accepted but had not finished (issue #5482).</summary>
[TestFixture]
public sealed class AppLifecycleFollowTests
{
    private const string Slug = "task-board";

    private static readonly AppLifecycleResult Accepted = new() { Slug = Slug, Version = "1.0.0", State = AppLifecycleState.Disabled, Pending = true };

    [Test]
    public async Task A_change_that_reaches_its_target_settles_as_changed()
    {
        var time = new ManualTimeProvider();
        var control = new FakeAppsControl();
        control.Listed.Add(Summary(AppLifecycleState.Disabled));

        var follow = AppLifecycleFollow.FollowAsync(control, Accepted, AppLifecycleState.Uninstalled, time, CancellationToken.None);
        await StepAsync(time, follow);
        Assert.That(follow.IsCompleted, Is.False, "the app has not moved yet");

        control.Settle(Slug, AppLifecycleState.Uninstalled);
        await StepAsync(time, follow);
        var result = await follow;

        Assert.Multiple(() =>
        {
            Assert.That(result.Pending, Is.False);
            Assert.That(result.Changed, Is.True);
            Assert.That(result.State, Is.EqualTo(AppLifecycleState.Uninstalled));
        });
    }

    [Test]
    public async Task An_app_no_longer_listed_counts_as_uninstalled()
    {
        var time = new ManualTimeProvider();
        var follow = AppLifecycleFollow.FollowAsync(new FakeAppsControl(), Accepted, AppLifecycleState.Uninstalled, time, CancellationToken.None);

        await StepAsync(time, follow);

        Assert.That((await follow).State, Is.EqualTo(AppLifecycleState.Uninstalled));
    }

    [Test]
    public async Task A_listed_failed_app_is_final_at_once()
    {
        var time = new ManualTimeProvider();
        var control = new FakeAppsControl();
        control.Listed.Add(Summary(AppLifecycleState.Failed));

        var follow = AppLifecycleFollow.FollowAsync(control, Accepted, AppLifecycleState.Uninstalled, time, CancellationToken.None);
        await StepAsync(time, follow);

        Assert.That(follow.IsCompleted, Is.True, "a listed Failed state is never interim, so it ends the follow on the first read");
        var result = await follow;
        Assert.Multiple(() =>
        {
            Assert.That(result.Pending, Is.False);
            Assert.That(result.Changed, Is.False);
            Assert.That(result.State, Is.EqualTo(AppLifecycleState.Failed));
        });
    }

    [Test]
    public async Task An_app_listed_with_a_run_in_flight_is_followed_until_the_run_settles()
    {
        var time = new ManualTimeProvider();
        var control = new FakeAppsControl();
        control.Listed.Add(Summary(AppLifecycleState.Disabled) with { PendingState = AppLifecycleState.Uninstalled });

        var follow = AppLifecycleFollow.FollowAsync(control, Accepted, AppLifecycleState.Uninstalled, time, CancellationToken.None);
        await StepAsync(time, follow);
        await StepAsync(time, follow);
        Assert.That(follow.IsCompleted, Is.False, "the run is still in flight");

        control.Settle(Slug, AppLifecycleState.Uninstalled);
        await StepAsync(time, follow);

        Assert.That((await follow).State, Is.EqualTo(AppLifecycleState.Uninstalled));
    }

    [TestCase(AppLifecycleState.Disabled, null)]
    [TestCase(AppLifecycleState.Disabled, AppLifecycleState.Uninstalled)]
    public async Task At_the_limit_an_app_that_has_not_reached_its_target_stays_pending(AppLifecycleState listed, AppLifecycleState? pendingState)
    {
        var time = new ManualTimeProvider();
        var control = new FakeAppsControl();
        control.Listed.Add(Summary(listed) with { PendingState = pendingState });

        var follow = AppLifecycleFollow.FollowAsync(control, Accepted, AppLifecycleState.Uninstalled, time, CancellationToken.None);
        var bound = (int)(AppLifecycleFollow.Limit / AppLifecycleFollow.PollInterval) + 2;
        for (var i = 0; i < bound && !follow.IsCompleted; i++)
        {
            await StepAsync(time, follow);
        }

        Assert.That(follow.IsCompleted, Is.True, "following stops at the limit");
        var result = await follow;
        Assert.Multiple(() =>
        {
            Assert.That(result.Pending, Is.True);
            Assert.That(result.State, Is.EqualTo(listed));
        });
    }

    [Test]
    public void Cancelling_stops_following()
    {
        var time = new ManualTimeProvider();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(() =>
            AppLifecycleFollow.FollowAsync(new FakeAppsControl(), Accepted, AppLifecycleState.Uninstalled, time, cts.Token));
    }

    /// <summary>Waits until the follow is waiting on the clock, then moves the clock one poll on.</summary>
    private static async Task StepAsync(ManualTimeProvider time, Task follow)
    {
        await TestPoll.UntilAsync(() => time.PendingTimerCount > 0 || follow.IsCompleted, "the follow to wait for its next read");
        time.Advance(AppLifecycleFollow.PollInterval);
        await TestPoll.UntilAsync(() => time.PendingTimerCount > 0 || follow.IsCompleted, "the follow to read the catalogue");
    }

    private static AppSummary Summary(AppLifecycleState state) =>
        new() { Slug = Slug, Version = "1.0.0", State = state, Provenance = new AppProvenanceDescriptor { Source = "in-image", Publisher = "p", Reference = "r" } };
}
