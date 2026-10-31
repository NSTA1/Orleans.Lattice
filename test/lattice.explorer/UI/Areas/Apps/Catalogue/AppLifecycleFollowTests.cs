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

    [TestCase(AppLifecycleState.Failed, false)]
    [TestCase(AppLifecycleState.Disabled, true)]
    public async Task At_the_limit_a_failed_app_settles_and_any_other_state_stays_pending(AppLifecycleState listed, bool pending)
    {
        var time = new ManualTimeProvider();
        var control = new FakeAppsControl();
        control.Listed.Add(Summary(listed));

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
            Assert.That(result.Pending, Is.EqualTo(pending));
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
