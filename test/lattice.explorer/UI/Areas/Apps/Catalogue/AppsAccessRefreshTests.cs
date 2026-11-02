using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// Issue #5517: the Apps area's memoized probe is trusted for the capability refresh
/// interval only. A read after it is answered at once from the snapshot held and
/// re-reads once in the background; a different answer replaces the snapshot and is
/// announced, the same answer or no answer is not, and a re-read overtaken by a new
/// caller is discarded.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AppsAccessRefreshTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly ManualTimeProvider _time = new();
    private readonly FakeActiveTenantProvider _tenant = new("acme");
    private readonly ILatticeAppWorkspace _workspace = Substitute.For<ILatticeAppWorkspace>();
    private readonly Queue<Func<Task<ImmutableArray<WorkspaceAppSummary>>>> _answers = new();
    private ImmutableArray<WorkspaceAppSummary> _apps = [];

    [SetUp]
    public void SetUp()
    {
        _workspace.ListMyAppsAsync(Arg.Any<CancellationToken>())
            .Returns(_ => _answers.TryDequeue(out var answer) ? answer() : Task.FromResult(_apps));
    }

    [Test]
    public void The_refresh_interval_defaults_to_ten_seconds() =>
        Assert.That(new ExplorerChromeOptions().CapabilityRefreshInterval, Is.EqualTo(TimeSpan.FromSeconds(10)));

    [Test]
    public async Task Within_the_interval_every_read_shares_one_probe()
    {
        var access = Create();

        var first = await access.GetAsync();
        _time.Advance(Interval - TimeSpan.FromSeconds(1));
        var second = await access.GetAsync();
        await access.Refreshing;

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first));
            Assert.That(_workspace.ReceivedCalls().Count(), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Past_the_interval_a_read_is_answered_at_once_and_re_reads_once_in_the_background()
    {
        var access = Create();
        var first = await access.GetAsync();

        var reread = new TaskCompletionSource<ImmutableArray<WorkspaceAppSummary>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _answers.Enqueue(() => reread.Task);
        _time.Advance(Interval);

        var stale = access.GetAsync();
        var alsoStale = access.GetAsync();
        var held = await stale;
        var refreshing = access.Refreshing;

        Assert.Multiple(() =>
        {
            Assert.That(stale.IsCompletedSuccessfully, Is.True, "a read never waits for the re-read");
            Assert.That(held, Is.SameAs(first));
            Assert.That(alsoStale.Result, Is.SameAs(first));
            Assert.That(refreshing.IsCompleted, Is.False);
        });

        reread.SetResult([Board()]);
        await refreshing;

        Assert.That(_workspace.ReceivedCalls().Count(), Is.EqualTo(2), "two reads in the same interval re-read once");
    }

    [Test]
    public async Task A_re_read_that_answers_differently_replaces_the_snapshot_and_is_announced()
    {
        var access = Create();
        var changed = 0;
        access.Changed += () => changed++;
        await access.GetAsync();

        _apps = [Board()];
        _time.Advance(Interval);
        await access.GetAsync();
        await access.Refreshing;

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.EqualTo(1));
            Assert.That(access.Current!.MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "task-board" }));
        });
        Assert.That((await access.GetAsync()).MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "task-board" }));
    }

    [Test]
    public async Task A_re_read_that_answers_the_same_is_not_announced_and_is_next_asked_an_interval_later()
    {
        var access = Create();
        var changed = 0;
        access.Changed += () => changed++;
        var first = await access.GetAsync();

        _time.Advance(Interval);
        await access.GetAsync();
        await access.Refreshing;
        _time.Advance(Interval - TimeSpan.FromSeconds(1));
        var later = await access.GetAsync();
        await access.Refreshing;

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.Zero);
            Assert.That(later, Is.SameAs(first));
            Assert.That(_workspace.ReceivedCalls().Count(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_re_read_the_cluster_does_not_answer_keeps_the_snapshot_held()
    {
        _apps = [Board()];
        var access = Create();
        var changed = 0;
        access.Changed += () => changed++;
        var first = await access.GetAsync();

        _answers.Enqueue(() => Task.FromException<ImmutableArray<WorkspaceAppSummary>>(new TimeoutException()));
        _time.Advance(Interval);
        await access.GetAsync();
        await access.Refreshing;

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.Zero);
            Assert.That(access.Current, Is.SameAs(first), "an unanswered re-read never empties what the caller sees");
        });
    }

    [Test]
    public async Task A_re_read_overtaken_by_a_tenant_switch_is_discarded()
    {
        var access = Create();
        var changed = 0;
        access.Changed += () => changed++;
        await access.GetAsync();

        var reread = new TaskCompletionSource<ImmutableArray<WorkspaceAppSummary>>(TaskCreationOptions.RunContinuationsAsynchronously);
        _answers.Enqueue(() => reread.Task);
        _time.Advance(Interval);
        await access.GetAsync();
        var refreshing = access.Refreshing;

        _tenant.Set("globex");
        reread.SetResult([Board()]);
        await refreshing;

        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.Zero);
            Assert.That(access.Current, Is.Null, "acme's re-read is never globex's answer");
        });
    }

    [Test]
    public async Task A_changed_answer_tells_the_area_directory()
    {
        var directory = new ExplorerAreaDirectory([], new ExplorerChromeOptions(), _time);
        var told = 0;
        directory.Changed += () => told++;
        var services = new ServiceCollection()
            .AddSingleton<ILatticeActiveTenantProvider>(_tenant)
            .AddKeyedSingleton(ShellFacades.Key, _workspace)
            .AddSingleton(directory)
            .BuildServiceProvider();
        var access = new AppsAccess(new AppsFacades(services), time: _time, options: new ExplorerChromeOptions { CapabilityRefreshInterval = Interval }, services: services);
        await access.GetAsync();

        _apps = [Board()];
        _time.Advance(Interval);
        await access.GetAsync();
        await access.Refreshing;

        Assert.That(told, Is.EqualTo(1));
    }

    private AppsAccess Create()
    {
        var services = new ServiceCollection()
            .AddSingleton<ILatticeActiveTenantProvider>(_tenant)
            .AddKeyedSingleton(ShellFacades.Key, _workspace)
            .BuildServiceProvider();
        return new AppsAccess(new AppsFacades(services), time: _time, options: new ExplorerChromeOptions { CapabilityRefreshInterval = Interval });
    }

    private static WorkspaceAppSummary Board() =>
        new() { Slug = "task-board", Version = "1.1.0", Roles = ["viewer"] };
}
