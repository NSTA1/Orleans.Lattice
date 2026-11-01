using System.Text;
using Orleans.Runtime;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Overlap-guard coverage for the backup scheduler grain: while a capture for a
/// scope is in flight, a second concurrent trigger for the same scope is skipped
/// (returns <c>null</c>) rather than starting an overlapping capture, and the
/// capture engine is invoked exactly once. A schedule reminder tick returns
/// without waiting for its capture, and ticks that fire while a scheduled cycle
/// is still running coalesce into it rather than queueing captures back to
/// back (issue #5473).
/// </summary>
[Category("Integration")]
public sealed class BackupSchedulerOverlapTests
{
    private const string Tree = "orders";

    private SchedulerClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp() => _fixture = new SchedulerClusterFixture();

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task TriggerFullAsync_skips_a_second_concurrent_capture_for_the_same_scope()
    {
        await _fixture.InitializeAsync(gated: true);
        var scope = BackupScopeSelector.WholeTree(Tree);
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var scheduler = _fixture.Scheduler(scope);

        // Start the first capture and wait until it is parked inside the gate, so
        // the grain's in-flight guard is armed before the second trigger arrives.
        var first = scheduler.TriggerFullAsync(scope);
        await _fixture.Gate.Started.WaitAsync(TimeSpan.FromSeconds(30));

        // The second trigger interleaves while the first is still running.
        var secondResult = await scheduler.TriggerFullAsync(scope);

        _fixture.Gate.Release();
        var firstResult = await first;

        Assert.Multiple(() =>
        {
            Assert.That(firstResult, Is.Not.Null, "the in-flight capture should complete with a backup id");
            Assert.That(secondResult, Is.Null, "the overlapping trigger should be skipped");
            Assert.That(_fixture.Gate.Calls, Is.EqualTo(1), "the capture engine should run exactly once");
        });
    }

    private const string FullScheduleReminder = "backup-schedule-full";
    private const string IncrementalScheduleReminder = "backup-schedule-incremental";

    [Test]
    public async Task A_full_tick_during_an_incremental_cycle_runs_its_full_cycle_once_that_cycle_ends()
    {
        var (scheduler, remindable) = await ArrangeScheduledScopeAsync();
        var scope = BackupScopeSelector.WholeTree(Tree);

        // The scope has no backup yet, so the incremental cycle captures its
        // baseline through the gated capture engine and parks there.
        var incremental = remindable.ReceiveReminder(IncrementalScheduleReminder, default(TickStatus));
        await _fixture.Gate.Started.WaitAsync(TimeSpan.FromSeconds(30));

        // Issue #5531: the daily full tick fires while the incremental cycle is
        // running. Coalescing it into that cycle would drop the full backup.
        var full = remindable.ReceiveReminder(FullScheduleReminder, default(TickStatus));

        _fixture.Gate.Release();
        await Task.WhenAll(incremental, full).WaitAsync(TimeSpan.FromSeconds(60));
        await WaitForAsync(async () => _fixture.Gate.Calls >= 2 && await scheduler.IsIdleAsync());

        // The tree does not change between the two captures, so both re-derive one
        // content-addressed backup id: the captures themselves are what is counted.
        var names = _fixture.Gate.Names.ToList();
        Assert.Multiple(async () =>
        {
            Assert.That(_fixture.Gate.Calls, Is.EqualTo(2), "the full tick should run one full cycle after the incremental one");
            Assert.That(names.Count(n => n.StartsWith("incremental-", StringComparison.Ordinal)), Is.EqualTo(1));
            Assert.That(names.Count(n => n.StartsWith("full-", StringComparison.Ordinal)), Is.EqualTo(1), "the full backup must not be dropped");
            Assert.That(await _fixture.ListScopeAsync(scope), Has.Count.EqualTo(1), "the full cycle republishes the unchanged tree's backup");
        });
    }

    [Test]
    public async Task An_incremental_tick_during_a_full_cycle_is_coalesced_into_it()
    {
        var (scheduler, remindable) = await ArrangeScheduledScopeAsync();

        var full = remindable.ReceiveReminder(FullScheduleReminder, default(TickStatus));
        await _fixture.Gate.Started.WaitAsync(TimeSpan.FromSeconds(30));

        // A full cycle subsumes an incremental one, so this tick starts nothing.
        var incremental = remindable.ReceiveReminder(IncrementalScheduleReminder, default(TickStatus));

        _fixture.Gate.Release();
        await Task.WhenAll(full, incremental).WaitAsync(TimeSpan.FromSeconds(60));
        await WaitUntilIdleAsync(scheduler);

        Assert.Multiple(async () =>
        {
            Assert.That(_fixture.Gate.Calls, Is.EqualTo(1));
            Assert.That(await _fixture.ListScopeAsync(BackupScopeSelector.WholeTree(Tree)), Has.Count.EqualTo(1));
        });
    }

    private static async Task WaitForAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                return;
            }

            await Task.Delay(50);
        }
    }

    [Test]
    public async Task A_schedule_tick_returns_without_waiting_for_its_capture()
    {
        var (scheduler, remindable) = await ArrangeScheduledScopeAsync();

        // Before #5473 the tick awaited the whole capture, so the reminder call
        // was held - and timed out - for as long as the capture took.
        var tick = remindable.ReceiveReminder(FullScheduleReminder, default(TickStatus));
        await _fixture.Gate.Started.WaitAsync(TimeSpan.FromSeconds(30));
        await tick.WaitAsync(TimeSpan.FromSeconds(10));
        var idleWhileGated = await scheduler.IsIdleAsync();

        _fixture.Gate.Release();
        await WaitUntilIdleAsync(scheduler);

        Assert.Multiple(async () =>
        {
            Assert.That(idleWhileGated, Is.False, "the dispatched cycle should still be running");
            Assert.That(_fixture.Gate.Calls, Is.EqualTo(1));
            Assert.That(await _fixture.ListScopeAsync(BackupScopeSelector.WholeTree(Tree)), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Schedule_ticks_that_fire_while_a_cycle_runs_coalesce_into_it()
    {
        var (scheduler, remindable) = await ArrangeScheduledScopeAsync();

        var first = remindable.ReceiveReminder(FullScheduleReminder, default(TickStatus));
        await _fixture.Gate.Started.WaitAsync(TimeSpan.FromSeconds(30));

        // Two more ticks fire while the first cycle's capture is still running.
        // Before #5473 each queued behind the first and then ran a capture of its
        // own, back to back, so a schedule shorter than its captures never caught up.
        var second = remindable.ReceiveReminder(FullScheduleReminder, default(TickStatus));
        var third = remindable.ReceiveReminder(FullScheduleReminder, default(TickStatus));

        _fixture.Gate.Release();
        await Task.WhenAll(first, second, third).WaitAsync(TimeSpan.FromSeconds(60));
        await WaitUntilIdleAsync(scheduler);

        Assert.Multiple(async () =>
        {
            Assert.That(_fixture.Gate.Calls, Is.EqualTo(1), "the overlapping ticks should start no capture of their own");
            Assert.That(await _fixture.ListScopeAsync(BackupScopeSelector.WholeTree(Tree)), Has.Count.EqualTo(1));
        });
    }

    private async Task<(ILatticeBackupSchedulerGrain Scheduler, IRemindable Remindable)> ArrangeScheduledScopeAsync()
    {
        await _fixture.InitializeAsync(gated: true);
        var scope = BackupScopeSelector.WholeTree(Tree);
        await _fixture.GrainFactory.GetGrain<ILattice>(Tree).SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        var scheduler = _fixture.Scheduler(scope);

        // Persists the scope a reminder cycle captures; no schedule is enabled,
        // so the test delivers every tick itself.
        await scheduler.EnsureScheduleAsync(scope);
        return (scheduler, scheduler.AsReference<IRemindable>());
    }

    private static async Task WaitUntilIdleAsync(ILatticeBackupSchedulerGrain scheduler)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (!await scheduler.IsIdleAsync())
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "the scheduled cycle did not finish");
            await Task.Delay(50);
        }
    }
}
