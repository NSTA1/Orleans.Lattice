using System.Text;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Scheduling coverage for the backup scheduler grain: enabling a schedule
/// registers the matching reminder; a scheduled cycle captures a backup exactly
/// as the reminder would; a disabled schedule registers no reminder; and a
/// previously registered reminder is unregistered when its schedule is turned
/// off.
/// </summary>
[Category("Integration")]
public sealed class BackupSchedulerScheduleTests
{
    private const string Tree = "orders";

    private SchedulerClusterFixture _fixture = null!;

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task EnsureScheduleAsync_registers_the_full_schedule_reminder_when_enabled()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(o =>
        {
            o.FullBackupScheduleEnabled = true;
            o.FullBackupInterval = TimeSpan.FromMinutes(1);
        });
        var scope = BackupScopeSelector.WholeTree(Tree);

        await _fixture.Scheduler(scope).EnsureScheduleAsync(scope);

        var hasFull = await _fixture.Scheduler(scope).HasScheduleAsync(incremental: false);
        var hasIncremental = await _fixture.Scheduler(scope).HasScheduleAsync(incremental: true);
        Assert.Multiple(() =>
        {
            Assert.That(hasFull, Is.True);
            Assert.That(hasIncremental, Is.False);
        });
    }

    [Test]
    public async Task EnsureScheduleAsync_registers_no_reminder_when_disabled()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Tree);

        await _fixture.Scheduler(scope).EnsureScheduleAsync(scope);

        var hasFull = await _fixture.Scheduler(scope).HasScheduleAsync(incremental: false);
        var hasIncremental = await _fixture.Scheduler(scope).HasScheduleAsync(incremental: true);
        Assert.Multiple(() =>
        {
            Assert.That(hasFull, Is.False);
            Assert.That(hasIncremental, Is.False);
        });
    }

    [Test]
    public async Task EnsureScheduleAsync_unregisters_a_reminder_when_the_schedule_is_turned_off()
    {
        // Deploy with the incremental schedule enabled (read from the mutable toggle).
        SchedulerClusterFixture.IncrementalScheduleEnabled = true;
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(
            o => o.IncrementalBackupScheduleEnabled = SchedulerClusterFixture.IncrementalScheduleEnabled);
        var scope = BackupScopeSelector.WholeTree(Tree);
        await _fixture.Scheduler(scope).EnsureScheduleAsync(scope);
        Assert.That(await _fixture.Scheduler(scope).HasScheduleAsync(incremental: true), Is.True);

        // Flip the schedule off and re-resolve options within the same activation:
        // EnsureSchedule must now unregister the previously registered reminder.
        SchedulerClusterFixture.IncrementalScheduleEnabled = false;
        _fixture.ClearOptionsCache();
        await _fixture.Scheduler(scope).EnsureScheduleAsync(scope);

        Assert.That(await _fixture.Scheduler(scope).HasScheduleAsync(incremental: true), Is.False);
    }

    [Test]
    public async Task RunScheduledCycleAsync_captures_a_backup_for_the_scheduled_scope()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(o => o.FullBackupScheduleEnabled = true);
        var scope = BackupScopeSelector.WholeTree(Tree);
        await SeedAsync("k1", "v1");
        // The scope must be known to the grain (persisted) before a cycle can run.
        await _fixture.Scheduler(scope).EnsureScheduleAsync(scope);

        var backupId = await _fixture.Scheduler(scope).RunScheduledCycleAsync(incremental: false);

        var manifests = await _fixture.ListScopeAsync(scope);
        Assert.Multiple(() =>
        {
            Assert.That(backupId, Is.Not.Null);
            Assert.That(manifests, Has.Count.EqualTo(1));
            Assert.That(manifests[0].Kind, Is.EqualTo(BackupKind.Full));
        });
    }

    [Test]
    public async Task RunScheduledCycleAsync_is_a_no_op_before_the_scope_is_known()
    {
        _fixture = new SchedulerClusterFixture();
        await _fixture.InitializeAsync(o => o.FullBackupScheduleEnabled = true);
        var scope = BackupScopeSelector.WholeTree(Tree);

        // No trigger or EnsureSchedule yet: the grain has no persisted scope.
        var backupId = await _fixture.Scheduler(scope).RunScheduledCycleAsync(incremental: false);

        Assert.That(backupId, Is.Null);
    }

    private async Task SeedAsync(string key, string value)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
    }
}
