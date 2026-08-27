using NSubstitute;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupScheduler"/>, the thin facade that
/// resolves the per-scope <see cref="ILatticeBackupSchedulerGrain"/> by
/// <see cref="BackupScopeKey"/> and forwards each operation to it. The grain
/// factory is substituted so the routing contract - every operation reaches the
/// grain keyed for its own scope, with the request's fields forwarded intact - is
/// asserted without deploying a cluster.
/// </summary>
[TestFixture]
public sealed class LatticeBackupSchedulerTests
{
    private IGrainFactory _grainFactory = null!;
    private ILatticeBackupSchedulerGrain _grain = null!;
    private LatticeBackupScheduler _scheduler = null!;

    [SetUp]
    public void SetUp()
    {
        _grain = Substitute.For<ILatticeBackupSchedulerGrain>();
        _grainFactory = Substitute.For<IGrainFactory>();
        _grainFactory
            .GetGrain<ILatticeBackupSchedulerGrain>(Arg.Any<string>())
            .Returns(_grain);
        _scheduler = new LatticeBackupScheduler(_grainFactory);
    }

    private static BackupScopeSelector Scope(string treeId = "orders") =>
        BackupScopeSelector.WholeTree(treeId);

    [Test]
    public async Task TriggerFullBackupAsync_forwards_to_the_grain_keyed_for_the_scope()
    {
        var scope = Scope();
        _grain.TriggerFullAsync(scope).Returns(Task.FromResult<string?>("backup-1"));

        var backupId = await _scheduler.TriggerFullBackupAsync(scope);

        Assert.That(backupId, Is.EqualTo("backup-1"));
        _grainFactory.Received(1).GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        await _grain.Received(1).TriggerFullAsync(scope);
    }

    [Test]
    public async Task TriggerIncrementalBackupAsync_forwards_to_the_grain_keyed_for_the_scope()
    {
        var scope = Scope();
        _grain.TriggerIncrementalAsync(scope).Returns(Task.FromResult<string?>("backup-2"));

        var backupId = await _scheduler.TriggerIncrementalBackupAsync(scope);

        Assert.That(backupId, Is.EqualTo("backup-2"));
        _grainFactory.Received(1).GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        await _grain.Received(1).TriggerIncrementalAsync(scope);
    }

    [Test]
    public async Task ScheduleRecurringBackupAsync_unpacks_the_request_onto_the_grain_call()
    {
        var scope = Scope();
        var request = new LatticeBackupScheduleRequest(scope, incremental: true, TimeSpan.FromHours(6));

        await _scheduler.ScheduleRecurringBackupAsync(request);

        _grainFactory.Received(1).GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        await _grain.Received(1).ScheduleRecurringAsync(scope, true, TimeSpan.FromHours(6));
    }

    [Test]
    public async Task CancelScheduleAsync_forwards_the_kind_flag()
    {
        var scope = Scope();

        await _scheduler.CancelScheduleAsync(scope, incremental: false);

        _grainFactory.Received(1).GetGrain<ILatticeBackupSchedulerGrain>(BackupScopeKey.For(scope));
        await _grain.Received(1).CancelScheduleAsync(false);
    }

    [Test]
    public async Task EnsureScheduleAsync_forwards_to_the_grain()
    {
        var scope = Scope();

        await _scheduler.EnsureScheduleAsync(scope);

        await _grain.Received(1).EnsureScheduleAsync(scope);
    }

    [Test]
    public async Task PruneAsync_returns_the_grain_retention_report()
    {
        var scope = Scope();
        var report = new BackupRetentionReport(3, ["pruned-1"]);
        _grain.PruneAsync(scope).Returns(Task.FromResult(report));

        var result = await _scheduler.PruneAsync(scope);

        Assert.That(result, Is.SameAs(report));
        await _grain.Received(1).PruneAsync(scope);
    }

    [Test]
    public void Each_scope_routes_to_its_own_grain_key()
    {
        var orders = Scope("orders");
        var invoices = Scope("invoices");

        Assert.That(
            BackupScopeKey.For(orders),
            Is.Not.EqualTo(BackupScopeKey.For(invoices)),
            "distinct scopes must serialize through distinct scheduler grains");
    }

    [Test]
    public void Every_operation_rejects_a_null_scope_or_request()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _scheduler.TriggerFullBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _scheduler.TriggerIncrementalBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _scheduler.ScheduleRecurringBackupAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _scheduler.CancelScheduleAsync(null!, true), Throws.ArgumentNullException);
            Assert.That(async () => await _scheduler.EnsureScheduleAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await _scheduler.PruneAsync(null!), Throws.ArgumentNullException);
        });
    }
}
