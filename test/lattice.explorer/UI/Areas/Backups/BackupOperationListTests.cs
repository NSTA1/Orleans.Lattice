using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// The cluster-backed operation list (#4122): it reads the caller's recent
/// operations from the cluster, reuses a read briefly for the same caller, and
/// asks again once the read is stale, forgotten, or the caller's tenant changes.
/// </summary>
[TestFixture]
public sealed class BackupOperationListTests
{
    [Test]
    public async Task A_read_is_reused_for_the_same_caller_until_it_is_stale_or_forgotten()
    {
        var control = new FakeBackupControl();
        control.Statuses["op-1"] = FakeBackupControl.Running("op-1", BackupOperationKinds.Capture, "orders");
        var time = new ManualTimeProvider();
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, time);

        var first = await list.RecentAsync(CancellationToken.None);
        await list.RecentAsync(CancellationToken.None);
        Assert.That(Lists(control), Is.EqualTo(1), "a read within its freshness is reused");

        time.Advance(BackupOperationList.Freshness);
        await list.RecentAsync(CancellationToken.None);
        Assert.That(Lists(control), Is.EqualTo(2), "a stale read is read again");

        list.Forget();
        await list.RecentAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(Lists(control), Is.EqualTo(3), "a forgotten read is read again");
            Assert.That(first.Select(static status => status.OperationId), Is.EqualTo(new[] { "op-1" }));
            var request = (LatticeOperationListRequest)control.Calls.Last(static call => call.Verb == nameof(FakeBackupControl.ListOperationsAsync)).Argument!;
            Assert.That(request.PageSize, Is.EqualTo(BackupOperationList.PageSize));
        });
    }

    [Test]
    public async Task A_change_of_tenant_is_never_served_the_previous_tenants_read()
    {
        var control = new FakeBackupControl();
        var tenant = new FakeActiveTenantProvider("acme");
        using var caller = new ShellCaller(tenant: tenant);
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());

        await list.RecentAsync(CancellationToken.None);
        tenant.Set("globex");
        await list.RecentAsync(CancellationToken.None);

        Assert.That(Lists(control), Is.EqualTo(2));
    }

    [Test]
    public void A_failed_read_is_not_kept()
    {
        var control = new FakeBackupControl { ListFault = new InvalidOperationException("unreachable") };
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());

        Assert.That(() => list.RecentAsync(CancellationToken.None), Throws.InvalidOperationException);
        control.ListFault = null;
        Assert.That(async () => await list.RecentAsync(CancellationToken.None), Is.Empty);
        Assert.That(Lists(control), Is.EqualTo(2));
    }

    [Test]
    public void The_list_needs_its_collaborators()
    {
        using var caller = new ShellCaller();
        var control = new FakeBackupControl();

        Assert.Multiple(() =>
        {
            Assert.That(() => new BackupOperationList(null!, caller, TimeProvider.System), Throws.ArgumentNullException);
            Assert.That(() => new BackupOperationList(control, null!, TimeProvider.System), Throws.ArgumentNullException);
            Assert.That(() => new BackupOperationList(control, caller, null!), Throws.ArgumentNullException);
        });
    }

    private static int Lists(FakeBackupControl control) =>
        control.Calls.Count(static call => call.Verb == nameof(FakeBackupControl.ListOperationsAsync));
}
