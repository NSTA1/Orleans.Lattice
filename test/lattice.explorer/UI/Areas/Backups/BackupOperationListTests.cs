using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Backup;
using NSubstitute;
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
    [TestCase(false)]
    [TestCase(true)]
    public async Task Forget_during_a_read_prevents_the_old_reply_from_becoming_fresh_again(bool refreshFirst)
    {
        var control = Substitute.For<ILatticeBackupOperations>();
        var pending = new TaskCompletionSource<LatticeOperationPage>();
        var calls = 0;
        var fresh = FakeBackupControl.Running("new-operation", BackupOperationKinds.Capture, "orders");
        control.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => ++calls == 1 ? pending.Task : Task.FromResult(new LatticeOperationPage { Operations = [fresh] }));
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());

        var oldRead = list.RecentAsync(CancellationToken.None);
        list.Forget();
        if (refreshFirst)
        {
            await list.RecentAsync(CancellationToken.None);
        }

        pending.SetResult(new LatticeOperationPage { Operations = [] });
        await oldRead;
        var current = await list.RecentAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(2), "the pre-invalidation read must not be reused");
            Assert.That(current.Select(status => status.OperationId), Is.EqualTo(new[] { "new-operation" }));
        });
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

    [Test]
    public async Task LatestAsync_keeps_the_newest_match_per_predicate_and_stops_when_all_are_found()
    {
        var control = Substitute.For<ILatticeBackupOperations>();
        var newest = FakeBackupControl.Running("newest", BackupOperationKinds.Capture, "orders");
        var older = newest with { OperationId = "older" };
        var scrub = FakeBackupControl.Running("scrub", BackupOperationKinds.CatalogScrub, "orders");
        var requests = new List<LatticeOperationListRequest>();
        using var cancellation = new CancellationTokenSource();
        control.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), cancellation.Token)
            .Returns(call =>
            {
                var request = call.Arg<LatticeOperationListRequest>();
                requests.Add(request);
                return request.PageToken switch
                {
                    null => new LatticeOperationPage { Operations = [newest, older], NextPageToken = "next" },
                    "next" => new LatticeOperationPage { Operations = [scrub, older], NextPageToken = "unused" },
                    _ => throw new AssertionException("All predicates matched; no further page should be read."),
                };
            });
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());

        var found = await list.LatestAsync(
            [status => status.Kind == BackupOperationKinds.CatalogScrub,
             status => status.Kind == BackupOperationKinds.Capture,
             status => status.OperationId == "newest"],
            cancellation.Token);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.EqualTo(new[] { scrub, newest, newest }),
                "result slots preserve predicate order, and a later match never replaces the newest");
            Assert.That(requests.Select(request => request.PageToken), Is.EqualTo(new string?[] { null, "next" }));
            Assert.That(requests.Select(request => request.PageSize), Is.All.EqualTo(BackupOperationList.ScanPageSize));
        });
        await control.Received(2).ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), cancellation.Token);
    }

    [Test]
    public async Task LatestAsync_follows_an_empty_page_with_a_continuation_and_returns_null_for_missing_matches()
    {
        var control = Substitute.For<ILatticeBackupOperations>();
        var match = FakeBackupControl.Running("found", BackupOperationKinds.Capture, "orders");
        control.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>())
            .Returns(call => call.Arg<LatticeOperationListRequest>().PageToken is null
                ? new LatticeOperationPage { Operations = [], NextPageToken = "last" }
                : new LatticeOperationPage { Operations = [match] });
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());

        var found = await list.LatestAsync(
            [status => status.OperationId == "missing", status => status.OperationId == "found"],
            CancellationToken.None);

        Assert.That(found, Is.EqualTo(new LatticeOperationStatus?[] { null, match }));
        await control.Received(2).ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), CancellationToken.None);
    }

    [Test]
    public async Task LatestAsync_stops_at_the_scan_limit_even_when_more_pages_are_available()
    {
        var control = Substitute.For<ILatticeBackupOperations>();
        var read = 0;
        var pageCount = BackupOperationList.ScanLimit / BackupOperationList.ScanPageSize;
        var match = FakeBackupControl.Running("found", BackupOperationKinds.Capture, "orders");
        var requests = new List<string?>();
        control.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                requests.Add(call.Arg<LatticeOperationListRequest>().PageToken);
                read++;
                Assert.That(read, Is.LessThanOrEqualTo(pageCount), "the lookback must stay bounded");
                return new LatticeOperationPage
                {
                    Operations = Enumerable.Repeat(match, BackupOperationList.ScanPageSize).ToArray(),
                    NextPageToken = $"page-{read}",
                };
            });
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());

        var found = await list.LatestAsync(
            [status => status.OperationId == "found", status => status.OperationId == "outside-window"],
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.EqualTo(new LatticeOperationStatus?[] { match, null }));
            Assert.That(read * BackupOperationList.ScanPageSize, Is.EqualTo(BackupOperationList.ScanLimit));
            Assert.That(requests, Is.EqualTo(new string?[] { null, "page-1", "page-2", "page-3" }));
        });
    }

    [Test]
    public void LatestAsync_requires_predicates()
    {
        using var caller = new ShellCaller();
        var list = new BackupOperationList(new FakeBackupControl(), caller, new ManualTimeProvider());

        var exception = Assert.ThrowsAsync<ArgumentNullException>(() => list.LatestAsync(null!, CancellationToken.None));

        Assert.That(exception!.ParamName, Is.EqualTo("matches"));
    }

    [Test]
    public async Task LatestAsync_reads_fresh_instead_of_using_or_replacing_the_recent_list_memo()
    {
        var control = new FakeBackupControl();
        var recent = FakeBackupControl.Running("recent", BackupOperationKinds.Capture, "orders");
        control.Statuses[recent.OperationId] = recent;
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());
        var memo = await list.RecentAsync(CancellationToken.None);
        control.Statuses.Clear();

        var found = await list.LatestAsync([_ => true], CancellationToken.None);
        var stillMemoized = await list.RecentAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.EqualTo(new LatticeOperationStatus?[] { null }));
            Assert.That(stillMemoized, Is.SameAs(memo));
            Assert.That(stillMemoized, Is.EqualTo(new[] { recent }));
            Assert.That(Lists(control), Is.EqualTo(2));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public void LatestAsync_propagates_a_later_page_fault_or_cancellation_without_returning_partial_matches(bool cancel)
    {
        var control = Substitute.For<ILatticeBackupOperations>();
        var match = FakeBackupControl.Running("found", BackupOperationKinds.Capture, "orders");
        using var cancellation = new CancellationTokenSource();
        var fault = new InvalidOperationException("second page unavailable");
        control.ListOperationsAsync(Arg.Any<LatticeOperationListRequest>(), cancellation.Token)
            .Returns(call =>
            {
                if (call.Arg<LatticeOperationListRequest>().PageToken is null)
                {
                    return Task.FromResult(new LatticeOperationPage { Operations = [match], NextPageToken = "next" });
                }

                if (cancel)
                {
                    cancellation.Cancel();
                    return Task.FromCanceled<LatticeOperationPage>(cancellation.Token);
                }

                return Task.FromException<LatticeOperationPage>(fault);
            });
        using var caller = new ShellCaller();
        var list = new BackupOperationList(control, caller, new ManualTimeProvider());

        if (cancel)
        {
            var exception = Assert.ThrowsAsync<TaskCanceledException>(() =>
                list.LatestAsync([_ => true, _ => false], cancellation.Token));
            Assert.That(exception!.CancellationToken, Is.EqualTo(cancellation.Token));
        }
        else
        {
            var exception = Assert.ThrowsAsync<InvalidOperationException>(() =>
                list.LatestAsync([_ => true, _ => false], cancellation.Token));
            Assert.That(exception, Is.SameAs(fault));
        }
    }

    private static int Lists(FakeBackupControl control) =>
        control.Calls.Count(static call => call.Verb == nameof(FakeBackupControl.ListOperationsAsync));
}
