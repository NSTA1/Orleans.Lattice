using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>Health reads and actions belong to the address that started them.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupHealthOwnershipTests : BackupsTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Catalogue_discards_a_superseded_page_or_fault(bool fault)
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        var pending = new TaskCompletionSource<BackupCatalogPage>(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.List = _ => pending.Task;
        var cut = RenderAt<BackupHealthPage>("backups/health");
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ListBackupsAsync)), Is.EqualTo(1)));

        Backups.List = _ => Task.FromResult(new BackupCatalogPage { Entries = [FakeBackupControl.Manifest("current", "current backup")] });
        Navigate(cut, "backups/health?refresh=1");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("current backup")));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old catalogue failed"));
        }
        else
        {
            pending.SetResult(new BackupCatalogPage { Entries = [FakeBackupControl.Manifest("stale", "stale backup")] });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains(fault ? "old catalogue failed" : "stale backup", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Markup, Does.Contain("current backup"));
        Assert.That(Backups.Calls.Where(call => call.Verb == nameof(ILatticeBackupControl.GetBackupHealthAsync))
            .Select(call => call.Argument), Does.Not.Contain("stale"));
    }

    [Test]
    public async Task Catalogue_discards_a_superseded_report_batch()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1"));
        var pending = new TaskCompletionSource<BackupHealthReport?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.GetHealth = _ => pending.Task;
        var cut = RenderAt<BackupHealthPage>("backups/health");
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupHealthAsync)), Is.EqualTo(1)));

        Backups.GetHealth = _ => Task.FromResult<BackupHealthReport?>(Report("b1", BackupHealthStatus.Healthy, "current report"));
        Navigate(cut, "backups/health?refresh=1");
        cut.WaitUntil(() => Assert.That(cut.Find("tbody").TextContent, Does.Contain("Healthy")));
        pending.SetResult(Report("b1", BackupHealthStatus.Missing, "stale report"));

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains(">Missing<", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Find("tbody").TextContent, Does.Contain("Healthy").And.Not.Contain("Missing"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Focus_discards_a_superseded_report_or_fault(bool fault)
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("old", "old backup"), FakeBackupControl.Manifest("current", "current backup"));
        var pending = new TaskCompletionSource<BackupHealthReport?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.GetHealth = id => id == "old"
            ? pending.Task
            : Task.FromResult<BackupHealthReport?>(Report(id, BackupHealthStatus.Healthy, "current report"));
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=old");
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupHealthAsync)), Is.EqualTo(1)));

        Navigate(cut, "backups/health?backup=current");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("current report")));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old report failed"));
        }
        else
        {
            pending.SetResult(Report("old", BackupHealthStatus.Missing, "stale report"));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains(fault ? "old report failed" : "stale report", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Markup, Does.Contain("current backup").And.Contain("current report").And.Not.Contain("old backup"));
    }

    [Test]
    public async Task Focus_discards_a_superseded_capability_probe_before_reading_health()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("old", tree: "old-tree"), FakeBackupControl.Manifest("current"));
        var pending = new TaskCompletionSource<BackupScopeCapabilities>(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.Probe = scope => scope.TreeId == "old-tree"
            ? pending.Task
            : Task.FromResult(FakeBackupControl.DenyAll(scope));
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=old");
        cut.WaitUntil(() => Assert.That(Backups.Calls.Any(call =>
            call.Verb == nameof(ILatticeBackupControl.ProbeCapabilitiesAsync)
            && call.Argument is BackupScopeSelector { TreeId: "old-tree" }), Is.True));

        Navigate(cut, "backups/health?backup=current");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("You may not check or configure")));
        pending.SetResult(FakeBackupControl.AllowAll(BackupScopeSelector.WholeTree("old-tree")));

        Assert.That(await TestPoll.TryUntilAsync(
            () => Backups.Calls.Any(call => call.Verb == nameof(ILatticeBackupControl.GetBackupHealthAsync)
                && Equals(call.Argument, "old")),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.FindAll("form"), Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Check_start_cannot_replace_the_next_backups_check_or_error(bool fault)
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("old"), FakeBackupControl.Manifest("current"));
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.StartGate = pending.Task;
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=old");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Any(button => button.TextContent == "Check now"), Is.True));
        var start = cut.FindAll("button").Single(button => button.TextContent == "Check now").ClickAsync();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupHealthCheckAsync)), Is.EqualTo(1)));

        Navigate(cut, "backups/health?backup=current");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Any(button => button.TextContent == "Check now"), Is.True,
            "the next backup must not inherit the old start's busy state"));
        Backups.StartGate = null;
        await cut.FindAll("button").Single(button => button.TextContent == "Check now").ClickAsync();
        cut.WaitUntil(() => Assert.That(cut.FindAll("a").Any(a => a.GetAttribute("href")?.Contains("health.current.", StringComparison.Ordinal) == true), Is.True));

        if (fault)
        {
            pending.SetException(new InvalidOperationException("old check failed"));
        }
        else
        {
            pending.SetResult();
        }

        await start;
        Assert.That(cut.Markup, Does.Not.Contain("old check failed").And.Not.Contain("health.old."));
        Assert.That(cut.FindAll("a").Any(a => a.GetAttribute("href")?.Contains("health.current.", StringComparison.Ordinal) == true), Is.True);
    }

    [Test]
    public async Task Focus_discards_a_superseded_monitoring_read()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("old"), FakeBackupControl.Manifest("current"));
        Backups.HealthConfigs["current"] = new BackupHealthConfig(true, TimeSpan.FromHours(2));
        var pending = new TaskCompletionSource<BackupHealthConfig?>();
        Backups.GetHealthConfig = id => id == "old"
            ? pending.Task
            : Task.FromResult<BackupHealthConfig?>(Backups.HealthConfigs[id]);
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=old");
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupHealthConfigAsync)), Is.EqualTo(1)));

        Navigate(cut, "backups/health?backup=current");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-backups-form input").Select(input => input.GetAttribute("value")), Is.EqualTo(new[] { "2", "0" })));

        // Completed on the renderer's own context, so the old read's continuation runs inline before the redraw.
        await cut.InvokeAsync(() => pending.SetResult(new BackupHealthConfig(false, TimeSpan.FromHours(9))));
        cut.Render();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("button[role=switch]").GetAttribute("aria-checked"), Is.EqualTo("true"));
            Assert.That(cut.FindAll("form.lt-backups-form input").Select(input => input.GetAttribute("value")), Is.EqualTo(new[] { "2", "0" }));
        });
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Monitoring_discards_a_superseded_save_or_fault(bool fault)
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("old"), FakeBackupControl.Manifest("current"));
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.Configure = (_, _) => pending.Task;
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=old");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(1)));
        var save = cut.Find("form").SubmitAsync();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.ConfigureBackupHealthAsync)), Is.EqualTo(1)));

        Navigate(cut, "backups/health?backup=current");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form"), Has.Count.EqualTo(1)));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old monitoring failed"));
        }
        else
        {
            pending.SetResult();
        }

        await save;
        Assert.That(cut.FindAll("form [role=status], form [role=alert]"), Is.Empty);
    }

    [Test]
    public async Task An_old_check_start_cannot_clear_a_new_pending_start()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("old"), FakeBackupControl.Manifest("current"));
        var old = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.StartGate = old.Task;
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=old");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Any(button => button.TextContent == "Check now"), Is.True));
        var oldStart = cut.FindAll("button").Single(button => button.TextContent == "Check now").ClickAsync();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupHealthCheckAsync)), Is.EqualTo(1)));

        Navigate(cut, "backups/health?backup=current");
        var current = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.StartGate = current.Task;
        var currentStart = cut.FindAll("button").Single(button => button.TextContent == "Check now").ClickAsync();
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupHealthCheckAsync)), Is.EqualTo(2)));
        old.SetResult();
        await oldStart;
        Assert.That(cut.FindAll("button").Single(button => button.TextContent == "Checking").HasAttribute("disabled"), Is.True);
        current.SetResult();
        await currentStart;
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_completed_checks_report_cannot_replace_a_new_load_of_the_same_backup(bool fault)
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1"));
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("has not been checked yet")));
        await cut.FindAll("button").Single(button => button.TextContent == "Check now").ClickAsync();
        var id = Backups.Statuses.Keys.Single();
        var pending = new TaskCompletionSource<BackupHealthReport?>(TaskCreationOptions.RunContinuationsAsynchronously);
        Backups.GetHealth = _ => pending.Task;
        Backups.Succeed(id, "b1", FakeBackupControl.HealthResult("b1", BackupHealthStatus.Healthy));
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True);
        Time.Advance(ClusterStatusPoller.Interval);
        cut.WaitUntil(() => Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.GetBackupHealthAsync)), Is.EqualTo(2)));

        Backups.GetHealth = _ => Task.FromResult<BackupHealthReport?>(Report("b1", BackupHealthStatus.Healthy, "current report"));
        Navigate(cut, "backups/health?backup=b1&refresh=1");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("current report")));
        if (fault)
        {
            pending.SetException(new InvalidOperationException("old completion failed"));
        }
        else
        {
            pending.SetResult(Report("b1", BackupHealthStatus.Missing, "stale completion"));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains(fault ? "old completion failed" : "stale completion", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.Markup, Does.Contain("current report"));
    }

    private void Navigate(IRenderedComponent<BackupHealthPage> cut, string address)
    {
        Navigation.NavigateTo(address);
        cut.Render();
    }

    private static BackupHealthReport Report(string id, BackupHealthStatus status, string explanation) =>
        new(id, status, true, [], [], DateTimeOffset.UnixEpoch, explanation);
}
