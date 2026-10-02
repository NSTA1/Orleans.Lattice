using Bunit;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Areas.Backups;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Backups;

/// <summary>
/// Backup health at <c>/backups/health</c>: not found unless monitoring applies,
/// the newest backups' latest health, and for one backup its report, a check now
/// and its monitoring configuration, gated by the capability probe.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class BackupHealthPageTests : BackupsTestContext
{
    [Test]
    public void Where_monitoring_does_not_apply_the_page_is_not_found_and_the_nav_omits_it()
    {
        var notFound = false;
        Navigation.OnNotFound += (_, _) => notFound = true;

        var cut = RenderAt<BackupHealthPage>("backups/health");

        cut.WaitUntil(() =>
        {
            Assert.That(notFound, Is.True);
            Assert.That(cut.FindAll(".lt-backups-nav__link").Select(link => link.TextContent), Is.EqualTo(new[] { "Catalogue", "Schedules", "Maintenance" }));
        });
    }

    [Test]
    public void The_list_shows_each_backups_latest_health()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"), FakeBackupControl.Manifest("b2", "weekly", createdAt: DateTimeOffset.UnixEpoch));
        Backups.HealthReports["b1"] = new BackupHealthReport("b1", BackupHealthStatus.Warning, true, [], ["a1"], new DateTimeOffset(2026, 9, 28, 0, 0, 0, TimeSpan.Zero), "mismatch");

        var cut = RenderAt<BackupHealthPage>("backups/health");

        cut.WaitUntil(() =>
        {
            var rows = cut.FindAll("tbody tr");
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows[0].TextContent, Does.Contain("Warning").And.Contain("2026-09-28 00:00:00 UTC"));
            Assert.That(rows[1].TextContent, Does.Contain("Not checked").And.Contain("Never"));
            Assert.That(rows[0].QuerySelector("a")!.GetAttribute("href"), Is.EqualTo("backups/health?backup=b1"));
            Assert.That(cut.Find(".lt-backups-nav__link[aria-current]").TextContent, Is.EqualTo("Health"));
        });
    }

    [Test]
    public void An_empty_catalogue_and_a_failed_list_are_stated()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        var empty = RenderAt<BackupHealthPage>("backups/health");
        empty.WaitUntil(() => Assert.That(empty.Find(".lt-empty__title").TextContent, Is.EqualTo("No backups yet")));

        Backups.List = _ => Task.FromException<BackupCatalogPage>(new LatticeAuthorizationDeniedException());
        var failed = RenderAt<BackupHealthPage>("backups/health?x=1");
        failed.WaitUntil(() => Assert.That(failed.Find("[role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
    }

    [Test]
    public void One_backups_report_names_what_is_missing_and_what_does_not_match()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));
        Backups.HealthReports["b1"] = new BackupHealthReport(
            "b1", BackupHealthStatus.Missing, false, ["a1"], ["a2"], DateTimeOffset.UnixEpoch, "Artifacts are missing.",
            BackupSinkSharingStatus.NotShared, ["us-east"]);

        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");

        cut.WaitUntil(() =>
        {
            var values = cut.FindAll(".lt-dl__row").ToDictionary(row => row.QuerySelector(".lt-dl__term")!.TextContent, row => row.QuerySelector(".lt-dl__value")!.TextContent.Trim());
            Assert.That(values["Status"], Is.EqualTo("Missing"));
            Assert.That(values["Manifest"], Is.EqualTo("Missing"));
            Assert.That(values["Missing or uncommitted artifacts"], Is.EqualTo("a1"));
            Assert.That(values["Hash mismatches"], Is.EqualTo("a2"));
            Assert.That(values["Visible to peer clusters"], Is.EqualTo("No: us-east cannot read it"));
            Assert.That(values["Explanation"], Is.EqualTo("Artifacts are missing."));
        });
    }

    [Test]
    public void Check_now_starts_a_cluster_check_followed_with_real_progress_until_the_fresh_report_shows()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("has not been checked yet")));

        cut.FindAll("button").Single(button => button.TextContent == "Check now").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Backups.LastOf<string>(nameof(ILatticeBackupOperations.StartBackupHealthCheckAsync)), Is.EqualTo("b1"));
            Assert.That(cut.FindAll("button").Single(button => button.TextContent == "Checking").HasAttribute("disabled"), Is.True);
            Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Queued"));
        });
        Assert.That(Backups.CountOf(nameof(ILatticeBackupControl.CheckBackupHealthAsync)), Is.Zero, "the blocking verb is not called");
        var id = Backups.Statuses.Keys.Single();
        Assert.That(id, Does.StartWith("health.b1."));

        Backups.Move(id, status => status with
        {
            State = LatticeOperationState.Running,
            Phase = BackupOperationPhases.Verifying,
            PhaseIndex = 0,
            PhaseCount = 1,
            CompletedUnits = 1,
            TotalUnits = 4,
            UnitName = BackupOperationUnits.Artifacts,
        });
        Tick();
        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Verifying artifacts"));
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("1 of 4 artifacts"));
            Assert.That(cut.FindAll("a").Single(a => a.TextContent == "Open the check's page").GetAttribute("href"), Is.EqualTo("backups/operations/" + id));
        });

        Backups.HealthReports["b1"] = new BackupHealthReport("b1", BackupHealthStatus.Healthy, true, [], [], DateTimeOffset.UnixEpoch, "All present.", BackupSinkSharingStatus.Shared);
        Backups.Succeed(id, "b1", FakeBackupControl.HealthResult("b1", BackupHealthStatus.Healthy));
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-dl .lt-pill").TextContent.Trim(), Is.EqualTo("Healthy"));
            Assert.That(cut.Markup, Does.Contain("Yes, every peer cluster can read it"));
            Assert.That(cut.FindAll("button").Single(button => button.TextContent == "Check now").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void A_check_still_running_from_an_earlier_page_is_picked_up()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));
        var id = BackupClusterOperation.HealthCheckId("b1", DateTimeOffset.UnixEpoch)!;
        Backups.Statuses[id] = FakeBackupControl.Running(id, BackupOperationKinds.HealthCheck, "orders") with
        {
            State = LatticeOperationState.Running,
            Phase = BackupOperationPhases.Verifying,
            CompletedUnits = 2,
            TotalUnits = 3,
            UnitName = BackupOperationUnits.Artifacts,
        };

        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__detail").TextContent, Is.EqualTo("2 of 3 artifacts"));
            Assert.That(cut.FindAll("button").Single(button => button.TextContent == "Checking").HasAttribute("disabled"), Is.True);
        });
        Assert.That(Backups.CountOf(nameof(ILatticeBackupOperations.StartBackupHealthCheckAsync)), Is.Zero);

        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True);
        cut.Instance.Dispose();
        var reads = Backups.StatusReads;
        Time.Advance(ClusterStatusPoller.Interval * 3);
        Assert.That(Backups.StatusReads, Is.EqualTo(reads), "leaving the page stops following the check");
    }

    [Test]
    public void A_refused_check_says_why()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));
        Backups.StartFault = new KeyNotFoundException();
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Check now"), Has.Exactly(1).Items));

        cut.FindAll("button").Single(button => button.TextContent == "Check now").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotFound)));
    }

    [Test]
    public void A_check_that_fails_on_the_cluster_says_where_it_stopped()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Check now"), Has.Exactly(1).Items));
        cut.FindAll("button").Single(button => button.TextContent == "Check now").Click();
        cut.WaitUntil(() => Assert.That(Backups.Statuses, Has.Count.EqualTo(1)));
        var id = Backups.Statuses.Keys.Single();

        Backups.Move(id, status => status with { State = LatticeOperationState.Failed, FailureReason = "IOException: the store went away", FinishedAtUtc = status.StartedAtUtc });
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-operation-progress .lt-pill").TextContent.Trim(), Is.EqualTo("Failed"));
            Assert.That(cut.Find(".lt-operation-progress").TextContent, Does.Contain("the store went away"));
            Assert.That(cut.FindAll("button").Single(button => button.TextContent == "Check now").HasAttribute("disabled"), Is.False);
        });
    }

    [Test]
    public void Monitoring_is_configured_per_backup()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-backups-form"), Has.Count.EqualTo(1)));

        // #4148: the interval is one duration field with a box per unit.
        Assert.That(cut.FindAll("form.lt-backups-form .lt-duration__input").Select(input => input.GetAttribute("aria-label")), Is.EqualTo(new[] { "Verify every, hours", "Verify every, minutes" }));
        cut.FindAll("form.lt-backups-form input")[0].Input("2");
        cut.FindAll("form.lt-backups-form input")[1].Input("15");
        cut.Find("form.lt-backups-form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find("form [role=status]").TextContent, Is.EqualTo("Monitoring on: this backup is verified every 2 h 15 min.")));
        var (id, config) = Backups.LastOf<(string, BackupHealthConfig)>(nameof(ILatticeBackupControl.ConfigureBackupHealthAsync));
        Assert.Multiple(() =>
        {
            Assert.That(id, Is.EqualTo("b1"));
            Assert.That(config, Is.EqualTo(new BackupHealthConfig(true, TimeSpan.FromMinutes(135))));
        });

        cut.Find("button[role=switch]").Click();
        cut.Find("form.lt-backups-form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find("form [role=status]").TextContent, Is.EqualTo("Monitoring off for this backup.")));
    }

    [Test]
    public void A_bad_or_refused_configuration_says_why()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));
        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-backups-form"), Has.Count.EqualTo(1)));

        cut.FindAll("form.lt-backups-form input")[0].Input("0");
        cut.FindAll("form.lt-backups-form input")[1].Input("0");
        cut.Find("form.lt-backups-form").Submit();
        Assert.That(cut.Find(".lt-field__error").TextContent, Does.Contain("Give at least 1 min."));

        Backups.Configure = (_, _) => Task.FromException(new LatticeAuthorizationDeniedException());
        cut.FindAll("form.lt-backups-form input")[0].Input("1");
        cut.Find("form.lt-backups-form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find("form [role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
    }

    [Test]
    public void A_restricted_identity_cannot_check_or_configure()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Backups.Probe = scope => Task.FromResult(FakeBackupControl.DenyAll(scope));
        Seed(FakeBackupControl.Manifest("b1", "nightly"));

        var cut = RenderAt<BackupHealthPage>("backups/health?backup=b1");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Markup, Does.Contain("You may not check or configure this backup's health."));
            Assert.That(cut.FindAll("button").Where(button => button.TextContent == "Check now"), Is.Empty);
            Assert.That(cut.FindAll("form"), Is.Empty);
        });
    }

    [Test]
    public void An_unknown_focused_backup_is_not_found_and_a_failed_read_says_why()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        var notFound = false;
        Navigation.OnNotFound += (_, _) => notFound = true;
        RenderAt<BackupHealthPage>("backups/health?backup=nope");
        Assert.That(notFound, Is.True);

        Backups.Describe = _ => Task.FromException<BackupChainDescription?>(new LatticeAuthorizationDeniedException());
        var denied = RenderAt<BackupHealthPage>("backups/health?backup=b1");
        denied.WaitUntil(() => Assert.That(denied.Find("[role=alert]").TextContent, Is.EqualTo(BackupsFaults.NotPermitted)));
    }

    [Test]
    public void The_health_list_is_compact_below_the_small_breakpoint()
    {
        Backups.HealthAvailable = () => Task.FromResult(true);
        Seed(FakeBackupControl.Manifest("b1", "nightly"));

        var cut = RenderAt<BackupHealthPage>("backups/health", LtBreakpoint.Compact);

        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-table-list__row"), Has.Count.EqualTo(1)));
        cut.Find(".lt-table-list__row button").Click();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog] a").GetAttribute("href"), Is.EqualTo("backups/health?backup=b1")));
    }

    /// <summary>Moves the circuit's clock one polling interval, once the follower has armed its wait.</summary>
    private void Tick()
    {
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True, "the follower re-arms");
        Time.Advance(ClusterStatusPoller.Interval);
    }
}
