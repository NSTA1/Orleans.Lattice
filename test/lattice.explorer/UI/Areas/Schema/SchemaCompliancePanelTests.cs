using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// The Compliance tab (#4126): a scan starts as a tracked cluster operation whose
/// progress is followed until its report is recorded; the scan the palette
/// command starts through the address; stopping a scan; a failed start and a
/// failed scan; picking up a scan already running or finished on the cluster; a
/// tree with no policy, a caller who may not scan, and the compact breakdown.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaCompliancePanelTests : SchemaTestContext
{
    /// <summary>Registers the scan operations the panel starts and follows.</summary>
    public SchemaCompliancePanelTests()
    {
        Scans = new FakeSchemaComplianceOperations();
        Services.AddKeyedSingleton<ILatticeSchemaComplianceOperations>(ShellFacades.Key, Scans);
    }

    private FakeSchemaComplianceOperations Scans { get; }

    private IRenderedComponent<SchemaTreePage> Open(string query = "tab=compliance", string tree = "orders", LtBreakpoint? band = null)
    {
        var cut = RenderAt<SchemaTreePage>($"schema/{tree}?{query}", band);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel]"), Has.Count.EqualTo(1)));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    private static bool HasButton(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Any(button => button.TextContent.Trim() == text);

    private IRenderedComponent<SchemaTreePage> StartScan()
    {
        var cut = Open();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Scan compliance"), Is.True));
        Button(cut, "Scan compliance").Click();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Stop scanning"), Is.True));
        return cut;
    }

    private void Tick()
    {
        Assert.That(SpinWait.SpinUntil(() => Time.ArmedTimers >= 1, TimeSpan.FromSeconds(10)), Is.True, "the follower re-arms");
        Time.Advance(ClusterStatusPoller.Interval);
    }

    [Test]
    public void A_scan_runs_on_the_cluster_and_its_progress_is_followed_until_the_report_is_recorded()
    {
        UseEstate();
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Not scanned in this session")));

        Button(cut, "Scan compliance").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(Scans.Calls, Does.Contain((nameof(ILatticeSchemaComplianceOperations.StartComplianceScanAsync), (object?)"orders")));
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-status").TextContent, Is.EqualTo("Scanning every value of orders..."));
            Assert.That(cut.Find("[role=progressbar]").GetAttribute("aria-label"), Is.EqualTo("Compliance scan progress"));
            Assert.That(HasButton(cut, "Stop scanning"), Is.True);
        });
        var id = Scans.Latest!;

        Scans.Progress(id, SchemaComplianceScanOperation.ScanningPhase, 500, 1000, SchemaComplianceScanOperation.EntriesUnit);
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-progress__phase").TextContent, Is.EqualTo("Scanning"));
            Assert.That(cut.Find("[role=progressbar]").GetAttribute("aria-valuenow"), Is.EqualTo("50"));
            Assert.That(Ledger.Find("orders"), Is.Null, "nothing is recorded before the scan finishes");
        });

        Scans.Succeed(id, SchemaTestData.Report("orders", 1200, 34));
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-result .lt-pill").GetAttribute("data-lt-state"), Is.EqualTo("drift"));
            Assert.That(cut.FindAll(".lt-schema-result dl.lt-dl dd").Select(value => value.TextContent.Trim()).Take(3),
                Is.EqualTo(new[] { "1,234 values", "1,200 values", "34 values" }));
            Assert.That(cut.FindAll(".lt-schema-result tbody tr")[0].Children.Select(cell => cell.TextContent.Trim()),
                Is.EqualTo(new[] { "currency does not match", "34" }));
            Assert.That(Ledger.Find("orders")!.Report.NonCompliantCount, Is.EqualTo(34));
            Assert.That(cut.FindAll(".lt-operation-progress"), Is.Empty, "a finished scan draws no progress");
            Assert.That(HasButton(cut, "Scan again"), Is.True);
        });
    }

    [Test]
    public void A_compliant_tree_is_drawn_healthy_with_no_breakdown()
    {
        UseEstate();
        var cut = StartScan();

        Scans.Succeed(Scans.Latest!, SchemaTestData.Report("orders", 10, 0));
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-result .lt-pill").GetAttribute("data-lt-state"), Is.EqualTo("healthy"));
            Assert.That(cut.FindAll(".lt-schema-result table"), Is.Empty);
        });
    }

    [Test]
    public void The_scan_command_arrives_as_an_address_that_starts_the_scan_once()
    {
        UseEstate();

        var cut = Open("tab=compliance&scan=start");

        cut.WaitUntil(() =>
        {
            Assert.That(Scans.CountOf(nameof(ILatticeSchemaComplianceOperations.StartComplianceScanAsync)), Is.EqualTo(1));
            Assert.That(Navigation.Uri, Does.EndWith("schema/orders?tab=compliance"), "the request is dropped from the address");
            Assert.That(HasButton(cut, "Stop scanning"), Is.True);
        });
    }

    [Test]
    public void A_scan_can_be_stopped()
    {
        UseEstate();
        var cut = StartScan();
        var id = Scans.Latest!;

        Button(cut, "Stop scanning").Click();

        cut.WaitUntil(() => Assert.That(Scans.Calls, Does.Contain((nameof(ILatticeOperations.CancelOperationAsync), (object?)id))));
        Scans.Cancel(id);
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-error").TextContent, Is.EqualTo(SchemaCompliancePanel.StoppedText));
            Assert.That(Ledger.Find("orders"), Is.Null);
            Assert.That(HasButton(cut, "Scan compliance"), Is.True);
        });
    }

    [Test]
    public void A_stop_the_cluster_refuses_says_why_while_the_scan_runs_on()
    {
        UseEstate();
        Scans.CancelFault = new LatticeAuthorizationDeniedException("denied");
        var cut = StartScan();

        Button(cut, "Stop scanning").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-error").TextContent, Is.EqualTo("You are not permitted to stop the scan."));
            Assert.That(HasButton(cut, "Stop scanning"), Is.True, "the scan is still running");
        });
    }

    [Test]
    public void A_scan_that_could_not_start_is_explained()
    {
        UseEstate();
        Scans.StartFault = new LatticeAuthorizationDeniedException("denied");
        var cut = Open();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Scan compliance"), Is.True));

        Button(cut, "Scan compliance").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-error").TextContent, Is.EqualTo("You are not permitted to scan compliance."));
            Assert.That(HasButton(cut, "Scan compliance"), Is.True, "the tab can try again");
        });
    }

    [Test]
    public void A_scan_that_failed_on_the_cluster_gives_its_reason()
    {
        UseEstate();
        var cut = StartScan();

        Scans.Fail(Scans.Latest!, "The tree was deleted while it was scanned.");
        Tick();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-error").TextContent, Is.EqualTo("The scan failed. The tree was deleted while it was scanned."));
            Assert.That(Ledger.Find("orders"), Is.Null);
        });
    }

    [Test]
    public void A_scan_already_running_on_the_cluster_is_followed_on_arrival_without_starting_another()
    {
        UseEstate();
        Scans.Statuses["elsewhere"] = Scans.Queued("elsewhere", "orders") with { State = LatticeOperationState.Running, Phase = SchemaComplianceScanOperation.ScanningPhase, CompletedUnits = 30, TotalUnits = 100 };

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=progressbar]").GetAttribute("aria-valuenow"), Is.EqualTo("30"));
            Assert.That(HasButton(cut, "Stop scanning"), Is.True);
        });
        Assert.That(Scans.CountOf(nameof(ILatticeSchemaComplianceOperations.StartComplianceScanAsync)), Is.Zero);

        Scans.Succeed("elsewhere", SchemaTestData.Report("orders", 9, 1));
        Tick();

        cut.WaitUntil(() => Assert.That(Ledger.Find("orders")!.Report.CompliantCount, Is.EqualTo(9)));
    }

    [Test]
    public void The_address_does_not_start_a_second_scan_over_one_already_running()
    {
        UseEstate();
        Scans.Statuses["elsewhere"] = Scans.Queued("elsewhere", "orders") with { State = LatticeOperationState.Running };

        var cut = Open("tab=compliance&scan=start");

        cut.WaitUntil(() =>
        {
            Assert.That(Navigation.Uri, Does.EndWith("schema/orders?tab=compliance"));
            Assert.That(HasButton(cut, "Stop scanning"), Is.True);
        });
        Assert.That(Scans.CountOf(nameof(ILatticeSchemaComplianceOperations.StartComplianceScanAsync)), Is.Zero);
    }

    [Test]
    public void A_scan_finished_on_the_cluster_is_shown_when_the_session_has_none()
    {
        UseEstate();
        Scans.Statuses["earlier"] = Scans.Queued("earlier", "orders");
        Scans.Succeed("earlier", SchemaTestData.Report("orders", 7, 2));

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-result dl.lt-dl dd")[0].TextContent.Trim(), Is.EqualTo("9 values"));
            Assert.That(Ledger.Find("orders")!.ScannedAt, Is.EqualTo(Scans.Statuses["earlier"].FinishedAtUtc));
        });
    }

    [Test]
    public void A_scan_of_another_tree_is_not_picked_up()
    {
        UseEstate();
        Scans.Statuses["other"] = Scans.Queued("other", "audit") with { State = LatticeOperationState.Running };

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Not scanned in this session"));
            Assert.That(HasButton(cut, "Scan compliance"), Is.True);
        });
    }

    [Test]
    public void A_head_that_serves_no_scans_says_so()
    {
        UseEstate();
        Services.AddKeyedSingleton<ILatticeSchemaComplianceOperations>(ShellFacades.Key, (_, _) => null!);
        var cut = Open();
        cut.WaitUntil(() => Assert.That(HasButton(cut, "Scan compliance"), Is.True));

        Button(cut, "Scan compliance").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-schema-error").TextContent, Is.EqualTo(SchemaFailure.NotServed)));
    }

    [Test]
    public void The_last_scan_in_the_session_is_shown_on_arrival()
    {
        UseEstate();
        Ledger.Record("orders", SchemaTestData.Report("orders", 7, 0), Time.GetUtcNow());

        var cut = Open();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-result dl.lt-dl dd")[0].TextContent.Trim(), Is.EqualTo("7 values"));
            Assert.That(cut.FindAll(".lt-schema-result dl.lt-dl dd")[3].TextContent.Trim(), Is.EqualTo("2026-01-01 00:00:00 UTC"));
            Assert.That(Scans.CountOf(nameof(ILatticeSchemaComplianceOperations.StartComplianceScanAsync)), Is.Zero);
        });
    }

    [Test]
    public void A_tree_with_no_policy_has_nothing_to_scan_against()
    {
        UseTrees("audit");
        Schema.Versions["audit"] = new(1, 1);

        var cut = Open(tree: "audit");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Nothing to scan against"));
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__actions a").GetAttribute("href"), Is.EqualTo("schema/audit"));
        });
    }

    [Test]
    public void A_caller_who_may_not_scan_is_told_so_even_when_the_address_asks()
    {
        UseEstate();
        Schema.Capabilities["orders"] = tree => FakeSchemaControl.ReadOnly(tree) with { CanScanCompliance = false };

        var cut = Open("tab=compliance&scan=start");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("You may not scan this tree"));
            Assert.That(Scans.Calls, Is.Empty, "a caller who may not scan neither starts nor lists scans");
        });
    }

    [Test]
    public void Below_the_small_breakpoint_the_breakdown_is_two_line_rows()
    {
        UseEstate();
        Ledger.Record("orders", SchemaTestData.Report("orders", 10, 3), Time.GetUtcNow());

        var cut = Open(band: LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll(".lt-schema-result table"), Is.Empty);
            var row = cut.Find(".lt-schema-result li.lt-table-list__row");
            Assert.That(row.QuerySelector(".lt-compact-row__primary")!.TextContent.Trim(), Is.EqualTo("currency does not match"));
            Assert.That(row.QuerySelector(".lt-compact-row__secondary")!.TextContent.Trim(), Is.EqualTo("3 values"));
        });
    }

    [TestCase("orders", "orders", true)]
    [TestCase("t/acme/orders", "orders", true)]
    [TestCase("t/acme/xorders", "orders", false)]
    [TestCase("t/acme/x/orders", "orders", false)]
    [TestCase("audit", "orders", false)]
    public void A_status_scans_a_tree_by_its_name_or_its_name_inside_a_tenant(string scanned, string treeId, bool expected)
    {
        var status = Scans.Queued("op", scanned);

        Assert.That(SchemaCompliancePanel.Scans(status, treeId), Is.EqualTo(expected));
    }

    [Test]
    public void A_status_over_several_trees_scans_none_of_them()
    {
        Assert.That(SchemaCompliancePanel.Scans(Scans.Queued("op", "orders", "audit"), "orders"), Is.False);
    }
}
