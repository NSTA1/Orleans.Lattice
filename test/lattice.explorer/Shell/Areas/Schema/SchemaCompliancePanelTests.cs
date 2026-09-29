using Bunit;
using Orleans.Lattice.Explorer.Shell.Areas.Schema;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Schema;

/// <summary>
/// The Compliance tab: scan and results, the scan the palette command starts
/// through the address, stopping a scan, a tree with no policy, a caller who may
/// not scan, a failed scan, and the compact breakdown.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaCompliancePanelTests : SchemaTestContext
{
    private IRenderedComponent<SchemaTreePage> Open(string query = "tab=compliance", string tree = "orders", LtBreakpoint? band = null)
    {
        var cut = RenderAt<SchemaTreePage>($"schema/{tree}?{query}", band);
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=tabpanel]"), Has.Count.EqualTo(1)));
        return cut;
    }

    private static AngleSharp.Dom.IElement Button(IRenderedComponent<SchemaTreePage> cut, string text) =>
        cut.FindAll("button").Single(button => button.TextContent.Trim() == text);

    [Test]
    public void A_scan_reads_every_value_and_shows_how_many_comply_and_why_the_rest_fail()
    {
        UseEstate();
        Schema.ScanGate = new TaskCompletionSource<LatticeSchemaComplianceReport>();
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-empty__title").TextContent, Is.EqualTo("Not scanned in this session")));

        Button(cut, "Scan compliance").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-status").TextContent, Is.EqualTo("Scanning every value of orders..."));
            Assert.That(Button(cut, "Stop scanning"), Is.Not.Null);
        });

        Schema.ScanGate.SetResult(SchemaTestData.Report("orders", 1200, 34));

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-schema-result .lt-pill").GetAttribute("data-lt-state"), Is.EqualTo("drift"));
            Assert.That(cut.FindAll(".lt-schema-result dl.lt-dl dd").Select(value => value.TextContent.Trim()).Take(3),
                Is.EqualTo(new[] { "1,234 values", "1,200 values", "34 values" }));
            Assert.That(cut.FindAll(".lt-schema-result tbody tr")[0].Children.Select(cell => cell.TextContent.Trim()),
                Is.EqualTo(new[] { "currency does not match", "34" }));
            Assert.That(Ledger.Find("orders")!.Report.NonCompliantCount, Is.EqualTo(34));
            Assert.That(Button(cut, "Scan again"), Is.Not.Null);
        });
    }

    [Test]
    public void A_compliant_tree_is_drawn_healthy_with_no_breakdown()
    {
        UseEstate();
        Schema.Compliance["orders"] = SchemaTestData.Report("orders", 10, 0);
        var cut = Open();

        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Scan compliance"), Is.EqualTo(1)));
        Button(cut, "Scan compliance").Click();

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
        Schema.Compliance["orders"] = SchemaTestData.Report("orders", 10, 0);

        var cut = Open("tab=compliance&scan=start");

        cut.WaitUntil(() =>
        {
            Assert.That(Schema.CountOf("ScanCompliance"), Is.EqualTo(1));
            Assert.That(Navigation.Uri, Does.EndWith("schema/orders?tab=compliance"), "the request is dropped from the address");
            Assert.That(cut.FindAll(".lt-schema-result"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void A_scan_can_be_stopped()
    {
        UseEstate();
        Schema.ScanGate = new TaskCompletionSource<LatticeSchemaComplianceReport>();
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Scan compliance"), Is.EqualTo(1)));
        Button(cut, "Scan compliance").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Stop scanning"), Is.EqualTo(1)));

        Button(cut, "Stop scanning").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=tabpanel] .lt-schema-error").TextContent, Is.EqualTo("The scan was stopped before it finished."));
            Assert.That(Ledger.Find("orders"), Is.Null);
        });
    }

    [Test]
    public void A_failed_scan_is_explained()
    {
        UseEstate();
        Schema.Faults["ScanCompliance"] = new LatticeAuthorizationDeniedException("denied");
        var cut = Open();
        cut.WaitUntil(() => Assert.That(cut.FindAll("button").Count(button => button.TextContent.Trim() == "Scan compliance"), Is.EqualTo(1)));

        Button(cut, "Scan compliance").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=tabpanel] .lt-schema-error").TextContent, Is.EqualTo("You are not permitted to scan compliance.")));
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
            Assert.That(Schema.CountOf("ScanCompliance"), Is.Zero);
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
            Assert.That(Schema.CountOf("ScanCompliance"), Is.Zero);
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
}
