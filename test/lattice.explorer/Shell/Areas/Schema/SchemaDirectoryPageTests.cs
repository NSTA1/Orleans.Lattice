using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Shell.Areas.Schema;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Tests.Shell.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Schema;

/// <summary>
/// <c>/schema</c>: the governed trees with policy, versioning, compliance and the
/// declaring app; every tree; the filter; the "Scan compliance..." picker and its
/// palette command; empty, loading and error states; tenancy; and the compact
/// presentation.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaDirectoryPageTests : SchemaTestContext
{
    private static AngleSharp.Dom.IElement Row(IRenderedComponent<SchemaDirectoryPage> cut, string tree) =>
        cut.FindAll("tbody tr").Single(row => row.Children[0].TextContent.Trim() == tree);

    private static string[] Cells(AngleSharp.Dom.IElement row) => [.. row.Children.Select(cell => cell.TextContent.Trim())];

    [Test]
    public void It_lists_the_trees_under_schema_with_policy_versioning_compliance_and_declaring_app()
    {
        UseEstate();
        Ledger.Record("orders", SchemaTestData.Report("orders", 90, 10), Time.GetUtcNow());
        Ledger.Record("audit", SchemaTestData.Report("audit", 12, 0), Time.GetUtcNow());

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("h1").TextContent, Is.EqualTo("Schema"));
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()),
                Is.EqualTo(new[] { "a/crm/orders", "audit", "orders" }), "an ungoverned tree is not listed");
            Assert.That(Cells(Row(cut, "orders")), Is.EqualTo(new[] { "orders", "3 rules", "family 7 at version 3, strict ingest", "10 values non-compliant", "None" }));
            Assert.That(Cells(Row(cut, "audit")), Is.EqualTo(new[] { "audit", "None", "family 2 at version 1", "Compliant, 12 values", "None" }));
            Assert.That(Cells(Row(cut, "a/crm/orders")), Is.EqualTo(new[] { "a/crm/orders", "None", "Unversioned", "Not scanned", "app crm" }));
            Assert.That(Row(cut, "orders").QuerySelector("th a")!.GetAttribute("href"), Is.EqualTo("schema/orders"));
            Assert.That(Row(cut, "a/crm/orders").QuerySelector("td a")!.GetAttribute("href"), Is.EqualTo("apps/crm"));
            Assert.That(cut.Find(".lt-schema-status").TextContent, Does.StartWith("3 trees of 4 trees under schema."));
            Assert.That(cut.Find(".lt-schema-show__link[aria-current]").TextContent, Is.EqualTo("Under schema"));
            Assert.That(cut.Find("link").GetAttribute("href"), Is.EqualTo(SchemaAssets.Stylesheet));
        });
    }

    [Test]
    public void Compliance_is_drawn_as_a_state_beside_its_words()
    {
        UseEstate();
        Ledger.Record("orders", SchemaTestData.Report("orders", 90, 10), Time.GetUtcNow());
        Ledger.Record("audit", SchemaTestData.Report("audit", 12, 0), Time.GetUtcNow());

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() =>
        {
            Assert.That(Row(cut, "orders").QuerySelector(".lt-pill")!.GetAttribute("data-lt-state"), Is.EqualTo("drift"));
            Assert.That(Row(cut, "audit").QuerySelector(".lt-pill")!.GetAttribute("data-lt-state"), Is.EqualTo("healthy"));
        });
    }

    [Test]
    public void Show_all_lists_every_tree_and_marks_the_switch()
    {
        UseEstate();

        var cut = RenderAt<SchemaDirectoryPage>("schema?show=all");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()),
                Is.EqualTo(new[] { "a/crm/orders", "audit", "orders", "scratch" }));
            Assert.That(Cells(Row(cut, "scratch"))[1..3], Is.EqualTo(new[] { "None", "Unversioned" }));
            Assert.That(cut.Find(".lt-schema-show__link[aria-current]").TextContent, Is.EqualTo("All trees"));
        });
    }

    [Test]
    public void The_filter_narrows_by_tree_id_and_travels_in_the_address()
    {
        UseEstate();

        var cut = RenderAt<SchemaDirectoryPage>("schema?filter=ORD");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr").Select(row => row.Children[0].TextContent.Trim()), Is.EqualTo(new[] { "a/crm/orders", "orders" }));
            Assert.That(cut.FindAll(".lt-schema-show__link").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "schema?filter=ORD", "schema?show=all&filter=ORD" }));
        });

        cut.Find("input[type=search], input").Input("aud");

        Assert.That(Navigation.Uri, Does.EndWith("schema?filter=aud"));
    }

    [Test]
    public void A_filter_that_matches_nothing_says_so()
    {
        UseEstate();

        var cut = RenderAt<SchemaDirectoryPage>("schema?filter=zzz");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("No tree matches")));
    }

    [Test]
    public void With_nothing_under_schema_it_offers_every_tree()
    {
        UseTrees("scratch");

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("No tree is under schema yet"));
            Assert.That(cut.Find(".lt-empty__actions a").GetAttribute("href"), Is.EqualTo("schema?show=all"));
        });
    }

    [Test]
    public void It_shows_a_skeleton_until_the_catalogue_answers()
    {
        var gate = new TaskCompletionSource<TreeCatalogPage>();
        Explorer.Connection.ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>()).Returns(gate.Task);

        var cut = RenderAt<SchemaDirectoryPage>("schema");
        Assert.That(cut.FindAll(".lt-skeleton"), Is.Not.Empty);

        gate.SetResult(new TreeCatalogPage { Entries = [SchemaTestData.Entry("orders")] });
        Schema.Policies["orders"] = SchemaTestData.Policy();

        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_head_without_a_connection_is_told_to_connect_and_can_try_again()
    {
        Services.AddSingleton<Orleans.Lattice.Explorer.Core.Configuration.IExplorerSession>(
            new Orleans.Lattice.Explorer.Tests.Shell.Session.FakeExplorerSession(new Orleans.Lattice.Explorer.Tests.Shell.Session.FakeStateConnection()));

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-empty__title").TextContent, Is.EqualTo("The trees could not be listed"));
            Assert.That(cut.Find(".lt-empty__body").TextContent, Is.EqualTo(SchemaTreeCatalog.NotConnected));
            Assert.That(cut.Find(".lt-empty__actions button").TextContent, Is.EqualTo("Try again"));
        });
    }

    [Test]
    public void A_cluster_that_does_not_serve_schema_says_so()
    {
        Services.RemoveAll<ILatticeSchemaControl>();

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-empty__body").TextContent, Is.EqualTo(SchemaFailure.NotServed)));
    }

    [Test]
    public void Refresh_reads_every_tree_again()
    {
        UseEstate();
        var cut = RenderAt<SchemaDirectoryPage>("schema");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3)));
        Schema.Policies["scratch"] = SchemaTestData.Policy();

        cut.FindAll(".lt-toolbar button").Single(button => button.TextContent == "Refresh").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(4));
            Assert.That(Schema.CountOf("GetPolicy"), Is.EqualTo(8));
        });
    }

    [Test]
    public void A_listing_that_stopped_short_says_how_to_reach_the_rest()
    {
        UseTrees([.. Enumerable.Range(0, SchemaDirectory.MaximumInspected + 1).Select(index => $"t{index:0000}")]);
        Schema.Policies["t0000"] = SchemaTestData.Policy();

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-schema-note").TextContent, Does.Contain("some are not shown here")));
    }

    [Test]
    public void Every_command_has_a_visible_control_on_the_page()
    {
        UseEstate();

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() =>
        {
            foreach (var command in Area.Commands)
            {
                ExplorerCommandControls.AssertVisibleControl(cut, command);
            }
        });
    }

    [Test]
    public void Scan_compliance_offers_only_trees_with_a_policy_and_goes_to_the_scan()
    {
        UseEstate();
        Schema.Policies["audit"] = SchemaTestData.Policy();
        var cut = RenderAt<SchemaDirectoryPage>("schema");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3)));

        cut.Find($"[data-lt-command='{SchemaArea.ScanCommandId}']").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=dialog], [role=alertdialog]").TextContent, Does.Contain("It changes nothing."));
            Assert.That(cut.FindAll("select option").Select(option => option.TextContent), Is.EqualTo(new[] { "audit", "orders" }));
        });

        cut.Find("select").Change("orders");
        cut.FindAll("button").Single(button => button.TextContent == "Scan").Click();

        Assert.That(Navigation.Uri, Does.EndWith("schema/orders?tab=compliance&scan=start"));
    }

    [Test]
    public void Scan_compliance_with_no_policy_anywhere_says_so()
    {
        UseTrees("audit");
        Schema.Versions["audit"] = new(1, 1);
        var cut = RenderAt<SchemaDirectoryPage>("schema");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(1)));

        cut.Find($"[data-lt-command='{SchemaArea.ScanCommandId}']").Click();

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find("[role=dialog] .lt-schema-note").TextContent, Does.StartWith("No tree you can see has a policy"));
            Assert.That(cut.FindAll("button").Single(button => button.TextContent == "Scan").HasAttribute("disabled"), Is.True);
        });

        cut.FindAll("button").Single(button => button.TextContent == "Cancel").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=dialog]"), Is.Empty));
    }

    [Test]
    public async Task The_palette_command_opens_the_picker_on_a_page_already_listening()
    {
        UseEstate();
        var cut = RenderAt<SchemaDirectoryPage>("schema");
        cut.WaitUntil(() => Assert.That(cut.FindAll("tbody tr"), Has.Count.EqualTo(3)));

        await Area.Commands.Single(command => command.Id == SchemaArea.ScanCommandId).InvokeAsync!(CancellationToken.None);

        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog] select").GetAttribute("id"), Is.Not.Null));
    }

    [Test]
    public async Task The_palette_command_is_held_for_a_page_that_renders_after_it()
    {
        UseEstate();
        await Area.Commands.Single(command => command.Id == SchemaArea.ScanCommandId).InvokeAsync!(CancellationToken.None);

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=dialog] option").Select(option => option.TextContent), Is.EqualTo(new[] { "orders" })));
    }

    [Test]
    public void A_scan_recorded_elsewhere_updates_the_compliance_column()
    {
        UseEstate();
        var cut = RenderAt<SchemaDirectoryPage>("schema");
        cut.WaitUntil(() => Assert.That(Cells(Row(cut, "orders"))[3], Is.EqualTo("Not scanned")));

        Ledger.Record("orders", SchemaTestData.Report("orders", 4, 0), Time.GetUtcNow());

        cut.WaitUntil(() => Assert.That(Cells(Row(cut, "orders"))[3], Is.EqualTo("Compliant, 4 values")));
    }

    [Test]
    public void A_denied_or_unavailable_read_is_named_not_hidden()
    {
        UseTrees("orders");
        Schema.Versions["orders"] = new(1, 1);
        Schema.Faults["GetPolicy"] = new LatticeAuthorizationDeniedException("denied");

        var cut = RenderAt<SchemaDirectoryPage>("schema");

        cut.WaitUntil(() => Assert.That(Cells(Row(cut, "orders"))[1], Is.EqualTo("Not permitted")));
    }

    [Test]
    public void Under_tenancy_every_link_stays_in_the_tenant()
    {
        UseEstate();
        UseTenancy("acme");

        var cut = RenderAt<SchemaDirectoryPage>("t/acme/schema", tenancy: true);

        cut.WaitUntil(() =>
        {
            Assert.That(Row(cut, "orders").QuerySelector("th a")!.GetAttribute("href"), Is.EqualTo("t/acme/schema/orders"));
            Assert.That(cut.FindAll(".lt-schema-show__link")[1].GetAttribute("href"), Is.EqualTo("t/acme/schema?show=all"));
            Assert.That(Row(cut, "a/crm/orders").QuerySelector("td a")!.GetAttribute("href"), Is.EqualTo("t/acme/apps/crm"));
        });
    }

    [Test]
    public void Below_the_small_breakpoint_each_tree_is_a_two_line_row_with_a_detail_sheet()
    {
        UseEstate();
        Ledger.Record("orders", SchemaTestData.Report("orders", 90, 10), Time.GetUtcNow());

        var cut = RenderAt<SchemaDirectoryPage>("schema", LtBreakpoint.Compact);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("table"), Is.Empty);
            var rows = cut.FindAll("li.lt-table-list__row");
            Assert.That(rows, Has.Count.EqualTo(3));
            var orders = rows.Single(row => row.QuerySelector(".lt-compact-row__primary")!.TextContent.Trim() == "orders");
            Assert.That(orders.TextContent, Does.Contain("Policy: 3 rules. Versioning: family 7 at version 3, strict ingest."));
            Assert.That(orders.TextContent, Does.Contain("10 values non-compliant"));
        });

        cut.FindAll("li.lt-table-list__row button")[2].Click();

        cut.WaitUntil(() =>
        {
            var sheet = cut.Find("[role=dialog]");
            Assert.That(sheet.QuerySelectorAll(".lt-dialog__actions a").Select(link => link.TextContent), Is.EqualTo(new[] { "Open schema", "Scan compliance" }));
            Assert.That(sheet.QuerySelectorAll(".lt-dialog__actions a").Select(link => link.GetAttribute("href")),
                Is.EqualTo(new[] { "schema/orders", "schema/orders?tab=compliance&scan=start" }));
        });
    }

    [Test]
    public void Below_the_small_breakpoint_the_picker_is_a_sheet()
    {
        UseEstate();
        var cut = RenderAt<SchemaDirectoryPage>("schema", LtBreakpoint.Compact);
        cut.WaitUntil(() => Assert.That(cut.FindAll("li.lt-table-list__row"), Has.Count.EqualTo(3)));

        cut.Find($"[data-lt-command='{SchemaArea.ScanCommandId}']").Click();

        cut.WaitUntil(() => Assert.That(cut.Find("[role=dialog]").ClassList, Does.Contain("lt-dialog--end")));
    }
}
