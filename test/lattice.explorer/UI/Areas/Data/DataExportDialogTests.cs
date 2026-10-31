using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// Export on the tree page (issue #5366): the action is drawn only for a caller the Data
/// API admits to range-read the tree, the palette command opens it, the dialog validates
/// the scope, prepares a single-use download whose form posts the ticket and this circuit's
/// binding, and follows the ticket to its verdict - complete, partial, or refused.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataExportDialogTests : DataTestContext
{
    private const string ExportControl = "[data-lt-command=\"data.export\"]";

    private readonly FakeDataExportSources _sources = new();

    /// <summary>Registers the in-memory export sources over the Shell's.</summary>
    public DataExportDialogTests() => Services.AddSingleton<IDataExportSources>(_sources);

    [Test]
    public void A_caller_who_may_range_read_the_tree_sees_export_as_the_command_control()
    {
        Client.WithTree("orders", keys: 3);

        var cut = RenderAt("data/orders");

        cut.WaitUntil(() => Assert.That(cut.FindAll(ExportControl), Has.Count.EqualTo(1)));
        ExplorerCommandControls.AssertVisibleControl(cut, Area.Commands.Single(command => command.Id == DataArea.ExportCommandId));
        Assert.That(_sources.Source.Requests.Single(), Has.Property(nameof(DataRangeRequest.IncludeGateCoverage)).True.And.Property(nameof(DataRangeRequest.PageSize)).EqualTo(1));
    }

    [Test]
    public void A_narrowed_grant_still_sees_export()
    {
        Client.WithTree("orders", keys: 3);
        _sources.Source.Coverage = LatticeRangeReadGateCoverage.Filtered;

        var cut = RenderAt("data/orders");

        cut.WaitUntil(() => Assert.That(cut.FindAll(ExportControl), Has.Count.EqualTo(1)));
    }

    [Test]
    public void A_caller_without_range_read_sees_no_export_and_the_command_opens_nothing()
    {
        Client.WithTree("orders", keys: 3);
        _sources.Source.Coverage = LatticeRangeReadGateCoverage.Denied;
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(_sources.Source.Requests, Has.Count.EqualTo(1)));

        cut.InvokeAsync(async () => await Area.Commands.Single(command => command.Id == DataArea.ExportCommandId).InvokeAsync!(CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(ExportControl), Is.Empty);
            Assert.That(cut.FindAll(".lt-data-export"), Is.Empty);
        });
    }

    [Test]
    public void A_probe_that_fails_hides_export()
    {
        Client.WithTree("orders", keys: 3);
        _sources.ProbeFault = new InvalidOperationException("no data api");

        var cut = RenderAt("data/orders");

        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("orders")));
        Assert.That(cut.FindAll(ExportControl), Is.Empty);
    }

    [Test]
    public void A_view_is_never_offered_for_export()
    {
        Client.WithTree("orders");
        Client.Views.Add(new ViewStateSummary { ViewName = "by-status", SourceTreeId = "orders" });
        Client.Entries["view-by-status"] = new(StringComparer.Ordinal);

        var cut = RenderAt("data/view-by-status");

        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("by-status")));
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(ExportControl), Is.Empty);
            Assert.That(_sources.Source.Requests, Is.Empty, "a view is not even probed");
        });
    }

    [Test]
    public void The_palette_command_opens_the_dialog_on_the_open_tree()
    {
        Client.WithTree("orders", keys: 3);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(cut.FindAll(ExportControl), Has.Count.EqualTo(1)));

        cut.InvokeAsync(async () => await Area.Commands.Single(command => command.Id == DataArea.ExportCommandId).InvokeAsync!(CancellationToken.None));

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("Export orders")));
    }

    [Test]
    public void A_command_run_before_the_tree_page_is_held_until_it_opens()
    {
        Client.WithTree("orders", keys: 3);
        Services.GetRequiredService<DataCommandSignals>().RequestAsync(DataArea.ExportCommandId);

        var cut = RenderAt("data/orders");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-dialog__title").TextContent, Is.EqualTo("Export orders")));
    }

    [Test]
    public void The_dialog_offers_scope_and_format_and_prefills_the_keys_panel_prefix()
    {
        Client.WithTree("orders", keys: 3);
        var cut = OpenDialog("data/orders?prefix=order%2F");

        Assert.Multiple(() =>
        {
            Assert.That(Control(cut, "What to export").QuerySelectorAll("option").Select(option => option.TextContent), Is.EqualTo(new[] { "The whole tree", "Keys with a prefix", "A key range" }));
            Assert.That(Control(cut, "What to export").QuerySelector("option[selected]")!.TextContent, Is.EqualTo("Keys with a prefix"));
            Assert.That(Control(cut, "Prefix").GetAttribute("value"), Is.EqualTo("order/"));
            Assert.That(Control(cut, "Format").QuerySelectorAll("option").Select(option => option.TextContent), Is.EqualTo(new[] { "NDJSON (one JSON object per line)", "CSV (key, value, encoding)" }));
        });
    }

    [Test]
    public void An_empty_prefix_and_a_reversed_range_are_refused_before_anything_is_prepared()
    {
        Client.WithTree("orders", keys: 3);
        var cut = OpenDialog("data/orders");

        Control(cut, "What to export").Change(nameof(DataExportScopeKind.Prefix));
        cut.Find(".lt-data-export__form").Submit();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-export__error").TextContent, Is.EqualTo("Enter the prefix the keys start with.")));

        Control(cut, "What to export").Change(nameof(DataExportScopeKind.Range));
        Control(cut, "From key").Input("order/9");
        Control(cut, "Up to key").Input("order/1");
        cut.Find(".lt-data-export__form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-export__error").TextContent, Does.StartWith("The range ends before it starts")));
        Assert.That(Services.GetRequiredService<ExplorerDownloadTicketStore>().Outstanding, Is.Zero);
    }

    [Test]
    public void Preparing_renders_a_form_that_posts_the_ticket_and_this_circuits_binding()
    {
        Client.WithTree("orders", keys: 3);
        var cut = OpenDialog("data/orders");

        Control(cut, "Format").Change(nameof(DataExportFormat.Csv));
        cut.Find(".lt-data-export__form").Submit();

        cut.WaitUntil(() =>
        {
            var form = cut.Find("form.lt-data-export__download");
            Assert.That(form.GetAttribute("method"), Is.EqualTo("post"));
            Assert.That(form.GetAttribute("action"), Is.EqualTo(ExplorerDownloads.FormAction));
            Assert.That(form.GetAttribute("data-enhance"), Is.EqualTo("false"));
            Assert.That(form.QuerySelector($"input[name={ExplorerDownloads.BindingField}]")!.GetAttribute("value"), Is.EqualTo(Services.GetRequiredService<ExplorerDownloads>().Binding));
            Assert.That(form.QuerySelector($"input[name={ExplorerDownloads.TokenField}]")!.GetAttribute("value"), Is.Not.Empty);
            Assert.That(cut.Find(".lt-data-export").GetAttribute("data-lt-export-state"), Is.EqualTo("ready"));
            Assert.That(cut.Markup, Does.Contain("orders.csv"));
        });
        Assert.That(_sources.Prepared.Single().Address, Is.Not.Empty, "the download runs over the captured connection");
    }

    [Test]
    public async Task A_finished_complete_export_reports_how_many_entries_it_holds()
    {
        Client.WithTree("orders", keys: 3);
        _sources.Source.With("order/1", "{\"total\":1}"u8.ToArray()).With("order/2", "{\"total\":2}"u8.ToArray());
        var cut = OpenDialog("data/orders");
        cut.Find(".lt-data-export__form").Submit();

        await DownloadAsync(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-export").GetAttribute("data-lt-export-state"), Is.EqualTo("finished"));
            Assert.That(cut.Find(".lt-data-export__result .lt-pill").TextContent.Trim(), Is.EqualTo("Complete"));
            Assert.That(cut.Find(".lt-data-export__result p").TextContent, Is.EqualTo("Exported 2 entries. Every key in the scope is in the file."));
        });
    }

    [Test]
    public async Task A_narrowed_export_says_the_file_is_not_complete_and_why()
    {
        Client.WithTree("orders", keys: 3);
        _sources.Source.With("order/1", "1"u8.ToArray());
        _sources.Source.Coverage = LatticeRangeReadGateCoverage.Filtered;
        var cut = OpenDialog("data/orders");
        cut.Find(".lt-data-export__form").Submit();

        await DownloadAsync(cut);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-export__result .lt-pill").TextContent.Trim(), Is.EqualTo("Not complete"));
            Assert.That(cut.Find(".lt-data-export__result p").TextContent, Does.StartWith("Exported 1 entry, but the file is not complete. Your access covers only part of this scope"));
        });
    }

    [Test]
    public void A_ticket_presented_from_another_circuit_is_reported_as_refused()
    {
        Client.WithTree("orders", keys: 3);
        var cut = OpenDialog("data/orders");
        cut.Find(".lt-data-export__form").Submit();
        cut.WaitUntil(() => cut.Find("form.lt-data-export__download"));

        var token = cut.Find($"input[name={ExplorerDownloads.TokenField}]").GetAttribute("value");
        Services.GetRequiredService<ExplorerDownloadTicketStore>().Redeem(token, "another-circuit", out _);

        cut.WaitUntil(() =>
        {
            Assert.That(cut.Find(".lt-data-export").GetAttribute("data-lt-export-state"), Is.EqualTo("refused"));
            Assert.That(cut.Find(".lt-data-export__error").TextContent, Does.Contain("not started from this tab"));
        });

        Button(cut, "Prepare again").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-export__form"), Has.Count.EqualTo(1)));
    }

    [Test]
    public void Cancelling_a_prepared_download_withdraws_its_ticket()
    {
        Client.WithTree("orders", keys: 3);
        var cut = OpenDialog("data/orders");
        cut.Find(".lt-data-export__form").Submit();
        cut.WaitUntil(() => cut.Find("form.lt-data-export__download"));
        var token = cut.Find($"input[name={ExplorerDownloads.TokenField}]").GetAttribute("value");

        Button(cut, "Cancel").Click();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-export").GetAttribute("data-lt-export-state"), Is.EqualTo("choose")));
        Assert.That(Services.GetRequiredService<ExplorerDownloadTicketStore>().Redeem(token, Services.GetRequiredService<ExplorerDownloads>().Binding, out _), Is.EqualTo(ExplorerDownloadRedemption.Unknown));
    }

    [Test]
    public void The_dialog_texts_name_the_scope_progress_and_size()
    {
        var store = new ExplorerDownloadTicketStore();
        var ticket = store.Mint(new DataExportDownload(new DataExportRequest { TreeId = "t", TreeName = "t" }, () => new FakeDataExportSource()), "b");
        ticket.Report(1, 2048);

        Assert.Multiple(() =>
        {
            Assert.That(DataExportDialog.ScopeText(new DataExportRequest { TreeId = "t", TreeName = "t" }), Is.EqualTo("The whole tree"));
            Assert.That(DataExportDialog.ScopeText(new DataExportRequest { TreeId = "t", TreeName = "t", Scope = DataExportScopeKind.Prefix, Prefix = "a/" }), Is.EqualTo("Keys starting with a/"));
            Assert.That(DataExportDialog.ScopeText(new DataExportRequest { TreeId = "t", TreeName = "t", Scope = DataExportScopeKind.Range, From = "a", To = "b" }), Is.EqualTo("From a up to b"));
            Assert.That(DataExportDialog.ScopeText(new DataExportRequest { TreeId = "t", TreeName = "t", Scope = DataExportScopeKind.Range, From = "a" }), Is.EqualTo("From a to the last key"));
            Assert.That(DataExportDialog.ScopeText(new DataExportRequest { TreeId = "t", TreeName = "t", Scope = DataExportScopeKind.Range, To = "b" }), Is.EqualTo("From the first key up to b"));
            Assert.That(DataExportDialog.ProgressText(ticket), Is.EqualTo("1 entry, 2.0 KB written"));
            Assert.That(DataExportDialog.Size(10), Is.EqualTo("10 bytes"));
            Assert.That(DataExportDialog.Size(5L * 1024 * 1024), Is.EqualTo("5.0 MB"));
            Assert.That(DataExportDialog.Size(3L * 1024 * 1024 * 1024), Is.EqualTo("3.00 GB"));
            Assert.That(DataExportDialog.RefusalText(ExplorerDownloadState.Expired), Does.Contain("expired"));
        });
    }

    private IRenderedComponent<DataPageHost> OpenDialog(string relative)
    {
        var cut = RenderAt(relative);
        cut.WaitUntil(() => Assert.That(cut.FindAll(ExportControl), Has.Count.EqualTo(1)));
        cut.Find(ExportControl).Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-data-export__form"), Has.Count.EqualTo(1)));
        return cut;
    }

    private async Task DownloadAsync(IRenderedComponent<DataPageHost> cut)
    {
        cut.WaitUntil(() => cut.Find("form.lt-data-export__download"));
        var token = cut.Find($"input[name={ExplorerDownloads.TokenField}]").GetAttribute("value");
        var binding = cut.Find($"input[name={ExplorerDownloads.BindingField}]").GetAttribute("value");
        Assert.That(Services.GetRequiredService<ExplorerDownloadTicketStore>().Redeem(token, binding, out var ticket), Is.EqualTo(ExplorerDownloadRedemption.Redeemed));
        using var body = new MemoryStream();
        await ticket!.Download.RunAsync(body, ticket, CancellationToken.None);
    }
}
