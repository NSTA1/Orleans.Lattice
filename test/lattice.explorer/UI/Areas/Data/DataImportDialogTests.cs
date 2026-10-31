using Bunit;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Data;

/// <summary>
/// The Import action on a tree page (issue #5365): drawn, and offered by the palette, only
/// for a caller who may write the tree or holds its bulk-load grant, with only the modes
/// that caller may use; an upsert streams the chosen file and lists every refused line; a
/// <c>?import=seed</c> address opens the dialog on a seed.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class DataImportDialogTests : DataTestContext
{
    private const string Tree = "orders";

    [Test]
    public void A_caller_who_may_neither_write_nor_bulk_load_sees_no_import_and_the_palette_offers_none()
    {
        Client.WithTree(Tree, keys: 1);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Assert.That(Writer.Probes, Does.Contain(Tree)));

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll($"[data-lt-command='{DataArea.ImportCommandId}']"), Is.Empty);
            Assert.That(Area.Commands.Select(command => command.Id), Has.None.EqualTo(DataArea.ImportCommandId));
        });
    }

    [Test]
    public void A_writer_sees_import_and_the_palette_offers_it_where_its_control_is_drawn()
    {
        Client.WithTree(Tree, keys: 1);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "Import..."));

        ExplorerCommandControls.AssertVisibleControl(cut, Area.Commands.Single(command => command.Id == DataArea.ImportCommandId));
        Button(cut, "Import...").Click();

        var modes = Control(cut, "Mode").QuerySelectorAll("option").Select(option => option.TextContent.Trim());
        Assert.That(modes, Is.EqualTo(new[] { "Add or replace entries" }), "a seed needs the bulk-load grant this caller lacks");
    }

    [Test]
    public void A_bulk_loader_without_write_is_offered_only_the_seed()
    {
        Client.WithTree(Tree, keys: 1);
        GrantBulkLoad();
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "Import..."));
        Button(cut, "Import...").Click();

        var modes = Control(cut, "Mode").QuerySelectorAll("option").Select(option => option.TextContent.Trim());
        Assert.Multiple(() =>
        {
            Assert.That(modes, Is.EqualTo(new[] { "Seed an empty tree (bulk load)" }));
            Assert.That(Button(cut, "Seed tree"), Is.Not.Null);
        });
    }

    [Test]
    public void An_import_seed_address_opens_the_dialog_on_a_seed()
    {
        Client.WithTree(Tree, keys: 0);
        Writer.TreeRights[Tree] = (true, true);
        GrantBulkLoad();
        var cut = RenderAt("data/orders?import=seed");

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-import").GetAttribute("data-lt-import-state"), Is.EqualTo("choose")));
        Assert.That(SelectedText(cut, "Mode"), Is.EqualTo("Seed an empty tree (bulk load)"));
    }

    [Test]
    public void An_upsert_streams_the_file_and_lists_each_refused_line()
    {
        Client.WithTree(Tree, keys: 0);
        Writer.TreeRights[Tree] = (true, true);
        Writer.RefusedKeys.Add("deny");
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "Import..."));
        Button(cut, "Import...").Click();

        cut.FindComponent<InputFile>().UploadFiles(InputFileContent.CreateFromText(
            "{\"id\":\"a\",\"n\":1}\n{\"id\":\"deny\"}\nnot json\n{\"id\":\"b\"}\n",
            "orders.ndjson"));
        cut.Find(".lt-data-import__form").Submit();

        cut.WaitUntil(() => Assert.That(cut.Find(".lt-data-import").GetAttribute("data-lt-import-state"), Is.EqualTo("succeeded")), TimeSpan.FromSeconds(10));
        var rows = cut.FindAll("[data-lt-import-refusals] tbody tr").Select(row => row.TextContent).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("[data-lt-import-committed]").GetAttribute("data-lt-import-committed"), Is.EqualTo("2"));
            Assert.That(cut.Find("[data-lt-import-refused]").GetAttribute("data-lt-import-refused"), Is.EqualTo("2"));
            Assert.That(rows, Has.Count.EqualTo(2));
            Assert.That(rows, Has.Some.Contains("deny").And.Some.Contains("2"));
            Assert.That(rows, Has.Some.Contains("not one JSON value"));
            Assert.That(Writer.Writes, Is.SupersetOf(new[] { "set:a", "set:b" }).And.None.EqualTo("set:deny"));
            Assert.That(Button(cut, "Prepare the list of refused lines"), Is.Not.Null);
        });
    }

    [Test]
    public void An_import_refuses_to_start_without_a_file_or_with_an_invalid_key()
    {
        Client.WithTree(Tree, keys: 0);
        Writer.TreeRights[Tree] = (true, true);
        var cut = RenderAt("data/orders");
        cut.WaitUntil(() => Button(cut, "Import..."));
        Button(cut, "Import...").Click();

        cut.Find(".lt-data-import__form").Submit();
        Assert.That(cut.Find(".lt-data-import__error").TextContent, Does.Contain("Choose a file"));

        cut.FindComponent<InputFile>().UploadFiles(InputFileContent.CreateFromText("{}\n", "orders.ndjson"));
        Control(cut, "Key").Input("id");
        cut.Find(".lt-data-import__form").Submit();
        Assert.That(cut.Find(".lt-data-import__error").TextContent, Does.Contain("JSON Pointer"));
    }

    [TestCase("rows.csv", DataImportFormat.Csv, ',')]
    [TestCase("rows.tsv", DataImportFormat.Csv, '\t')]
    [TestCase("rows.jsonl", DataImportFormat.Ndjson, ',')]
    [TestCase("rows.txt", DataImportFormat.KeyValue, ',')]
    public void A_file_name_suggests_its_format(string name, DataImportFormat format, char delimiter) =>
        Assert.That(DataImportDialog.FormatFor(name), Is.EqualTo((format, delimiter)));

    [Test]
    public void A_file_name_with_no_known_extension_suggests_nothing() =>
        Assert.That(DataImportDialog.FormatFor("rows.bin"), Is.Null);

    private void GrantBulkLoad() =>
        Admin.ProbeCapabilitiesAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(new LatticeTreeAdminCapabilities
            {
                TreeId = call.Arg<string>(),
                CanBulkLoad = true,
                Schema = new LatticeSchemaCapabilities { TreeId = call.Arg<string>() },
            }));

    private static string SelectedText(IRenderedComponent<DataPageHost> cut, string label) =>
        Control(cut, label).QuerySelectorAll("option").First(option => option.HasAttribute("selected")).TextContent.Trim();
}
