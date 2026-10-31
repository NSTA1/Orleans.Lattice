using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A3;

/// <summary>
/// Journey A3 (#5374), provisioning the factory. olivia creates <c>factory-orders</c> with four
/// shards and full-value history, seeds it from the October orders export, refuses an unsorted
/// seed into <c>factory-scratch</c> and deletes it, creates <c>factory-lines</c> and seeds it by
/// pasting twelve lines into the Bulk load panel (resuming after a dropped append), gives the
/// orders the stable name <c>orders</c>, sets the telemetry's retention and the orders' publish
/// events, builds the <c>orders-by-status</c> view, reads a digest and triggers a compaction,
/// and, once the line feed is attached, watches the orders move and reconciles the line
/// feed's <c>order-status</c> tag index.
/// </summary>
/// <remarks>
/// Every step is driven through the Explorer. The harness acts only as the external actors the
/// issue names: it drops one proxied bulk-load append (step 4) and attaches the line feed's
/// orders (step 9). Its other calls read the cluster to assert what the UI did.
/// </remarks>
[Journey(A3Steps.JourneyId)]
[JourneyChapter("A3", After = new[] { "A2" })]
public sealed class ProvisioningTheFactoryJourney : JourneyChapter
{
    private const string OrdersKeyTemplate = "order/{/id}";

    /// <summary>The unsorted seed file of step 3: the first ten orders, with orders 4 and 5 swapped.</summary>
    private const string UnsortedFile = "orders-unsorted.ndjson";

    /// <summary>The line of <see cref="UnsortedFile"/> the seed's pre-flight refuses.</summary>
    private const int UnsortedLine = 5;

    /// <summary>The oversize file of step 2, one MiB over Contoso's import limit.</summary>
    private const string OversizeFile = "orders-oversize.ndjson";

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;
        var files = Path.Combine(context.ArtifactsDirectory, "a3-files");
        var ordersFile = StoryData.WriteFile(files, Names.Files.OrdersNdjson);
        var unsortedFile = WriteUnsortedFile(files);
        var oversizeFile = WriteOversizeFile(files);

        await using var olivia = await estate.SignInAsAsync(Personas.Olivia);
        var shell = olivia.Shell;
        var page = olivia.Page;
        var steps = new A3Steps(olivia);
        var cluster = new ClusterPages(shell);
        var data = new DataPages(shell);
        var facades = ContractFacades.Of(context);

        // 1. New tree factory-orders, 4 shards; land on Summary; full-value history before any order is written.
        await steps.StepAsync(1, async () =>
        {
            await cluster.OpenOverviewAsync();
            var create = await cluster.OpenTreesAsync();
            await steps.SubstepAsync("affordance", "The Cluster tree list offers New tree...");

            await cluster.FillNewTreeAsync(create, Names.Trees.FactoryOrders, A3Contract.OrdersShardCount);
            await steps.SubstepAsync("form", "New tree: factory-orders with 4 physical shards");

            await ClusterPages.ReviewAsync(cluster.NewTreeDialog, Names.Trees.FactoryOrders, A3Contract.OrdersShardCount);
            await steps.SubstepAsync("review", "The review names the tree and its sizing");

            await cluster.CreateAndLandOnSummaryAsync(Names.Trees.FactoryOrders, A3Contract.OrdersShardCount);
            Coverage.Hit("use-case:cluster/create-tree");
            await steps.SubstepAsync("summary", "factory-orders is created and the page lands on its Summary");

            await shell.Content.Locator("[data-lt-tree-created-next=configuration]").ClickAsync();
            await A3Ui.ExpectAddressAsync(page, "tab=configuration");
            await Expect(shell.Content.GetByRole(AriaRole.Tab, new() { Name = "Configuration", Selected = true })).ToHaveCountAsync(1);
            await cluster.FillRetentionAsync("Full value", days: null);
            await steps.SubstepAsync("retention-form", "Configuration: full-value history, chosen before any order is written");

            await cluster.SaveRetentionAsync();
            await Expect(cluster.Term("Mode")).ToHaveTextAsync("Full value: each revision keeps its bytes");
            Coverage.Hit("query:cluster.tab");
            Coverage.Hit("tab:cluster/configuration");
            await steps.SubstepAsync("retention-saved", "factory-orders keeps every revision's value");
        });

        // 2. Import the October orders into factory-orders as a seed.
        await steps.StepAsync(2, async () =>
        {
            await data.OpenDirectoryAsync();
            await Expect(shell.Content).ToContainTextAsync(Names.Trees.FactoryOrders);
            await data.OpenTreeAsync(Names.Trees.FactoryOrders);
            await Expect(data.ImportButton).ToHaveTextAsync("Import...");
            await steps.SubstepAsync("affordance", "The empty factory-orders workspace offers Import...");

            await data.ImportButton.ClickAsync();
            var import = data.ImportDialog(Names.Trees.FactoryOrders);
            await DataPages.ChooseSeedFileAsync(import, oversizeFile, OrdersKeyTemplate);
            await import.GetByRole(AriaRole.Button, new() { Name = "Seed tree", Exact = true }).ClickAsync();
            await Expect(import.Locator(".lt-data-import__error")).ToContainTextAsync(
                new Regex("^The file is 65\\.0 MB; the largest file this Explorer imports is 64\\.0 MB\\.$"));
            await Expect(import.Locator(".lt-data-import")).ToHaveAttributeAsync("data-lt-import-state", "choose");
            Coverage.Hit("option:MaxImportBytes");
            await steps.SubstepAsync("oversize-refused", "A file over the head's 64 MiB import limit is refused before anything is read");

            await import.Locator("#data-import-file").SetInputFilesAsync(ordersFile);
            await import.GetByLabel("Key", new() { Exact = true }).FillAsync(OrdersKeyTemplate);
            await Expect(import.Locator(".lt-data-import__error")).ToHaveCountAsync(0);
            await steps.SubstepAsync("file-chosen", "orders-2026-10.ndjson, Seed an empty tree, key order/{/id}, the whole line as the value");

            await DataPages.SeedAsync(import, "succeeded");
            await Expect(import.Locator("[data-lt-import-committed]")).ToHaveAttributeAsync("data-lt-import-committed", Names.Orders.SeededCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
            await Expect(import.Locator(".lt-data-import__result")).ToContainTextAsync($"Seeded {Names.Orders.SeededCount} entries.");
            Coverage.Hit("command:data.import");
            await steps.SubstepAsync("seeded", "The seed ran to 240 entries");

            await import.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true }).ClickAsync();
            await Expect(import).ToBeHiddenAsync();
            await Expect(data.Rows.First).ToContainTextAsync(Names.Orders.Key(1), new() { Timeout = 30_000 });
            var stored = await ContractCalls.GetAsync(facades.Data, Names.Trees.FactoryOrders, Names.Orders.Key(240));
            Assert.That(stored is null ? null : Encoding.UTF8.GetString(stored), Is.EqualTo(StoryData.Order(240).Json), "each line is stored verbatim");
            Coverage.Hit("use-case:data/import-ndjson-seed");
            await steps.SubstepAsync("keys", "factory-orders lists the seeded orders");
        });

        // 3. An unsorted seed into a new factory-scratch is refused with its line; then delete factory-scratch.
        await steps.StepAsync(3, async () =>
        {
            await data.OpenDirectoryAsync();
            await Expect(data.NewTreeFromFile).ToHaveTextAsync("New tree from file...");
            await steps.SubstepAsync("affordance", "The Data directory offers New tree from file...");

            await data.NewTreeFromFile.ClickAsync();
            var create = page.GetByRole(AriaRole.Dialog, new() { Name = "New tree from file" });
            await create.GetByLabel("Tree name").FillAsync(Names.Trees.FactoryScratch);
            await create.GetByLabel("Physical shards").FillAsync("2");
            await ClusterPages.ReviewAsync(create, Names.Trees.FactoryScratch, 2);
            await steps.SubstepAsync("review", "New tree from file: factory-scratch, reviewed");

            await create.GetByRole(AriaRole.Button, new() { Name = "Create tree", Exact = true }).ClickAsync();
            var import = data.ImportDialog(Names.Trees.FactoryScratch);
            await Expect(import).ToBeVisibleAsync(new() { Timeout = 30_000 });
            await A3Ui.ExpectAddressAsync(page, "import=seed");
            await Expect(import.GetByLabel("Mode", new() { Exact = true })).ToHaveValueAsync("Seed");
            Coverage.Hit("query:data.import");
            Coverage.Hit("query-value:data.import=seed");
            await DataPages.ChooseSeedFileAsync(import, unsortedFile, OrdersKeyTemplate);
            await steps.SubstepAsync("file-chosen", "The new tree's Import dialog opens on a seed; an unsorted file is chosen");

            await DataPages.SeedAsync(import, "failed");
            await Expect(import.Locator(".lt-data-import__result")).ToContainTextAsync(
                $"Line {UnsortedLine}: the key \"{Names.Orders.Key(4)}\" does not sort after \"{Names.Orders.Key(5)}\" on line {UnsortedLine - 1}.");
            await Expect(import.Locator("[data-lt-import-committed]")).ToHaveAttributeAsync("data-lt-import-committed", "0");
            Assert.That(await ContractCalls.CountPrefixAsync(facades.Data, Names.Trees.FactoryScratch, "order/"), Is.Zero, "a refused seed writes nothing");
            Coverage.Hit("use-case:data/import-out-of-order");
            await steps.SubstepAsync("refused", "Pre-flight refuses the seed at line 5, and nothing is written");

            await import.GetByRole(AriaRole.Button, new() { Name = "Done", Exact = true }).ClickAsync();
            await Expect(import).ToBeHiddenAsync();

            await cluster.OpenTreeAsync(Names.Trees.FactoryScratch, "lifecycle");
            await shell.Content.Locator("[data-lt-cluster=delete]").ClickAsync();
            var confirm = await A3Ui.TypeConfirmationAsync(page, "Delete tree", Names.Trees.FactoryScratch);
            await steps.SubstepAsync("delete-confirm", "Lifecycle: Delete tree, with the tree's name typed");

            await A3Ui.ConfirmAsync(confirm, "Delete");
            await A3Ui.ExpectToastAsync(shell, "Tree deleted. It can be recovered until its window closes.");
            await Expect(cluster.Term("State")).ToHaveTextAsync("Deleted, recoverable");
            Coverage.Hit("tab:cluster/lifecycle");
            await steps.SubstepAsync("deleted", "factory-scratch is deleted");
        });

        // 4. New tree factory-lines; paste its twelve lines into Bulk load, back once, drop one append, resume.
        await steps.StepAsync(4, async () =>
        {
            var create = await cluster.OpenTreesAsync();
            await cluster.FillNewTreeAsync(create, Names.Trees.FactoryLines, shards: null);
            await ClusterPages.ReviewAsync(cluster.NewTreeDialog, Names.Trees.FactoryLines, 64);
            await cluster.CreateAndLandOnSummaryAsync(Names.Trees.FactoryLines, 64);
            await steps.SubstepAsync("created", "factory-lines is created with the defaults; its Summary offers a bulk load");

            await shell.Content.Locator("[data-lt-tree-created-next-steps]").GetByRole(AriaRole.Link, new() { Name = "Seed it with a bulk load" }).ClickAsync();
            await Expect(shell.Heading).ToHaveTextAsync($"Admin tools {Names.Trees.FactoryLines}", new() { Timeout = 30_000 });
            Coverage.Hit("subroute:cluster/trees/{tree}/tools");
            await cluster.BulkInput.FillAsync(StoryData.LinesPaste);
            await steps.SubstepAsync("compose", "Bulk load: twelve key=value lines with JSON values, pasted");

            var bulk = cluster.BulkSection;
            await bulk.GetByRole(AriaRole.Button, new() { Name = "Review", Exact = true }).ClickAsync();
            await Expect(bulk).ToContainTextAsync($"Load 12 entries into {Names.Trees.FactoryLines}, from line/01 to line/12, in 1 chunk.");
            await steps.SubstepAsync("review", "The review names the entries, their range and the chunks");

            await bulk.GetByRole(AriaRole.Button, new() { Name = "Back", Exact = true }).ClickAsync();
            await Expect(cluster.BulkInput).ToHaveValueAsync(StoryData.LinesPaste);
            await steps.SubstepAsync("back", "Back returns to Compose with the lines kept");

            await bulk.GetByRole(AriaRole.Button, new() { Name = "Review", Exact = true }).ClickAsync();
            await bulk.GetByRole(AriaRole.Button, new() { Name = "Bulk load...", Exact = true }).ClickAsync();
            var confirm = await A3Ui.TypeConfirmationAsync(page, "Bulk load", Names.Trees.FactoryLines);
            await steps.SubstepAsync("confirm", "Bulk load, confirmed with the tree's name");

            // The harness drops the next proxied append, as a lost connection would.
            await estate.FailNextGatewayCallAsync("/orleans.lattice.api.treeadmin/AppendBulkLoad");
            await A3Ui.ConfirmAsync(confirm, "Bulk load");
            await Expect(bulk.GetByRole(AriaRole.Alert)).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await Expect(bulk).ToContainTextAsync("0 chunks of 1 acknowledged.");
            await Expect(bulk.GetByRole(AriaRole.Button, new() { Name = "Resume", Exact = true })).ToBeVisibleAsync();
            await steps.SubstepAsync("dropped", "The dropped append stops the load with nothing acknowledged, and offers Resume");

            await bulk.GetByRole(AriaRole.Button, new() { Name = "Resume", Exact = true }).ClickAsync();
            await Expect(bulk.GetByRole(AriaRole.Status)).ToHaveTextAsync("Bulk load committed: the tree holds 12 live keys.", new() { Timeout = 60_000 });
            Assert.That(await ContractCalls.CountPrefixAsync(facades.Data, Names.Trees.FactoryLines, "line/"), Is.EqualTo(Names.Lines.Count));
            Coverage.Hit("use-case:cluster/bulk-load");
            await steps.SubstepAsync("committed", "Resume completes the load: factory-lines holds its 12 lines");
        });

        // 5. Lifecycle: give factory-orders the stable name orders; /data/orders reaches it.
        await steps.StepAsync(5, async () =>
        {
            await cluster.OpenTreeAsync(Names.Trees.FactoryOrders, "lifecycle");
            await cluster.AddNameForm.GetByLabel("New name", new() { Exact = true }).FillAsync(Names.Aliases.Orders);
            await steps.SubstepAsync("affordance", "Lifecycle: Add name... names the alias orders for factory-orders");

            await cluster.AddNameForm.GetByRole(AriaRole.Button, new() { Name = "Add name...", Exact = true }).ClickAsync();
            var confirm = await A3Ui.TypeConfirmationAsync(page, "Add a name", Names.Aliases.Orders);
            await Expect(confirm).ToContainTextAsync($"Reads and writes routed through {Names.Aliases.Orders} will reach {Names.Trees.FactoryOrders}.");
            await steps.SubstepAsync("confirm", "The confirmation states what the alias reaches");

            await A3Ui.ConfirmAsync(confirm, "Add name");
            await A3Ui.ExpectToastAsync(shell, $"{Names.Aliases.Orders} now reaches {Names.Trees.FactoryOrders}.");
            var alias = await facades.TreeAdmin.ResolveTreeAliasAsync(Names.Aliases.Orders);
            Assert.That(alias.PhysicalTreeId, Is.EqualTo(Names.Trees.FactoryOrders));
            await steps.SubstepAsync("named", "orders now reaches factory-orders");

            await data.OpenTreeAsync(Names.Aliases.Orders);
            await Expect(data.Rows.First).ToContainTextAsync(Names.Orders.Key(1), new() { Timeout = 30_000 });
            Coverage.Hit("use-case:cluster/alias");
            Coverage.Hit("use-case:cluster/alias-name");
            await steps.SubstepAsync("data-through-alias", "/data/orders browses the orders of factory-orders");
        });

        // 6. Configuration: factory-telemetry retention 7 days; factory-orders publishes events.
        await steps.StepAsync(6, async () =>
        {
            await cluster.OpenTreeAsync(Names.Trees.FactoryTelemetry, "configuration");
            await cluster.FillRetentionAsync("Default (metadata only)", days: 7);
            await steps.SubstepAsync("telemetry-retention", "factory-telemetry: a 7-day history window, mode left at its default");

            await cluster.SaveRetentionAsync();
            await Expect(cluster.Term("Window")).ToHaveTextAsync("7 days");
            await steps.SubstepAsync("telemetry-retention-saved", "factory-telemetry keeps 7 days of history");

            await cluster.OpenTreeAsync(Names.Trees.FactoryOrders, "configuration");
            await cluster.ChooseConfigurationAsync("Publish events", "On");
            await steps.SubstepAsync("orders-configuration", "factory-orders: publish events On");

            await cluster.SaveConfigurationAsync();
            await Expect(cluster.Term("Publish events")).ToHaveTextAsync("On");
            await page.ReloadAsync();
            await Expect(cluster.Term("Publish events")).ToHaveTextAsync("On", new() { Timeout = 30_000 });
            await Expect(cluster.Term("Mode")).ToHaveTextAsync("Full value: each revision keeps its bytes");
            Coverage.Hit("use-case:cluster/configuration-and-retention");
            await steps.SubstepAsync("orders-configuration-saved", "Read again, Configuration shows publish events On and full-value history");
        });

        // 7. Data views: New view orders-by-status from the order-status provider; it is Current and browsable.
        await steps.StepAsync(7, async () =>
        {
            await data.OpenTreeAsync(Names.Trees.FactoryOrders, "views");
            await Expect(data.NewViewButton).ToHaveTextAsync("New view...");
            await steps.SubstepAsync("affordance", "factory-orders' Views tab offers New view...");

            await data.NewViewButton.ClickAsync();
            await Expect(data.NewViewDialog).ToBeVisibleAsync();
            await A3Ui.ExpectAddressAsync(page, "new=view");
            Coverage.Hit("query:data.new");
            await Expect(data.NewViewDialog.Locator("select[data-lt-view-provider]")).ToHaveValueAsync(A3Contract.ViewProviderKey);
            await data.NewViewDialog.GetByLabel("View name", new() { Exact = true }).FillAsync(Names.Views.OrdersByStatus);
            await steps.SubstepAsync("form", "New view: the order-status provider, named orders-by-status, its field left empty");

            await data.NewViewDialog.GetByRole(AriaRole.Button, new() { Name = "Create view", Exact = true }).ClickAsync();
            await A3Ui.ExpectToastAsync(shell, $"Created view {Names.Views.OrdersByStatus}.");
            await Expect(data.NewViewDialog).ToBeHiddenAsync();
            await data.WaitForCurrentAsync(Names.Views.OrdersByStatus);
            Coverage.Hit("command:data.create-view");
            await steps.SubstepAsync("current", "orders-by-status is Current");

            await data.ViewRow(Names.Views.OrdersByStatus).GetByRole(AriaRole.Link, new() { Name = Names.Views.OrdersByStatus }).ClickAsync();
            await Expect(shell.Heading).ToHaveTextAsync(Names.Views.OrdersByStatus, new() { Timeout = 30_000 });
            var first = data.Rows.First;
            await Expect(first).ToContainTextAsync(Names.Orders.Key(1), new() { Timeout = 30_000 });
            await Expect(first).ToContainTextAsync(StoryData.SeededStatus(1));
            Coverage.Hit("use-case:data/view-create");
            await steps.SubstepAsync("projection", "The view's projection is browsable: each order's status");
        });

        // 8. Cluster tools: digest of shard 0 and compaction of shard 1; the tree tabs render the new trees.
        await steps.StepAsync(8, async () =>
        {
            await cluster.OpenToolsAsync(Names.Trees.FactoryOrders);
            await cluster.DigestSection.GetByLabel("Shard", new() { Exact = true }).FillAsync("0");
            await cluster.DigestSection.GetByRole(AriaRole.Button, new() { Name = "Read digest", Exact = true }).ClickAsync();
            await Expect(A3Ui.Term(cluster.DigestSection, "Shard")).ToHaveTextAsync("0");
            await Expect(A3Ui.Term(cluster.DigestSection, "Hash")).ToHaveTextAsync(new Regex("^[0-9a-f]{16,}$"));
            Coverage.Hit("use-case:cluster/digest");
            await steps.SubstepAsync("digest", "Read digest: shard 0's content hash, entries and checkpoint");

            await cluster.CompactionForm.GetByLabel("Shard", new() { Exact = true }).FillAsync("1");
            await cluster.CompactionForm.GetByRole(AriaRole.Button, new() { Name = "Compact...", Exact = true }).ClickAsync();
            var confirm = await A3Ui.TypeConfirmationAsync(page, "Trigger compaction", Names.Trees.FactoryOrders);
            await steps.SubstepAsync("compaction-confirm", "Trigger compaction on shard 1, confirmed with the tree's name");

            await A3Ui.ConfirmAsync(confirm, "Compact");
            await Expect(cluster.CompactionSection.GetByRole(AriaRole.Status)).ToContainTextAsync("shard 1", new() { Timeout = 30_000 });
            Coverage.Hit("use-case:cluster/compaction");
            await steps.SubstepAsync("compaction", "The cluster answers the compaction of shard 1");

            await cluster.OpenTreeAsync(Names.Trees.FactoryOrders);
            await Expect(cluster.Term("Physical shards")).ToHaveTextAsync("4", new() { Timeout = 30_000 });
            await steps.SubstepAsync("orders-summary", "factory-orders' Summary");

            await cluster.OpenTreeAsync(Names.Trees.FactoryOrders, "shards");
            await Expect(cluster.Term("Physical shards")).ToHaveTextAsync("4", new() { Timeout = 30_000 });
            Coverage.Hit("tab:cluster/shards");
            await steps.SubstepAsync("orders-shards", "factory-orders' Shards");

            await cluster.OpenTreeAsync(Names.Trees.FactoryOrders, "storage");
            await Expect(cluster.Term("Total")).ToBeVisibleAsync(new() { Timeout = 30_000 });
            Coverage.Hit("tab:cluster/storage");
            await steps.SubstepAsync("orders-storage", "factory-orders' Storage");

            await cluster.OpenTreeAsync(Names.Trees.FactoryLines);
            await Expect(cluster.Term("Physical shards")).ToHaveTextAsync("64", new() { Timeout = 30_000 });
            await steps.SubstepAsync("lines-summary", "factory-lines' Summary");

            await cluster.OpenTreeAsync(Names.Trees.FactoryLines, "configuration");
            await Expect(cluster.Term("Physical shards")).ToHaveTextAsync("64", new() { Timeout = 30_000 });
            await steps.SubstepAsync("lines-configuration", "factory-lines' Configuration");
        });

        // 9. Off-UI the line feed attaches its orders; the keys panel shows them move; reconcile order-status.
        await steps.StepAsync(9, async () =>
        {
            var feed = estate.LineFeed;
            await feed.AttachOrdersAsync();
            await feed.WaitUntilAsync(status => status.OrdersWritten >= 3, TimeSpan.FromMinutes(2));

            await data.OpenTreeAsync(Names.Trees.FactoryOrders, query: "prefix=order/1000");
            var firstFed = data.Rows.Filter(new() { HasText = "order/100001" });
            await Expect(firstFed).ToContainTextAsync("\"status\":\"shipped\"", new() { Timeout = 60_000 });
            await Expect(data.Rows.Filter(new() { HasText = "\"status\":\"open\"" }).First).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await steps.SubstepAsync("status-transitions", "The line feed's orders arrive open and move to shipped");

            await data.OpenTreeAsync(Names.Trees.FactoryOrders, "tag-indexes");
            var index = shell.Content.GetByRole(AriaRole.Link, new() { Name = Names.TagIndexes.OrderStatus, Exact = true }).First;
            await Expect(index).ToBeVisibleAsync(new() { Timeout = 60_000 });
            await steps.SubstepAsync("indexes", "Tag indexes lists the line feed's order-status");

            await index.ClickAsync();
            var section = shell.Content.Locator("section").Filter(new() { Has = page.Locator("h2", new() { HasText = $"Tag index {Names.TagIndexes.OrderStatus}" }) });
            await Expect(section.Locator("ul.lt-data-list")).ToContainTextAsync(Names.Trees.FactoryOrders, new() { Timeout = 30_000 });
            var reconcile = section.GetByRole(AriaRole.Button, new() { Name = "Reconcile", Exact = true });
            await Expect(reconcile).ToBeEnabledAsync();
            await steps.SubstepAsync("index-selected", "order-status covers factory-orders, and offers Reconcile");

            await reconcile.ClickAsync();
            var confirm = await A3Ui.TypeConfirmationAsync(page, "Reconcile this tag index?", Names.TagIndexes.OrderStatus);
            await steps.SubstepAsync("reconcile-confirm", "Reconcile, confirmed with the index's name");

            await A3Ui.ConfirmAsync(confirm, "Reconcile");
            await Expect(data.Operation.Locator(".lt-operation-progress")).ToHaveAttributeAsync("data-lt-operation-state", "Succeeded", new() { Timeout = 180_000 });
            await Expect(data.Operation.Locator("h2")).ToContainTextAsync($"Reconcile of {Names.TagIndexes.OrderStatus}");
            Coverage.Hit("use-case:data/tag-index-reconcile");
            await steps.SubstepAsync("reconciled", "The reconcile of order-status succeeded");
        });
    }

    private static string WriteUnsortedFile(string directory)
    {
        Directory.CreateDirectory(directory);
        var orders = Enumerable.Range(1, 10).Select(StoryData.Order).ToList();
        (orders[3], orders[4]) = (orders[4], orders[3]);
        var path = Path.Combine(directory, UnsortedFile);
        File.WriteAllText(path, string.Concat(orders.Select(order => order.Json + "\n")), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        return path;
    }

    // One MiB over Contoso's import limit. Only its size is read before the refusal, so the
    // file is extended rather than written.
    private static string WriteOversizeFile(string directory)
    {
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, OversizeFile);
        using var file = new FileStream(path, FileMode.Create, FileAccess.Write);
        file.SetLength(EstateProfile.ContosoMaxImportBytes + (1024 * 1024));
        return path;
    }
}
