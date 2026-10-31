using System.Buffers.Binary;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>Steps 4 to 7: editing entries under the policy, binary and raw values, a refused edit, and an entry's whole life.</summary>
public sealed partial class DataStewardJourney
{
    /// <summary>The line priya edits in raw UTF-8 mode.</summary>
    private static readonly string RawLine = StoryData.LineKey(1);

    /// <summary>The owner priya writes into that line.</summary>
    private const string RawLineOwner = "line-1@contoso.example";

    /// <summary>The prefix whose live tail priya follows: every order the line feed opens.</summary>
    private const string LineFeedOrdersPrefix = "order/1";

    private async Task FixAnOrderAndResolveAConcurrentChangeAsync()
    {
        var fixedTotal = (-StoryData.SeededTotal(17)).ToString(System.Globalization.CultureInfo.InvariantCulture);
        await _tree.OpenEntryAsync(Names.Trees.FactoryOrders, Names.Orders.Order17);
        await Expect(_tree.EntryValue).ToContainTextAsync("-" + fixedTotal);
        await CheckpointAsync("4", "negative-order", "order/000017 holds a negative total");

        var editor = await OpenEditorAsync();
        await editor.FillAsync(U1Contract.ValidOrder17.Json);
        await Expect(editor.SchemaPasses).ToContainTextAsync("Passes this tree's schema policy");
        await CheckpointAsync("4", "schema-preview-passes", "The editor's schema preview passes the corrected order");

        await editor.SaveAndCloseAsync();
        await ExpectToastAsync($"Entry {Names.Orders.Order17} saved.");
        await Expect(_tree.EntryValue).ToContainTextAsync(fixedTotal);
        await Expect(_tree.EntryValue).Not.ToContainTextAsync("-" + fixedTotal);
        Coverage.Hit("command:data.edit-entry");
        Coverage.Hit("use-case:data/entry-edit-preview");
        await CheckpointAsync("4", "saved", "The save lands: order/000017 is valid");

        await _tree.HistoryOfKey.ClickAsync();
        await Expect(_tree.Tab("History")).ToHaveAttributeAsync("aria-selected", "true");
        var revisions = _priya.Shell.Content.Locator(".lt-data-timeline__rev");
        await Expect(revisions.Nth(1)).ToBeVisibleAsync(new() { Timeout = 30_000 });
        Coverage.Hit("tab:data/history");
        await CheckpointAsync("4", "history", "History shows both revisions of order/000017");

        // A concurrent change: the line feed ships order 50 while priya's editor is open.
        await _tree.OpenEntryAsync(Names.Trees.FactoryOrders, Names.Orders.Order50);
        editor = await OpenEditorAsync();
        await editor.FillAsync(StoryData.OrderJson(50, StoryData.SeededTotal(50) + 2m, StoryData.SeededStatus(50)));
        var shipped = StoryData.OrderJson(50, StoryData.SeededTotal(50), "shipped");
        await ContractFacades.Of(_context, Personas.LineFeed.Id).Data.SetAsync(new DataSetRequest
        {
            TreeId = Names.Trees.FactoryOrders,
            Key = Names.Orders.Order50,
            Value = Encoding.UTF8.GetBytes(shipped),
        });

        await editor.Submit.ClickAsync();
        await Expect(editor.Conflict).ToContainTextAsync("This entry changed since you opened the editor.");
        await Expect(editor.Conflict.Locator("pre")).ToContainTextAsync("shipped");
        await CheckpointAsync("4", "concurrent-change", "On save, the editor shows the line feed's newer value of order/000050");

        await editor.Conflict.GetByRole(AriaRole.Button, new() { Name = "Reload the newer value" }).ClickAsync();
        await Expect(editor.Conflict).ToHaveCountAsync(0);
        await Expect(editor.Value).ToHaveValueAsync(new Regex("shipped"));
        await CheckpointAsync("4", "reloaded", "Reload puts the newer value in the editor");

        await editor.FillAsync(StoryData.OrderJson(50, StoryData.SeededTotal(50) + 2m, "shipped"));
        await Expect(editor.SchemaPasses).ToBeVisibleAsync();
        await editor.SaveAndCloseAsync();
        await ExpectToastAsync($"Entry {Names.Orders.Order50} saved.");
        await Expect(_tree.EntryValue).ToContainTextAsync("shipped");
        Coverage.Hit("use-case:data/entry-edit-conflict");
        await CheckpointAsync("4", "saved-after-reload", "Edited from the newer value, the save lands");
    }

    private async Task EditBinaryAndRawValuesAsync()
    {
        await _priya.Shell.GotoAsync("/data/" + Names.Trees.FactoryTelemetry);
        await Expect(_tree.Heading).ToHaveTextAsync(Names.Trees.FactoryTelemetry);
        await _tree.WaitForKeysAsync();
        var reading = (await _tree.KeysAsync())[0];
        await _tree.OpenEntryAsync(Names.Trees.FactoryTelemetry, reading);
        await CheckpointAsync("5", "binary-reading", "A telemetry reading: eight bytes, a little-endian double");

        var editor = await OpenEditorAsync();
        await Expect(editor.EditAs).ToHaveValueAsync("Base64");
        await Expect(editor.Note).ToContainTextAsync("This value is binary, so it is shown as base64");
        var original = await editor.Value.InputValueAsync();
        Assert.That(Convert.FromBase64String(original.Trim()), Has.Length.EqualTo(sizeof(double)), "the reading's eight bytes, as base64");
        await CheckpointAsync("5", "binary-as-base64", "The editor shows the binary reading as base64, to be replaced whole");

        var replacement = new byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleLittleEndian(replacement, 21.5);
        await editor.FillAsync(Convert.ToBase64String(replacement));
        await editor.SaveAndCloseAsync();
        await ExpectToastAsync($"Entry {reading} saved.");
        await CheckpointAsync("5", "binary-replaced", "The reading is replaced");

        await _tree.OpenEntryAsync(Names.Trees.FactoryLines, RawLine);
        editor = await OpenEditorAsync();
        await editor.EditAs.SelectOptionAsync("Text");
        await Expect(editor.EditAs).ToHaveValueAsync("Text");
        var text = await editor.Value.InputValueAsync();
        Assert.That(text, Does.Contain("ops@contoso.example"), "line/01's owner, as raw UTF-8");
        await editor.FillAsync(text.Replace("ops@contoso.example", RawLineOwner, StringComparison.Ordinal));
        await CheckpointAsync("5", "raw-utf8", "line/01 edited as raw UTF-8 text");

        await editor.SaveAndCloseAsync();
        await ExpectToastAsync($"Entry {RawLine} saved.");
        await Expect(_tree.EntryValue).ToContainTextAsync(RawLineOwner);
        Coverage.Hit("use-case:data/entry-edit-binary-and-raw");
        await CheckpointAsync("5", "raw-saved", "The raw edit is saved");
    }

    private async Task HaveAnInvalidEditRefusedAsync()
    {
        await _tree.OpenEntryAsync(Names.Trees.FactoryOrders, Names.Orders.Order101);
        var editor = await OpenEditorAsync();
        await editor.FillAsync(U1Contract.RefusedOrder101.Json);
        await Expect(editor.SchemaFails).ToContainTextAsync("The cluster will refuse this value");
        await Expect(editor.Violations.First).ToBeVisibleAsync();
        await CheckpointAsync("6", "preview-warns", $"Status {StoryData.InvalidStatus}: the schema preview warns before she saves");

        await editor.Submit.ClickAsync();
        await Expect(editor.Refusal).ToContainTextAsync("rejected by this tree's schema policy", new() { Timeout = 30_000 });
        await CheckpointAsync("6", "save-refused", "Saved anyway, the write is refused with the policy's reason");

        await editor.Dialog.GetByRole(AriaRole.Button, new() { Name = "Cancel", Exact = true }).ClickAsync();
        await Expect(editor.Dialog).ToHaveCountAsync(0);
        await Expect(_tree.EntryValue).Not.ToContainTextAsync(StoryData.InvalidStatus);
        Coverage.Hit("use-case:data/edit-refused");

        await _tree.OpenTabAsync("Dead letters", "dead-letters");
        await Expect(_priya.Shell.Content.GetByRole(AriaRole.Heading, new() { Name = "No dead letters" })).ToBeVisibleAsync(new() { Timeout = 30_000 });
        await CheckpointAsync("6", "no-dead-letters", "A refused user write is never dead-lettered: Dead letters is empty");
    }

    private async Task CreateAndDeleteAnEntryAndTailHistoryAsync()
    {
        await _priya.Shell.GotoAsync("/data/" + Names.Trees.FactoryOrders);
        await Expect(_tree.Heading).ToHaveTextAsync(Names.Trees.FactoryOrders);
        await _tree.WaitForKeysAsync();
        await _tree.NewEntry.ClickAsync();
        var editor = new DataEntryEditor(_priya.Page, "New entry");
        await editor.WaitOpenAsync();
        await editor.Key.FillAsync(Names.Orders.Scratch);
        await editor.FillAsync(StoryData.Order(1).Json);
        await Expect(editor.SchemaPasses).ToBeVisibleAsync();
        await CheckpointAsync("7", "new-entry", "New entry...: order/scratch, a valid order");

        await editor.SaveAndCloseAsync();
        await ExpectToastAsync($"Entry {Names.Orders.Scratch} created.");
        await Expect(_priya.Page).ToHaveURLAsync(new Regex("key=" + Regex.Escape(Uri.EscapeDataString(Names.Orders.Scratch))));
        await Expect(_tree.EntryValue).ToBeVisibleAsync();
        Coverage.Hit("command:data.new-entry");
        await CheckpointAsync("7", "created", "order/scratch is created");

        await _tree.Delete.ClickAsync();
        var confirm = await _tree.TypeConfirmationAsync("Delete this entry", Names.Orders.Scratch);
        await CheckpointAsync("7", "delete-confirmation", "Delete... asks her to type the key");
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Delete entry", Exact = true }).ClickAsync();
        await ExpectToastAsync($"Entry {Names.Orders.Scratch} deleted.");
        await Expect(_tree.Entry).ToContainTextAsync("No entry at this key");
        Coverage.Hit("command:data.delete-entry");
        await CheckpointAsync("7", "deleted", "order/scratch is deleted");

        await _tree.HistoryOfKey.ClickAsync();
        var kinds = _priya.Shell.Content.Locator(".lt-data-timeline__kind");
        await Expect(kinds.First).ToHaveTextAsync("Deleted", new() { Timeout = 30_000 });
        Assert.That(await kinds.CountAsync(), Is.GreaterThanOrEqualTo(2), "the create and the deletion");
        Coverage.Hit("use-case:data/entry-new-and-delete");
        await CheckpointAsync("7", "tombstone", "History keeps the deletion as a tombstone");

        // The live tail: every change under the line feed's orders, as it arrives.
        await _priya.Shell.GotoAsync("/data/" + Names.Trees.FactoryOrders);
        await _tree.WaitForKeysAsync();
        await _tree.SetPrefixAsync(LineFeedOrdersPrefix);
        await _tree.OpenTabAsync("History", "history");
        await Expect(_priya.Shell.Content.GetByRole(AriaRole.Heading, new() { NameRegex = new Regex("^Changes under ") })).ToContainTextAsync(LineFeedOrdersPrefix);
        var changes = _priya.Shell.Content.GetByRole(AriaRole.List, new() { Name = "Changes under the prefix, newest first" }).Locator("li");
        await Expect(changes.Nth(1)).ToBeVisibleAsync(new() { Timeout = 60_000 });
        Coverage.Hit("use-case:data/history-live-tail");
        await CheckpointAsync("7", "live-tail", "History's live tail shows the line feed's writes under order/1 arriving");
    }

    /// <summary>Opens the open entry's editor and waits for its value.</summary>
    private async Task<DataEntryEditor> OpenEditorAsync()
    {
        await _tree.Edit.ClickAsync();
        var editor = new DataEntryEditor(_priya.Page, "Edit entry");
        await editor.WaitOpenAsync();
        return editor;
    }

    /// <summary>Waits for a toast saying exactly <paramref name="message"/>.</summary>
    private Task ExpectToastAsync(string message) =>
        Expect(_priya.Shell.Toasts.Filter(new() { HasTextRegex = Exactly(message) })).Not.ToHaveCountAsync(0, new() { Timeout = 30_000 });
}
