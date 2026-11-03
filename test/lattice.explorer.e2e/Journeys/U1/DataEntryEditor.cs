using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>The value editor (#5367): the New entry and Edit entry dialog, its schema preview and its concurrent-change warning.</summary>
/// <param name="page">The page.</param>
/// <param name="title">The dialog's title: <c>New entry</c> or <c>Edit entry</c>.</param>
internal sealed class DataEntryEditor(IPage page, string title)
{
    /// <summary>The dialog.</summary>
    public ILocator Dialog => page.GetByRole(AriaRole.Dialog, new() { Name = title });

    /// <summary>The key field of a new entry.</summary>
    public ILocator Key => Dialog.GetByRole(AriaRole.Textbox, new() { Name = "Key", Exact = true });

    /// <summary>The value field.</summary>
    public ILocator Value => Dialog.GetByRole(AriaRole.Textbox, new() { Name = "Value", Exact = true });

    /// <summary>The Edit as select (JSON, Text (UTF-8), Binary (base64)).</summary>
    public ILocator EditAs => Dialog.GetByLabel("Edit as", new() { Exact = true });

    /// <summary>The editor's notes, such as the binary note.</summary>
    public ILocator Note => Dialog.Locator(".lt-data-note");

    /// <summary>The schema preview's verdict that the draft passes.</summary>
    public ILocator SchemaPasses => Dialog.Locator(".lt-data-editor__schema-ok");

    /// <summary>The schema preview's verdict that the draft fails.</summary>
    public ILocator SchemaFails => Dialog.Locator(".lt-data-editor__schema-fail");

    /// <summary>The reasons the draft fails the policy.</summary>
    public ILocator Violations => Dialog.Locator(".lt-data-editor__violations li");

    /// <summary>The save's refusal.</summary>
    public ILocator Refusal => Dialog.Locator(".lt-data-editor__error");

    /// <summary>The concurrent-change warning.</summary>
    public ILocator Conflict => Dialog.Locator(".lt-data-editor__conflict");

    /// <summary>The submit button: Save, or Create entry for a new one.</summary>
    public ILocator Submit => Dialog.GetByRole(AriaRole.Button, new() { Name = title == "New entry" ? "Create entry" : "Save", Exact = true });

    /// <summary>Waits for the dialog to finish loading the value it edits.</summary>
    public async Task WaitOpenAsync()
    {
        await Expect(Dialog).ToBeVisibleAsync();
        await PanelSettled.ExpectDrawnAsync(Value, PanelSettled.Failure(Dialog), "the editor's value");
    }

    /// <summary>Waits for the schema preview's verdict on the draft, which the cluster evaluates, and asserts it passes.</summary>
    public async Task ExpectSchemaPassesAsync()
    {
        await Expect(SchemaPasses.Or(SchemaFails).First).ToBeVisibleAsync(new() { Timeout = PanelSettled.BoundMs });
        await Expect(SchemaPasses).ToBeVisibleAsync();
    }

    /// <summary>
    /// Submits and waits for the save's outcome: the dialog closes, or it shows the cluster's
    /// refusal or the concurrent-change warning. Returns whether it closed.
    /// </summary>
    public async Task<bool> SubmitAsync()
    {
        await Submit.ClickAsync();
        var settled = Dialog.Filter(new() { HasNot = page.Locator(".lt-data-editor__error, .lt-data-editor__conflict") });
        await Expect(settled).ToHaveCountAsync(0, new() { Timeout = PanelSettled.BoundMs });
        return await Dialog.CountAsync() == 0;
    }

    /// <summary>Replaces the draft with <paramref name="text"/>.</summary>
    /// <param name="text">The new draft.</param>
    public async Task FillAsync(string text)
    {
        await Value.FillAsync(text);
        await Expect(Value).ToHaveValueAsync(text);
    }

    /// <summary>Saves and waits for the dialog to close; a refusal or a conflict fails with the dialog's words.</summary>
    public async Task SaveAndCloseAsync()
    {
        if (!await SubmitAsync())
        {
            Assert.Fail("The save did not land: " + (await Refusal.Or(Conflict).First.InnerTextAsync()).ReplaceLineEndings(" "));
        }
    }
}
