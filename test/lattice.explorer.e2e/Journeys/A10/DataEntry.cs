using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A10;

/// <summary>
/// One entry of a tree in the Data area, as A10 reads, edits and deletes it through the
/// entry editor (G3, #5367): <c>/data/{tree}?key={key}</c>.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal sealed class DataEntry(ShellPage shell)
{
    private IPage Page => shell.Page;

    /// <summary>The entry panel.</summary>
    public ILocator Panel => shell.Content.Locator(".lt-data-entry");

    /// <summary>The entry panel's value.</summary>
    public ILocator Value => Panel.Locator(".lt-data-value");

    /// <summary>The pattern a line's JSON value matches when its rate is <paramref name="rate"/>.</summary>
    /// <param name="rate">The rate.</param>
    public static Regex Rate(int rate) => new("\"rate\"\\s*:\\s*" + rate + "\\b");

    /// <summary>Opens <paramref name="key"/> of <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree id.</param>
    /// <param name="key">The key.</param>
    public async Task OpenAsync(string tree, string key)
    {
        await shell.GotoAsync("/data/" + tree + "?key=" + Uri.EscapeDataString(key));
        await Expect(Page).ToHaveURLAsync(new Regex("/data/" + Regex.Escape(tree) + "\\?.*key=" + Regex.Escape(Uri.EscapeDataString(key))));
        await Expect(Panel).ToBeVisibleAsync();
    }

    /// <summary>Asserts the open entry's value reads with <paramref name="rate"/>.</summary>
    /// <param name="rate">The rate.</param>
    public Task ExpectRateAsync(int rate) => Expect(Value).ToContainTextAsync(Rate(rate));

    /// <summary>Asserts the open key holds no live value.</summary>
    public Task ExpectNoEntryAsync() => Expect(Panel).ToContainTextAsync("No entry at this key");

    /// <summary>Opens the entry editor on the open entry with <b>Edit</b>, and replaces its value with <paramref name="json"/>.</summary>
    /// <param name="json">The new value.</param>
    /// <returns>The editor dialog, ready to save.</returns>
    public async Task<ILocator> EditAsync(string json)
    {
        var edit = Panel.GetByRole(AriaRole.Button, new() { Name = "Edit", Exact = true });
        await Expect(edit).ToHaveAttributeAsync("data-lt-command", "data.edit-entry");
        await edit.ClickAsync();
        var dialog = Page.GetByRole(AriaRole.Dialog, new() { Name = "Edit entry" });
        await Expect(dialog).ToBeVisibleAsync();
        await dialog.GetByRole(AriaRole.Textbox, new() { Name = "Value" }).FillAsync(json);
        return dialog;
    }

    /// <summary>Saves the editor and waits for the saved toast and the new value.</summary>
    /// <param name="dialog">The editor.</param>
    /// <param name="key">The key being edited.</param>
    /// <param name="rate">The rate the saved value carries.</param>
    public async Task SaveAsync(ILocator dialog, string key, int rate)
    {
        await dialog.GetByRole(AriaRole.Button, new() { Name = "Save", Exact = true }).ClickAsync();
        await Expect(shell.Toasts).ToContainTextAsync([$"Entry {key} saved."]);
        await Expect(dialog).ToHaveCountAsync(0);
        await ExpectRateAsync(rate);
    }

    /// <summary>Presses <b>Delete...</b> on the open entry and types its key into the confirmation.</summary>
    /// <param name="key">The key.</param>
    /// <returns>The confirmation, ready to delete.</returns>
    public async Task<ILocator> RequestDeleteAsync(string key)
    {
        var delete = Panel.GetByRole(AriaRole.Button, new() { Name = "Delete...", Exact = true });
        await Expect(delete).ToHaveAttributeAsync("data-lt-command", "data.delete-entry");
        await delete.ClickAsync();
        var confirm = Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Delete this entry" });
        await Expect(confirm).ToContainTextAsync(key);
        await confirm.GetByRole(AriaRole.Textbox).FillAsync(key);
        return confirm;
    }

    /// <summary>Confirms the deletion and waits for the deleted toast and the empty entry.</summary>
    /// <param name="confirm">The confirmation.</param>
    /// <param name="key">The key.</param>
    public async Task ConfirmDeleteAsync(ILocator confirm, string key)
    {
        await confirm.GetByRole(AriaRole.Button, new() { Name = "Delete entry" }).ClickAsync();
        await Expect(shell.Toasts).ToContainTextAsync([$"Entry {key} deleted."]);
        await ExpectNoEntryAsync();
    }
}
