using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

/// <summary>The address line: its completions, and the command palette it becomes on <c>&gt;</c>.</summary>
public sealed partial class ShellPage
{
    /// <summary>The address line's combobox, once the line is open.</summary>
    public ILocator AddressInput => Page.GetByRole(AriaRole.Combobox, new() { Name = "Address, search or command" });

    /// <summary>The address line's suggestions: completions, or commands in the palette.</summary>
    public ILocator Suggestions => Page.GetByRole(AriaRole.Listbox, new() { Name = "Suggestions" }).GetByRole(AriaRole.Option);

    /// <summary>The address line as a full-screen sheet: how the command palette opens when compact.</summary>
    public ILocator PaletteSheet => Page.Locator(".lt-shell-address-line--sheet[role=search]");

    /// <summary>Opens the address line by its keyboard shortcut and waits for its combobox to take focus.</summary>
    public async Task OpenAddressLineAsync()
    {
        await Page.Locator("body").FocusAsync();
        await Page.Keyboard.PressAsync("Control+k");
        await Expect(AddressInput).ToBeFocusedAsync();
    }

    /// <summary>
    /// Types <paramref name="text"/> into the address line and waits for its suggestions:
    /// the completions of a partial address, search results, or (after <c>&gt;</c>) commands.
    /// </summary>
    /// <param name="text">What to type, such as <c>/data/</c> or <c>&gt;Go to</c>.</param>
    /// <returns>Each suggestion's text, in order.</returns>
    public async Task<IReadOnlyList<string>> CompleteAsync(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (!await AddressInput.IsVisibleAsync())
        {
            await OpenAddressLineAsync();
        }

        await AddressInput.FillAsync(text);
        await Expect(AddressInput).ToHaveValueAsync(text);
        await Expect(Suggestions.First).ToBeVisibleAsync();
        return await Suggestions.AllInnerTextsAsync();
    }

    /// <summary>Types <paramref name="text"/> and chooses the suggestion that contains <paramref name="suggestion"/>.</summary>
    /// <param name="text">What to type.</param>
    /// <param name="suggestion">Text the chosen suggestion contains.</param>
    public async Task ChooseCompletionAsync(string text, string suggestion)
    {
        ArgumentNullException.ThrowIfNull(suggestion);
        await CompleteAsync(text);
        var option = Suggestions.Filter(new() { HasText = suggestion });
        await Expect(option).ToHaveCountAsync(1);
        await option.ClickAsync();
        await Expect(AddressInput).ToBeHiddenAsync();
    }

    /// <summary>Opens the command palette (the address line holding <c>&gt;</c>) and returns the commands it lists.</summary>
    public Task<IReadOnlyList<string>> OpenCommandPaletteAsync() => CompleteAsync(">");

    /// <summary>Runs the palette command titled <paramref name="title"/>, such as <c>Go to Data</c>.</summary>
    /// <param name="title">The command's title.</param>
    public Task RunCommandAsync(string title)
    {
        ArgumentNullException.ThrowIfNull(title);
        return ChooseCompletionAsync(">" + title, title);
    }

    /// <summary>Closes the address line (or palette) with Escape, restoring the address.</summary>
    public async Task CloseAddressLineAsync()
    {
        for (var attempt = 0; attempt < 2 && await AddressInput.IsVisibleAsync(); attempt++)
        {
            // The first Escape may only close the suggestions; the next one restores the address.
            await Page.Keyboard.PressAsync("Escape");
        }

        await Expect(AddressInput).ToBeHiddenAsync();
    }
}
