using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U5;

/// <summary>
/// The address line's completions and the command palette as U5 reads them (#5387): the
/// commands a filter leaves, including none at all, and choosing one completion by its exact
/// label.
/// </summary>
/// <param name="shell">The chrome of the page to drive.</param>
public sealed partial class PalettePage(ShellPage shell)
{
    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell ?? throw new ArgumentNullException(nameof(shell));

    /// <summary>The address line's polite status: how many suggestions there are.</summary>
    public ILocator Status => Shell.Page.Locator(".lt-shell-combobox [role=status]");

    /// <summary>Each suggestion's label (without its detail), in order.</summary>
    public ILocator Labels => Shell.Page.GetByRole(AriaRole.Listbox, new() { Name = "Suggestions" }).Locator(".lt-shell-combobox__label");

    /// <summary>
    /// Opens the address line afresh, types <c>&gt;</c> and <paramref name="filter"/>, and waits
    /// until the palette has announced how many commands match. The palette is left open.
    /// </summary>
    /// <param name="filter">The text after <c>&gt;</c>, such as <c>schema.</c>.</param>
    /// <returns>The matching commands' titles, in order; empty when none matches.</returns>
    public async Task<IReadOnlyList<string>> CommandsAsync(string filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return await TypeAsync(">" + filter);
    }

    /// <summary>
    /// Opens the address line afresh, types <paramref name="text"/>, and waits until every
    /// completion source has answered and the count is announced. The line is left open.
    /// </summary>
    /// <param name="text">What to type, such as <c>a/</c>.</param>
    /// <returns>The suggestions' labels, in order.</returns>
    public async Task<IReadOnlyList<string>> TypeAsync(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (await Shell.AddressInput.IsVisibleAsync())
        {
            await Shell.CloseAddressLineAsync();
        }

        await Shell.OpenAddressLineAsync();
        await Shell.AddressInput.FillAsync(text);
        await Expect(Shell.AddressInput).ToHaveValueAsync(text);
        await Expect(Status).ToHaveTextAsync(Announced());
        var match = Announced().Match(await Status.InnerTextAsync());
        var count = match.Groups["none"].Success ? 0 : match.Groups["one"].Success ? 1 : int.Parse(match.Groups["many"].Value, CultureInfo.InvariantCulture);
        await Expect(Labels).ToHaveCountAsync(count);
        return count == 0 ? [] : await Labels.AllInnerTextsAsync();
    }

    /// <summary>Chooses the open list's suggestion whose label is exactly <paramref name="label"/>.</summary>
    /// <param name="label">The label, such as <c>a/task-board</c>.</param>
    public async Task ChooseAsync(string label)
    {
        ArgumentNullException.ThrowIfNull(label);
        var option = Shell.Suggestions.Filter(new()
        {
            Has = Shell.Page.Locator(".lt-shell-combobox__label", new() { HasTextRegex = new Regex("^" + Regex.Escape(label) + "$") }),
        });
        await Expect(option).ToHaveCountAsync(1);
        await option.ClickAsync();
        await Expect(Shell.AddressInput).ToBeHiddenAsync();
    }

    /// <summary>The status once the count is announced: after the area searches end, not while one runs.</summary>
    [GeneratedRegex(@"^(?:(?<none>No suggestions)|(?<one>1 suggestion)|(?<many>\d+) suggestions)\.")]
    private static partial Regex Announced();
}
