using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// What every A8 page object reads the page through: the shell's content region, a
/// definition list's value by its term, a toast, a button and a link by their names.
/// </summary>
/// <param name="shell">The Explorer's chrome on the persona's page.</param>
internal abstract class A8PageObject(ShellPage shell)
{
    /// <summary>A long operation's patience: a reshard, resize or WAL move runs one coordinator step per tick.</summary>
    public static readonly TimeSpan Structural = TimeSpan.FromMinutes(8);

    /// <summary>The shell.</summary>
    protected ShellPage Shell { get; } = shell;

    /// <summary>The page.</summary>
    protected IPage Page => Shell.Page;

    /// <summary>The page's content region.</summary>
    protected ILocator Content => Shell.Content;

    /// <summary>The value (<c>dd</c>) of the definition-list row whose term (<c>dt</c>) is exactly <paramref name="term"/>.</summary>
    /// <param name="term">The term.</param>
    public ILocator Term(string term) =>
        Content.Locator(".lt-dl__row")
            .Filter(new() { Has = Page.Locator("dt", new() { HasTextRegex = new Regex("^" + Regex.Escape(term) + "$") }) })
            .Locator("dd");

    /// <summary>The toast whose message contains <paramref name="text"/>.</summary>
    /// <param name="text">The message text.</param>
    public ILocator Toast(string text) => Shell.Toasts.Filter(new() { HasText = text });

    /// <summary>The content's button named exactly <paramref name="name"/>.</summary>
    /// <param name="name">The button's accessible name.</param>
    protected ILocator Button(string name) => Content.GetByRole(AriaRole.Button, new() { Name = name, Exact = true });

    /// <summary>The content's link named exactly <paramref name="name"/>.</summary>
    /// <param name="name">The link's accessible name.</param>
    protected ILocator Link(string name) => Content.GetByRole(AriaRole.Link, new() { Name = name, Exact = true });

    /// <summary>A whole number as the Explorer formats counts, such as <c>1,024</c>.</summary>
    /// <param name="value">The number.</param>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>Reads a count the Explorer formatted, such as <c>1,024</c>; a dash reads as <see langword="null"/>.</summary>
    /// <param name="text">The text.</param>
    public static long? ParseCount(string? text) =>
        long.TryParse(text?.Trim(), NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var value) ? value : null;

    /// <summary>Polls <paramref name="condition"/> on what the page shows until it holds, failing after <paramref name="timeout"/>.</summary>
    /// <param name="what">What is awaited, for the failure message.</param>
    /// <param name="condition">Reads the page and answers whether the state has been reached.</param>
    /// <param name="timeout">How long to wait.</param>
    protected static async Task UntilAsync(string what, Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (await condition())
            {
                return;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"The page did not show {what} within {timeout}.");
            }

            await Task.Delay(250);
        }
    }
}
