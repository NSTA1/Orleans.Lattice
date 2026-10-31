using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A7;

/// <summary>
/// One tree's Replication page, <c>/replication/trees/{tree}</c>: its per-peer links, re-read
/// on a cadence that runs only while the browser page is visible. The journey hides and shows
/// the page with Playwright's emulation of the Page Visibility API: the document reports
/// <c>hidden</c> and raises <c>visibilitychange</c>, as a browser does when its tab goes to the
/// background.
/// </summary>
internal sealed class ReplicationTreeScreen(ShellPage shell)
{
    private const string HideScript =
        """
        () => {
          Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'hidden' });
          Object.defineProperty(document, 'hidden', { configurable: true, get: () => true });
          document.dispatchEvent(new Event('visibilitychange'));
        }
        """;

    private const string ShowScript =
        """
        () => {
          delete document.visibilityState;
          delete document.hidden;
          document.dispatchEvent(new Event('visibilitychange'));
        }
        """;

    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell;

    /// <summary>The page.</summary>
    public IPage Page => Shell.Page;

    /// <summary>The status line: how many links, when they were read, and whether the cadence runs.</summary>
    public ILocator StatusLine => Shell.Content.GetByText(new Regex("Read at .* UTC")).First;

    /// <summary>The page's lede: enrolment state, merge mode and source.</summary>
    public ILocator Lede => Shell.Content.GetByText(new Regex("(Replicated|Not replicated) - .* enrolment")).First;

    /// <summary>The row of the link with <paramref name="peer"/>, in <paramref name="direction"/>.</summary>
    /// <param name="peer">The peer region.</param>
    /// <param name="direction"><c>Outbound</c> or <c>Inbound</c>.</param>
    /// <param name="compact">Whether the page draws its compact chrome.</param>
    public ILocator LinkRow(string peer, string direction, bool compact) =>
        ReplicationTables.Rows(Shell.Content, peer, compact)
            .Filter(new() { HasText = compact ? (direction == "Outbound" ? "To " : "From ") + peer : direction });

    /// <summary>Waits for the page of <paramref name="treeId"/> to have read its links.</summary>
    /// <param name="treeId">The tree id.</param>
    public async Task ExpectLoadedAsync(string treeId)
    {
        await Expect(Page).ToHaveURLAsync(new Regex("/replication/trees/" + Regex.Escape(treeId) + "(\\?|$)"));
        await Shell.ExpectHeadingAsync(treeId);
        await Expect(StatusLine).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }

    /// <summary>Waits until the status line says the page is refreshing on its cadence.</summary>
    public Task ExpectRefreshingAsync() =>
        Expect(StatusLine).ToContainTextAsync(new Regex("refreshing every \\d+ s\\."), new() { Timeout = 30_000 });

    /// <summary>Waits until the status line says the cadence is paused.</summary>
    public Task ExpectPausedAsync() =>
        Expect(StatusLine).ToContainTextAsync("paused while the page is hidden.", new() { Timeout = 30_000 });

    /// <summary>Waits until the links are read again: the status line's read time moves.</summary>
    /// <param name="timeout">How long to wait.</param>
    public async Task ExpectReadAgainAsync(TimeSpan timeout)
    {
        var before = await ReadAtAsync();
        await Expect(StatusLine).Not.ToContainTextAsync("Read at " + before, new() { Timeout = (float)timeout.TotalMilliseconds });
    }

    /// <summary>The status line's read time, <c>HH:mm:ss</c>.</summary>
    public async Task<string> ReadAtAsync()
    {
        var text = await StatusLine.InnerTextAsync();
        var match = Regex.Match(text, "Read at (\\d{2}:\\d{2}:\\d{2}) UTC");
        return match.Success ? match.Groups[1].Value : throw new InvalidOperationException($"The status line names no read time: '{text}'.");
    }

    /// <summary>Hides the page, as a browser does when its tab goes to the background.</summary>
    public Task HideAsync() => Page.EvaluateAsync(HideScript);

    /// <summary>Shows the page again, as a browser does when its tab returns.</summary>
    public Task ShowAsync() => Page.EvaluateAsync(ShowScript);
}
