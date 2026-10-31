using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// One Telemetry board as C2 reads it: its charts drawn, failed with a retry, or drawn again.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed class TelemetryBoardView(PersonaSession session)
{
    /// <summary>What a chart says when the metrics backend did not answer.</summary>
    public const string BackendDidNotAnswer = "The metrics backend did not answer. Try again in a moment.";

    private ILocator Content => session.Shell.Content;

    /// <summary>Every chart on the board.</summary>
    public ILocator Charts => Content.Locator("section.lt-telemetry-chart");

    /// <summary>The charts whose read failed: each shows why, in an alert.</summary>
    public ILocator Failures => Content.Locator(".lt-telemetry-chart__problem[role=alert]");

    /// <summary>The charts drawn: a figure, a table of readings, or the note that no series matched.</summary>
    public ILocator Drawn => Content.Locator("section.lt-telemetry-chart:has(figure.lt-telemetry-figure, table, .lt-telemetry-chart__empty)");

    /// <summary>Opens the board titled <paramref name="title"/> from the Telemetry area's board list.</summary>
    /// <param name="title">The board's title, such as <c>Throughput</c>.</param>
    public async Task OpenAsync(string title)
    {
        await session.Shell.GoToAreaAsync("telemetry");
        await Content.GetByRole(AriaRole.Link, new() { Name = title, Exact = true }).First.ClickAsync();
        await Expect(session.Shell.Heading).ToHaveTextAsync(title);
    }

    /// <summary>Waits until every chart has settled and none failed: each is drawn.</summary>
    /// <param name="timeout">How long to wait.</param>
    public async Task ExpectDrawnAsync(TimeSpan timeout)
    {
        await Expect(Charts.First).ToBeVisibleAsync();
        var expected = await Charts.CountAsync() - await Unaskable.CountAsync();
        await Expect(Failures).ToHaveCountAsync(0, new() { Timeout = (float)timeout.TotalMilliseconds });
        await Expect(Drawn).ToHaveCountAsync(expected, new() { Timeout = (float)timeout.TotalMilliseconds });
    }

    /// <summary>Charts the board does not ask at all under its window, each saying why.</summary>
    public ILocator Unaskable => Content.Locator(".lt-telemetry-chart__problem[role=status]");

    /// <summary>Redraws every chart with the board's <b>Refresh</b>.</summary>
    public Task RefreshAsync() =>
        Content.Locator("button[data-lt-command='telemetry.refresh']").First.ClickAsync();

    /// <summary>Waits until every chart the board asks reports that the metrics backend did not answer, each with <b>Try again</b>.</summary>
    /// <param name="timeout">How long to wait.</param>
    public async Task ExpectBackendDownAsync(TimeSpan timeout)
    {
        var expected = await Charts.CountAsync() - await Unaskable.CountAsync();
        await Expect(Failures).ToHaveCountAsync(expected, new() { Timeout = (float)timeout.TotalMilliseconds });
        await Expect(Drawn).ToHaveCountAsync(0);
        foreach (var failure in await Failures.AllAsync())
        {
            await Expect(failure).ToContainTextAsync(BackendDidNotAnswer);
            await Expect(failure.GetByRole(AriaRole.Button, new() { Name = "Try again", Exact = true })).ToBeVisibleAsync();
        }
    }

    /// <summary>Presses <b>Try again</b> on every chart that offers it.</summary>
    /// <returns>How many charts were retried.</returns>
    public async Task<int> TryAgainAsync()
    {
        var retried = 0;
        var buttons = Content.Locator(".lt-telemetry-chart__problem").GetByRole(AriaRole.Button, new() { NameRegex = new Regex("^Try again$") });
        while (await buttons.CountAsync() > 0)
        {
            await buttons.First.ClickAsync();
            retried++;
            if (retried > 100)
            {
                throw new InvalidOperationException("Try again was pressed on more than 100 charts without the failures clearing.");
            }

            await Task.Delay(250);
        }

        return retried;
    }
}
