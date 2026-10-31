using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A11;

/// <summary>
/// The Telemetry area as A11 drives it: the board list, one board's toolbar (range, pinned
/// window, step, view), its tree filter, and its charts - read as figures, legends, tables and
/// the keyboard readout.
/// </summary>
/// <param name="page">The persona's page.</param>
/// <param name="shell">The persona's shell chrome.</param>
internal sealed class TelemetryBoardPage(IPage page, ShellPage shell)
{
    /// <summary>The notice a board shows for a window it cannot read (from after to, or an unreadable value).</summary>
    public const string InvalidWindowNotice =
        "This address names the time window in a form the Explorer cannot read, so each chart uses its default instead.";

    /// <summary>The readout's text while no point is selected.</summary>
    public const string ReadoutHint = "Point at the chart, or focus it and use the arrow keys, to read the values at a time.";

    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(90);

    /// <summary>The page.</summary>
    public IPage Page { get; } = page;

    /// <summary>The main landmark.</summary>
    public ILocator Content => shell.Content;

    /// <summary>Every chart section on the board.</summary>
    public ILocator Charts => Content.Locator("section.lt-telemetry-chart");

    /// <summary>The board's page-level notes (the invalid-window notice among them).</summary>
    public ILocator Notes => Content.Locator(".lt-telemetry-notes .lt-telemetry-note");

    /// <summary>The paragraph naming the pinned absolute window.</summary>
    public ILocator AbsoluteWindow => Content.Locator("p.lt-telemetry-filter").Filter(new() { HasTextRegex = new Regex("^\\s*From ") });

    /// <summary>The paragraph naming the tree the board is narrowed to.</summary>
    public ILocator TreeFilter => Content.Locator("p.lt-telemetry-filter").Filter(new() { HasTextRegex = new Regex("Narrowed to tree") });

    /// <summary>The Step select, offered once the board names a window.</summary>
    public ILocator StepSelect => Content.GetByLabel("Step", new() { Exact = true });

    private ILocator RangeGroup => Content.GetByRole(AriaRole.Group, new() { Name = "Time range", Exact = true });

    private ILocator ViewGroup => Content.GetByRole(AriaRole.Group, new() { Name = "View", Exact = true });

    /// <summary>The chart titled <paramref name="title"/>.</summary>
    /// <param name="title">The chart's heading.</param>
    public ILocator Chart(string title) => Content.GetByRole(AriaRole.Region, new() { Name = title, Exact = true });

    /// <summary>The plot of <paramref name="chart"/>.</summary>
    /// <param name="chart">The chart's title.</param>
    public ILocator Plot(string chart) => Chart(chart).Locator(".lt-telemetry-plot");

    /// <summary>The live readout of <paramref name="chart"/>.</summary>
    /// <param name="chart">The chart's title.</param>
    public ILocator Readout(string chart) => Chart(chart).Locator("p.lt-telemetry-readout");

    /// <summary>The value the page's address carries for <paramref name="key"/>, or <see langword="null"/>.</summary>
    /// <param name="key">The query key.</param>
    public string? Query(string key)
    {
        foreach (var pair in new Uri(Page.Url).Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var at = pair.IndexOf('=', StringComparison.Ordinal);
            if (Uri.UnescapeDataString(at < 0 ? pair : pair[..at]) == key)
            {
                return at < 0 ? string.Empty : Uri.UnescapeDataString(pair[(at + 1)..]);
            }
        }

        return null;
    }

    /// <summary>Waits until the address's <paramref name="key"/> is <paramref name="value"/> (absent, for <see langword="null"/>).</summary>
    /// <param name="key">The query key.</param>
    /// <param name="value">The value, or <see langword="null"/>.</param>
    public async Task ExpectQueryAsync(string key, string? value)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (Query(key) != value && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
        }

        Assert.That(Query(key), Is.EqualTo(value), $"?{key}= in {Page.Url}");
    }

    /// <summary>Reads the board list: each listed board's key and title, in listing order.</summary>
    public async Task<IReadOnlyList<ListedBoard>> ReadBoardListAsync()
    {
        await shell.ExpectHeadingAsync("Telemetry");
        await Expect(Content.Locator(".lt-skeleton")).ToHaveCountAsync(0, new() { Timeout = (float)SettleTimeout.TotalMilliseconds });
        if (await shell.IsCompactAsync())
        {
            var rows = Content.Locator(".lt-table-list__rows .lt-compact-row > span:first-child");
            await Expect(rows.First).ToBeVisibleAsync();
            var titles = await rows.AllInnerTextsAsync();
            return [.. titles.Select(title => new ListedBoard { Key = title.Trim().ToLowerInvariant(), Title = title.Trim() })];
        }

        var links = Content.Locator("table a.lt-telemetry-link");
        await Expect(links.First).ToBeVisibleAsync();
        return await links.EvaluateAllAsync<ListedBoard[]>(
            "links => links.map(link => ({ key: link.getAttribute('href').split('?')[0].split('/').pop(), title: link.textContent.trim() }))");
    }

    /// <summary>Opens <paramref name="board"/> from the board list.</summary>
    /// <param name="board">The board.</param>
    public async Task OpenFromListAsync(ListedBoard board)
    {
        if (await shell.IsCompactAsync())
        {
            await Content.Locator(".lt-table-list__rows button").Filter(new() { HasTextString = board.Title }).First.ClickAsync();
            await Page.GetByRole(AriaRole.Link, new() { Name = $"Open the {board.Title} board", Exact = true }).ClickAsync();
        }
        else
        {
            await Content.Locator("table a.lt-telemetry-link").Filter(new() { HasTextString = board.Title }).First.ClickAsync();
        }

        await ExpectBoardAsync(board);
    }

    /// <summary>Opens <paramref name="board"/> from the board navigation of the board on screen.</summary>
    /// <param name="board">The board.</param>
    public async Task OpenFromBoardNavigationAsync(ListedBoard board)
    {
        if (await shell.IsCompactAsync())
        {
            await Content.GetByLabel("Board", new() { Exact = true }).SelectOptionAsync(board.Key);
        }
        else
        {
            await Content.GetByRole(AriaRole.Navigation, new() { Name = "Boards" })
                .GetByRole(AriaRole.Link, new() { Name = board.Title, Exact = true }).ClickAsync();
        }

        await ExpectBoardAsync(board);
    }

    /// <summary>Asserts <paramref name="board"/> is on screen at its own address.</summary>
    /// <param name="board">The board.</param>
    public async Task ExpectBoardAsync(ListedBoard board)
    {
        await Expect(Page).ToHaveURLAsync(new Regex($"/telemetry/{Regex.Escape(board.Key)}(\\?|$)"));
        await shell.ExpectHeadingAsync(board.Title);
    }

    /// <summary>
    /// Waits until every chart on the board has answered - none still reading, each one a figure,
    /// a table, an empty answer or a stated problem - and returns what each chart shows.
    /// </summary>
    public async Task<IReadOnlyList<ChartState>> SettleAsync()
    {
        await Expect(Charts.First).ToBeVisibleAsync();
        await Page.WaitForFunctionAsync(
            """
            () => {
              const charts = [...document.querySelectorAll('main#lt-shell-content section.lt-telemetry-chart')];
              return charts.length > 0 && charts.every(chart =>
                !chart.querySelector('.lt-skeleton')
                && !!chart.querySelector('figure.lt-telemetry-figure, .lt-table-frame, .lt-table-list, .lt-table-empty, .lt-telemetry-chart__empty, .lt-telemetry-chart__problem'));
            }
            """,
            null,
            new() { Timeout = (float)SettleTimeout.TotalMilliseconds, PollingInterval = 250 });
        return await Charts.EvaluateAllAsync<ChartState[]>(
            """
            charts => charts.map(chart => {
              const problem = chart.querySelector('.lt-telemetry-chart__problem');
              const empty = chart.querySelector('.lt-telemetry-chart__empty');
              return {
                title: chart.querySelector('.lt-telemetry-chart__title').textContent.trim(),
                shows: problem ? (problem.getAttribute('role') === 'alert' ? 'failure' : 'problem')
                  : chart.querySelector('figure.lt-telemetry-figure') ? 'figure'
                  : empty ? 'empty'
                  : 'table',
                text: (problem || empty || { textContent: '' }).textContent.trim(),
              };
            })
            """);
    }

    /// <summary>Chooses a relative range from the toolbar by its token (<c>15m</c>, <c>1h</c>, ...), or <c>""</c> for Default.</summary>
    /// <param name="token">The range token, or empty for each chart's default window.</param>
    /// <param name="label">The toolbar label (<c>15 min</c>, <c>1 h</c>, <c>Default</c>).</param>
    public async Task ChooseRangeAsync(string token, string label)
    {
        if (await shell.IsCompactAsync())
        {
            await Content.GetByLabel("Range", new() { Exact = true }).SelectOptionAsync(token);
        }
        else
        {
            await RangeGroup.GetByRole(AriaRole.Link, new() { Name = label, Exact = true }).ClickAsync();
        }

        await ExpectQueryAsync("range", token.Length == 0 ? null : token);
        await ExpectRangeChosenAsync(token, label);
    }

    /// <summary>Asserts the toolbar shows <paramref name="label"/> as the current range.</summary>
    /// <param name="token">The range token, or empty for Default.</param>
    /// <param name="label">The toolbar label.</param>
    public async Task ExpectRangeChosenAsync(string token, string label)
    {
        if (await shell.IsCompactAsync())
        {
            await Expect(Content.GetByLabel("Range", new() { Exact = true })).ToHaveValueAsync(token);
        }
        else
        {
            await Expect(RangeGroup.Locator("a[aria-current=true]")).ToHaveTextAsync(label);
        }
    }

    /// <summary>Pins the current relative range as an absolute window with the toolbar's Pin this window.</summary>
    /// <returns>The window's <c>?from=</c> and <c>?to=</c>.</returns>
    public async Task<(string From, string To)> PinWindowAsync()
    {
        await Content.GetByRole(AriaRole.Link, new() { Name = "Pin this window", Exact = true }).ClickAsync();
        await ExpectQueryAsync("range", null);
        await Expect(AbsoluteWindow).ToBeVisibleAsync();
        var from = Query("from");
        var to = Query("to");
        Assert.That(from, Does.Match("^\\d{8}T\\d{6}Z$"), "the pinned window's ?from=");
        Assert.That(to, Does.Match("^\\d{8}T\\d{6}Z$"), "the pinned window's ?to=");
        return (from!, to!);
    }

    /// <summary>Chooses a step from the Step select by its token (<c>15s</c>, <c>1m</c>, ...).</summary>
    /// <param name="token">The step token.</param>
    public async Task ChooseStepAsync(string token)
    {
        await StepSelect.SelectOptionAsync(token);
        await ExpectQueryAsync("step", token);
        await Expect(StepSelect).ToHaveValueAsync(token);
    }

    /// <summary>Switches the board between charts and tables from the View group.</summary>
    /// <param name="tables">Whether to show tables.</param>
    public async Task ChooseViewAsync(bool tables)
    {
        var label = tables ? "Tables" : "Charts";
        await ViewGroup.GetByRole(AriaRole.Link, new() { Name = label, Exact = true }).ClickAsync();
        await ExpectQueryAsync("view", tables ? "table" : null);
        await Expect(ViewGroup.Locator("a[aria-current=true]")).ToHaveTextAsync(label);
    }

    /// <summary>Re-reads the catalogue and redraws every chart with the toolbar's Refresh.</summary>
    public Task RefreshAsync() =>
        Content.Locator(".lt-toolbar button[data-lt-command=\"telemetry.refresh\"]").ClickAsync();

    /// <summary>Reloads the document and waits for the live circuit.</summary>
    public async Task ReloadAsync()
    {
        await Page.ReloadAsync();
        await JourneyBrowser.WaitForInteractiveAsync(Page);
    }

    /// <summary>Reads the legend of <paramref name="chart"/>: each drawn series' name and latest reading.</summary>
    /// <param name="chart">The chart's title.</param>
    public async Task<IReadOnlyList<LegendEntry>> ReadLegendAsync(string chart) =>
        await Chart(chart).Locator("li.lt-telemetry-legend__item").EvaluateAllAsync<LegendEntry[]>(
            """
            items => items.map(item => {
              const name = item.querySelector('.lt-telemetry-legend__name').cloneNode(true);
              name.querySelectorAll('.lt-visually-hidden').forEach(hidden => hidden.remove());
              return { name: name.textContent.trim(), value: item.querySelector('.lt-telemetry-legend__value').textContent.trim() };
            })
            """);

    /// <summary>Reads <paramref name="chart"/>'s values table (the board's table view): its columns and its rows of cell text.</summary>
    /// <param name="chart">The chart's title.</param>
    public async Task<ValuesTable> ReadValuesTableAsync(string chart)
    {
        var table = Chart(chart).Locator("table.lt-table");
        await Expect(table).ToBeVisibleAsync();
        return await table.EvaluateAsync<ValuesTable>(
            """
            table => ({
              columns: [...table.querySelectorAll('thead th')].map(th => th.textContent.trim()),
              rows: [...table.querySelectorAll('tbody tr')].map(tr => [...tr.children].map(cell => cell.textContent.trim())),
            })
            """);
    }

    /// <summary>Moves the keyboard focus, with Tab alone, to <paramref name="chart"/>'s plot, as a keyboard user reaches it.</summary>
    /// <param name="chart">The chart's title.</param>
    public async Task TabToPlotAsync(string chart)
    {
        const int MaxPresses = 250;
        for (var press = 0; press < MaxPresses; press++)
        {
            var arrived = await Page.EvaluateAsync<bool>(
                """
                title => {
                  const active = document.activeElement;
                  if (!active || !active.classList.contains('lt-telemetry-plot')) { return false; }
                  const section = active.closest('section.lt-telemetry-chart');
                  return !!section && section.querySelector('.lt-telemetry-chart__title').textContent.trim() === title;
                }
                """,
                chart);
            if (arrived)
            {
                await Expect(Plot(chart)).ToBeFocusedAsync();
                return;
            }

            await Page.Keyboard.PressAsync("Tab");
        }

        Assert.Fail($"Tab did not reach the {chart} plot within {MaxPresses} presses.");
    }
}
