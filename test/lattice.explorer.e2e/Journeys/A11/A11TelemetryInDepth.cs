using System.Reflection;
using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A11;

/// <summary>
/// A11, telemetry in depth (#5381): olivia prepares the capacity review. She opens every board,
/// reads the Throughput board over relative ranges, a pinned absolute window and an explicit
/// step (each in the address, each surviving a reload), meets an invalid window, narrows the
/// board to <c>factory-telemetry</c> and back to every tree, reads the series as tables, and
/// reads a chart by keyboard alone. A read-only chapter: it produces nothing.
/// </summary>
[JourneyChapter("A11", After = new[] { "A3", "A8" })]
public sealed class A11TelemetryInDepth : JourneyChapter
{
    private const string JourneyId = "a11";
    private const string ThroughputKey = "throughput";
    private const string WriteChart = "Write operations";

    private static readonly string[] BuiltInBoards = ["throughput", "latency", "storage", "pressure"];

    private static readonly (string Token, string Label)[] RelativeRanges = [("15m", "15 min"), ("1h", "1 h"), ("6h", "6 h")];

    private JourneyEstate _estate = default!;
    private PersonaSession _olivia = default!;
    private TelemetryBoardPage _telemetry = default!;
    private JourneyRecorder _recorder = default!;
    private JourneyStepAttribute _step = default!;
    private IReadOnlyList<ListedBoard> _boards = [];
    private IReadOnlyList<LegendEntry> _pinnedLegend = [];
    private ValuesTable? _pinnedTable;

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _estate = context.Estate;
        _recorder = JourneyRecorder.For(JourneyId);

        // Not an action: the line feed has been writing since A2; this only waits until every
        // silo has been scraped twice, so every rate on a board has the samples it needs.
        await _estate.WaitForPrometheusScrapesAsync();

        await using var olivia = await _estate.SignInAsAsync(Personas.Olivia);
        _olivia = olivia;
        _telemetry = new TelemetryBoardPage(olivia.Page, olivia.Shell);

        await StepAsync(nameof(EveryBoardAsync), EveryBoardAsync);
        await StepAsync(nameof(WindowsAsync), WindowsAsync);
        await StepAsync(nameof(InvalidWindowAsync), InvalidWindowAsync);
        await StepAsync(nameof(TreeScopeAsync), TreeScopeAsync);
        await StepAsync(nameof(TablesAsync), TablesAsync);
        await StepAsync(nameof(KeyboardReadoutAsync), KeyboardReadoutAsync);
    }

    [JourneyStep("a11-1", "Every board in the list opens and renders its charts")]
    private async Task EveryBoardAsync()
    {
        await _olivia.Shell.GoToAreaAsync("telemetry");
        _boards = await _telemetry.ReadBoardListAsync();
        var keys = _boards.Select(board => board.Key).ToArray();
        Assert.That(keys, Is.SupersetOf(BuiltInBoards), $"the built-in boards are listed (listed: {string.Join(", ", _boards)})");
        // olivia is a platform operator on the reserved default tenant, so she sees tenancy chrome
        // (tenant-scope.md, "The reserved default tenant") and with it the Tenant board.
        Assert.That(keys, Does.Contain("tenant"), "an operator on the default tenant sees the Tenant board");
        await ShotAsync("board-list", $"Olivia opens Telemetry: {_boards.Count} boards ({string.Join(", ", _boards.Select(board => board.Title))})");

        for (var i = 0; i < _boards.Count; i++)
        {
            var board = _boards[i];
            if (i == 0)
            {
                await _telemetry.OpenFromListAsync(board);
            }
            else
            {
                await _telemetry.OpenFromBoardNavigationAsync(board);
            }

            var charts = await _telemetry.SettleAsync();
            Assert.That(charts, Is.Not.Empty, $"the {board.Title} board draws charts");
            Assert.That(
                charts.Where(chart => chart.Shows is ChartState.Failure or ChartState.Problem).Select(chart => chart.ToString()),
                Is.Empty,
                $"every chart on the {board.Title} board answers over its default window");
            if (board.Key == ThroughputKey)
            {
                Assert.That(charts.Single(chart => chart.Title == WriteChart).Shows, Is.EqualTo(ChartState.Figure), "the line feed's writes are drawn on Write operations");
            }

            await ShotAsync($"board-{board.Key}", $"The {board.Title} board renders its {charts.Count} charts: {string.Join("; ", charts.Select(chart => $"{chart.Title} ({chart.Shows})"))}");
            Coverage.Hit($"subroute:telemetry/{board.Key}");
        }
    }

    [JourneyStep("a11-2", "Relative ranges, a pinned absolute window and an explicit step, each in the address and surviving a reload")]
    private async Task WindowsAsync()
    {
        await _telemetry.OpenFromBoardNavigationAsync(Board(ThroughputKey));
        foreach (var (token, label) in RelativeRanges)
        {
            await _telemetry.ChooseRangeAsync(token, label);
            await ExpectDrawnAsync();
            await _telemetry.ReloadAsync();
            await _telemetry.ExpectQueryAsync("range", token);
            await _telemetry.ExpectRangeChosenAsync(token, label);
            await ExpectDrawnAsync();
            await ShotAsync($"range-{token}", $"Range {label}: ?range={token} is in the address, the toolbar marks it, and both survive a reload");
        }

        Coverage.Hit("query:telemetry.range");
        Coverage.Hit("query-value:telemetry.range=relative");

        await _telemetry.ChooseRangeAsync("1h", "1 h");
        await ExpectDrawnAsync();
        var (from, to) = await _telemetry.PinWindowAsync();
        await ExpectDrawnAsync();
        await _telemetry.ReloadAsync();
        await _telemetry.ExpectQueryAsync("from", from);
        await _telemetry.ExpectQueryAsync("to", to);
        await _telemetry.ExpectQueryAsync("range", null);
        await Expect(_telemetry.AbsoluteWindow).ToContainTextAsync("Back to each chart's default window");
        await ExpectDrawnAsync();
        await ShotAsync("pinned-window", "Pin this window: the last hour becomes ?from= and ?to=, named above the charts, and survives a reload");
        Coverage.Hit("query:telemetry.from");
        Coverage.Hit("query:telemetry.to");
        Coverage.Hit("query-value:telemetry.from=instant");

        await _telemetry.ChooseStepAsync("1m");
        await ExpectDrawnAsync();
        await _telemetry.ReloadAsync();
        await _telemetry.ExpectQueryAsync("step", "1m");
        await _telemetry.ExpectQueryAsync("from", from);
        await _telemetry.ExpectQueryAsync("to", to);
        await Expect(_telemetry.StepSelect).ToHaveValueAsync("1m");
        await ExpectDrawnAsync();
        await ShotAsync("explicit-step", "Step 1 min: ?step=1m joins the pinned window and survives a reload");
        Coverage.Hit("query:telemetry.step");
        Coverage.Hit("query-value:telemetry.step=explicit");
        Coverage.Hit("use-case:telemetry/windows");
    }

    [JourneyStep("a11-3", "An invalid window: from after to, then a step too fine for the range")]
    private async Task InvalidWindowAsync()
    {
        // A hand-edited link in the browser's address bar: the window ends before it starts.
        var address = new UriBuilder(_olivia.Page.Url) { Query = "from=20261031T120000Z&to=20261031T110000Z" };
        await JourneyBrowser.GotoAsync(_olivia.Page, address.Uri.ToString());
        await _telemetry.ExpectBoardAsync(Board(ThroughputKey));
        await Expect(_telemetry.Notes.Filter(new() { HasTextString = TelemetryBoardPage.InvalidWindowNotice })).ToBeVisibleAsync();
        await Expect(_telemetry.AbsoluteWindow).ToHaveCountAsync(0);
        await _telemetry.ExpectRangeChosenAsync(string.Empty, "Default");
        await ExpectDrawnAsync();
        await ShotAsync("from-after-to", "A window whose from is after its to: the invalid-window notice, no pinned window, and each chart on its default window");

        await _telemetry.ChooseRangeAsync("6h", "6 h");
        await _telemetry.ChooseStepAsync("15s");
        await ExpectDrawnAsync();
        // TelemetryDurations writes 24 hours as the shortest exact token, 1d, labelled "1 d".
        await _telemetry.ChooseRangeAsync("1d", "1 d");
        await _telemetry.ExpectQueryAsync("step", "15s");
        var charts = await _telemetry.SettleAsync();
        Assert.Multiple(() =>
        {
            foreach (var chart in charts)
            {
                // A problem stands in place of the chart's request: the chart asked the cluster nothing.
                Assert.That(chart.Shows, Is.EqualTo(ChartState.Problem), $"{chart} asks nothing at 15 s over 24 h");
                Assert.That(chart.Text, Does.Match("needs 5.?761 points and this chart draws at most 1.?500"), $"{chart} says why");
            }
        });
        await Expect(_telemetry.Content.Locator("figure.lt-telemetry-figure")).ToHaveCountAsync(0);
        await ShotAsync("step-too-fine", "A 15 s step over 24 h: every chart says the window needs 5,761 points of its 1,500 and asks nothing");
        Coverage.Hit("use-case:telemetry/invalid-window");
    }

    [JourneyStep("a11-4", "Narrowed to factory-telemetry, then Show every tree")]
    private async Task TreeScopeAsync()
    {
        var tree = Names.Trees.FactoryTelemetry;
        await _telemetry.ChooseRangeAsync(string.Empty, "Default");
        await _telemetry.ExpectQueryAsync("step", null);
        await ExpectDrawnAsync();
        var series = SeriesOf(tree);
        await _telemetry.Chart(WriteChart).GetByRole(Microsoft.Playwright.AriaRole.Link, new() { NameRegex = new Regex($"^{series}, narrow the board to this tree$") }).First.ClickAsync();
        await _telemetry.ExpectQueryAsync("tree", tree);
        await Expect(_telemetry.TreeFilter).ToContainTextAsync($"Narrowed to tree {tree}.");
        await ExpectDrawnAsync();
        var narrowed = await _telemetry.ReadLegendAsync(WriteChart);
        Assert.That(narrowed, Is.Not.Empty, "Write operations still draws factory-telemetry");
        Assert.That(narrowed.Select(entry => entry.Name), Has.All.Match($"^{series}$"), "every series left on Write operations is factory-telemetry's");
        await ShotAsync("one-tree", $"From the Write operations legend, ?tree={tree}: the board is narrowed to that tree");
        Coverage.Hit("query:telemetry.tree");
        Coverage.Hit("query-value:telemetry.tree=tree");

        await _telemetry.TreeFilter.GetByRole(Microsoft.Playwright.AriaRole.Link, new() { Name = "Show every tree", Exact = true }).ClickAsync();
        await _telemetry.ExpectQueryAsync("tree", null);
        await Expect(_telemetry.TreeFilter).ToHaveCountAsync(0);
        await ExpectDrawnAsync();
        var every = await _telemetry.ReadLegendAsync(WriteChart);
        Assert.That(every.Count, Is.GreaterThanOrEqualTo(narrowed.Count), "every tree draws at least the series one tree drew");
        Assert.That(every.Select(entry => entry.Name), Has.Some.Match($"^{series}$"), "factory-telemetry is still among every tree");
        await ShotAsync("every-tree", $"Show every tree: ?tree= is cleared and Write operations draws {every.Count} series again");
        Coverage.Hit("use-case:telemetry/every-tree");
    }

    [JourneyStep("a11-5", "The board as tables: the same values the charts draw")]
    private async Task TablesAsync()
    {
        await _telemetry.ChooseRangeAsync("1h", "1 h");
        await ExpectDrawnAsync();
        await _telemetry.PinWindowAsync();
        await _telemetry.ChooseStepAsync("1m");

        // Not an action: once Prometheus has scraped again, every sample inside the pinned
        // window is stored, so the charts and the tables answer from the same data.
        await _estate.WaitForPrometheusScrapesAsync();
        await _telemetry.ReloadAsync();
        await ExpectDrawnAsync();
        _pinnedLegend = await _telemetry.ReadLegendAsync(WriteChart);
        Assert.That(_pinnedLegend, Is.Not.Empty, "Write operations draws series over the pinned hour");
        await ShotAsync("charts", $"The pinned hour at 1 min steps as charts; Write operations' legend reads {string.Join("; ", _pinnedLegend)}");

        await _telemetry.ChooseViewAsync(tables: true);
        var charts = await _telemetry.SettleAsync();
        Assert.That(
            charts.Where(chart => chart.Shows is ChartState.Figure or ChartState.Failure or ChartState.Problem).Select(chart => chart.ToString()),
            Is.Empty,
            "every chart is drawn as a table");
        if (await _olivia.Shell.IsCompactAsync())
        {
            await Expect(_telemetry.Chart(WriteChart).Locator(".lt-table-list")).ToBeVisibleAsync();
        }
        else
        {
            _pinnedTable = await _telemetry.ReadValuesTableAsync(WriteChart);
            Assert.That(_pinnedTable.Columns.FirstOrDefault(), Is.EqualTo("Time (UTC)"));
            Assert.That(_pinnedTable.Rows, Has.Length.GreaterThan(2), "the pinned hour has a row per minute");
            Assert.Multiple(() =>
            {
                foreach (var entry in _pinnedLegend)
                {
                    Assert.That(_pinnedTable.LatestOf(entry.Name), Is.EqualTo(entry.Value), $"the table's latest reading of {entry.Name} is the chart's");
                }
            });
        }

        await ShotAsync("tables", "?view=table: every chart as a table of values by time, with the latest readings the legend named");
        Coverage.Hit("query:telemetry.view");
        Coverage.Hit("query-value:telemetry.view=table");
    }

    [JourneyStep("a11-6", "Reading a chart by keyboard alone")]
    private async Task KeyboardReadoutAsync()
    {
        await _telemetry.ChooseViewAsync(tables: false);
        await ExpectDrawnAsync();
        var readout = _telemetry.Readout(WriteChart);
        await Expect(readout).ToHaveAttributeAsync("aria-live", "polite");
        await Expect(_telemetry.Plot(WriteChart)).ToHaveAttributeAsync("aria-describedby", (await readout.GetAttributeAsync("id"))!);

        await _telemetry.TabToPlotAsync(WriteChart);
        await Expect(readout).ToHaveTextAsync(TelemetryBoardPage.ReadoutHint);
        await ShotAsync("focused", "Tab reaches the Write operations chart; its readout says how to read it");

        var last = (_pinnedTable?.Rows.Length ?? 0) - 1;
        await _olivia.Page.Keyboard.PressAsync("End");
        await ExpectReadoutAsync(last);
        await ShotAsync("last-point", "End: the readout announces every drawn series at the last minute, as the table read it");

        await _olivia.Page.Keyboard.PressAsync("ArrowLeft");
        await ExpectReadoutAsync(last - 1);
        await _olivia.Page.Keyboard.PressAsync("ArrowLeft");
        await ExpectReadoutAsync(last - 2);
        await ShotAsync("stepped-back", "Left arrow twice: the readout steps back a minute at a time");

        await _olivia.Page.Keyboard.PressAsync("Home");
        await ExpectReadoutAsync(0);
        await _olivia.Page.Keyboard.PressAsync("ArrowRight");
        await ExpectReadoutAsync(1);
        await ShotAsync("first-points", "Home, then right arrow: the readout announces the first minutes of the window");

        await _olivia.Page.Keyboard.PressAsync("Escape");
        await Expect(readout).ToHaveTextAsync(TelemetryBoardPage.ReadoutHint);
        await Expect(_telemetry.Plot(WriteChart)).ToBeFocusedAsync();
        await ShotAsync("cleared", "Escape clears the readout; focus stays on the chart");
        Coverage.Hit("use-case:telemetry/chart-readout");
    }

    private static string SeriesOf(string tree) => $"{Regex.Escape(tree)}( / .+)?";

    private ListedBoard Board(string key) =>
        _boards.FirstOrDefault(board => board.Key == key) ?? throw new AssertionException($"The {key} board is not listed.");

    private async Task ExpectDrawnAsync()
    {
        var charts = await _telemetry.SettleAsync();
        Assert.That(
            charts.Where(chart => chart.Shows is ChartState.Failure or ChartState.Problem).Select(chart => chart.ToString()),
            Is.Empty,
            $"every chart answers at {_olivia.Page.Url}");
    }

    private async Task ExpectReadoutAsync(int row)
    {
        var readout = _telemetry.Readout(WriteChart);
        if (_pinnedTable is not { } table)
        {
            await Expect(readout).ToHaveTextAsync(new Regex("^\\S.* UTC: .+\\.$"));
            return;
        }

        var time = table.Rows[row][0];
        var prefix = $"{time} UTC: ";
        await Expect(readout).ToHaveTextAsync(new Regex($"^{Regex.Escape(prefix)}"));
        var text = (await readout.TextContentAsync())!.Trim();
        var segments = text[prefix.Length..].TrimEnd('.').Split("; ");
        Assert.That(segments, Has.Length.EqualTo(_pinnedLegend.Count), $"the readout '{text}' names every drawn series");
        Assert.Multiple(() =>
        {
            foreach (var segment in segments)
            {
                var name = table.Columns.Skip(1)
                    .Where(column => segment.StartsWith(column + " ", StringComparison.Ordinal))
                    .OrderByDescending(column => column.Length)
                    .FirstOrDefault();
                Assert.That(name, Is.Not.Null, $"the readout segment '{segment}' names a series of the table");
                if (name is not null)
                {
                    Assert.That(segment[(name.Length + 1)..], Is.EqualTo(table.Cell(row, name)), $"the readout of {name} at {time} is the table's");
                }
            }
        });
    }

    private Task<JourneyManifestEntry> ShotAsync(string substep, string title) =>
        _olivia.SubstepAsync(_recorder, _step.Id, substep, title);

    private async Task StepAsync(string method, Func<Task> body)
    {
        _step = GetType().GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic)?.GetCustomAttribute<JourneyStepAttribute>()
            ?? throw new InvalidOperationException($"{method} carries no [JourneyStep].");
        using var scope = Coverage.BeginScope(_step.Id.ToUpperInvariant().Replace('-', '.'));
        await body();
        scope.Complete();
    }
}
