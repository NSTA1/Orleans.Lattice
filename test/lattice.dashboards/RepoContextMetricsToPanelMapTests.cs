using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Drift guard over the <c>Orleans.Lattice.Api.Mcp.RepoContext</c> meter's
/// entry in the metrics-to-panel map.
/// </summary>
/// <remarks>
/// <para>
/// The repository-context MCP surface publishes instruments named
/// <c>repocontext.*</c> rather than <c>orleans.lattice.*</c>, so neither
/// <see cref="DashboardJsonTests"/> nor the shared
/// <c>MeterDashboardCoverageTestsBase</c> sees them: both are hardwired to the
/// <c>orleans_lattice_</c> token prefix. Those instruments were therefore
/// charted nowhere and guarded by nothing, and the panel map's record that they
/// are deliberately unpaneled was held in place by prose alone. Prose is not a
/// guard: an instrument could be added, renamed, or removed without anything
/// failing, and the map would drift silently.
/// </para>
/// <para>
/// This fixture makes that state executable in both directions. Forward: every
/// <c>repocontext.*</c> instrument declared in source has a row. Reverse: every
/// row names an instrument that still exists, so a stale row cannot survive a
/// rename. Each row's charted/not-charted claim must agree with actual panel
/// expressions, and every referenced token must resolve to a documented instrument.
/// </para>
/// <para>
/// Every scan carries a floor, because a guard whose scan matches nothing
/// reports success for a set it never examined, which is strictly worse than no
/// guard. The dashboard scan additionally carries a positive control: the same
/// regex family, run over the same dashboard JSON, must find a substantial
/// number of <c>orleans_lattice_</c> tokens. Without it, a change that broke
/// token extraction outright would read as "no repocontext tokens found" and
/// pass.
/// </para>
/// </remarks>
[TestFixture]
public sealed class RepoContextMetricsToPanelMapTests
{
    private const string PackageRelativePath = "src/lattice.api.mcp.repocontext";
    private const string PanelMapRelativePath = "docs/lattice.dashboards/metrics-to-panel-map.md";

    /// <summary>
    /// Floor on the number of distinct <c>repocontext.*</c> instrument names the
    /// source scan must find. The surface publishes eleven at the time of
    /// writing; the floor sits below that so ordinary retirement of an
    /// instrument does not fail the guard, but far enough above zero that a
    /// scan which silently stops matching does.
    /// </summary>
    private const int MinimumDeclaredInstruments = 8;

    /// <summary>
    /// Floor on the number of distinct <c>orleans_lattice_</c> tokens the
    /// positive control must find in the bundled dashboards. This is the
    /// control that distinguishes "the dashboards reference no repocontext
    /// instrument" from "token extraction is broken and finds nothing at all".
    /// </summary>
    private const int MinimumPositiveControlTokens = 20;

    private static readonly Regex InstrumentNameLiteralRegex =
        new(@"""(repocontext\.[a-z0-9_.]+)""", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex PanelMapRowRegex =
        new(@"^\|\s*`(repocontext\.[a-z0-9_.]+)`\s*\|", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex RepoContextTokenRegex =
        new(@"\brepocontext_[a-z0-9_]+\b", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex PositiveControlTokenRegex =
        new(@"\borleans_lattice_[a-z0-9_]+\b", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Test]
    public void Every_repocontext_instrument_declared_in_source_has_a_panel_map_row()
    {
        var declared = DiscoverDeclaredInstrumentNames();
        var rows = ReadPanelMapRows();

        var missing = declared.Keys
            .Where(name => !rows.ContainsKey(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .Select(name => $"- {name}  (declared in {declared[name]})")
            .ToList();

        Assert.That(
            missing,
            Is.Empty,
            $"The following instruments on the Orleans.Lattice.Api.Mcp.RepoContext meter have no row in "
                + $"{PanelMapRelativePath}. An instrument absent from the panel map is charted nowhere and "
                + "recorded nowhere, so nothing distinguishes a deliberate gap from an oversight. Add a row "
                + "for each, marking the Panel(s) column '**not charted**' until a panel lands:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, missing));
    }

    [Test]
    public void Every_repocontext_panel_map_row_names_an_instrument_that_still_exists()
    {
        var declared = DiscoverDeclaredInstrumentNames();
        var rows = ReadPanelMapRows();

        var orphaned = rows.Keys
            .Where(name => !declared.ContainsKey(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .Select(name => $"- {name}  ({PanelMapRelativePath}:{rows[name].LineNumber})")
            .ToList();

        Assert.That(
            orphaned,
            Is.Empty,
            $"The following rows in {PanelMapRelativePath} name instruments that are not declared anywhere "
                + $"under {PackageRelativePath}. Either the instrument was renamed and the row was not, or it "
                + "was retired and the row outlived it. A stale row is worse than a missing one: it asserts "
                + "coverage of a measurand that does not exist."
                + Environment.NewLine
                + string.Join(Environment.NewLine, orphaned));
    }

    [Test]
    public void Every_repocontext_panel_map_row_agrees_with_actual_panel_expressions()
        => AssertPanelMapAgrees(ReadPanelReferences());

    private static void AssertPanelMapAgrees(HashSet<(string Token, string Dashboard, string Panel)> referenced)
    {
        var rows = ReadPanelMapRows();
        var mismatches = rows
            .Where(row =>
            {
                var forms = PrometheusForms(row.Key, row.Value.Text).ToHashSet(StringComparer.Ordinal);
                var matches = referenced.Where(reference => forms.Contains(reference.Token)).ToArray();
                var cells = row.Value.Text.Split('|');
                return cells[5].Contains("not charted", StringComparison.OrdinalIgnoreCase)
                    ? matches.Length != 0
                    : !matches.Any(reference => cells[4].Trim() == reference.Dashboard
                        && cells[5].Trim().StartsWith(reference.Panel, StringComparison.Ordinal));
            })
            .OrderBy(row => row.Key, StringComparer.Ordinal)
            .Select(row => $"- {row.Key}  ({PanelMapRelativePath}:{row.Value.LineNumber})")
            .ToList();

        Assert.That(
            mismatches,
            Is.Empty,
            "The following map rows disagree with bundled panel expressions. Update the panel and map together:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, mismatches));
    }

    [Test]
    public void Every_bundled_repocontext_panel_token_resolves_to_a_documented_instrument()
    {
        var expected = ReadPanelMapRows()
            .SelectMany(row => PrometheusForms(row.Key, row.Value.Text))
            .ToHashSet(StringComparer.Ordinal);
        Assert.That(ReadPanelReferences().Select(reference => reference.Token).Except(expected), Is.Empty,
            "A panel references an unknown repocontext metric or the wrong Prometheus unit/type suffix.");
    }

    [Test]
    public void Exact_scan_cost_panels_are_present_and_do_not_fabricate_zero_or_filter_by_tree()
    {
        using var json = JsonDocument.Parse(LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Overview));
        var panels = json.RootElement.GetProperty("panels").EnumerateArray()
            .Where(panel => panel.GetProperty("title").GetString()!.StartsWith("Exact KNN ", StringComparison.Ordinal))
            .ToArray();
        Assert.That(panels, Has.Length.EqualTo(3));
        foreach (var panel in panels)
        foreach (var target in panel.GetProperty("targets").EnumerateArray())
        {
            var expression = target.GetProperty("expr").GetString();
            Assert.That(expression, Does.Not.Contain("or vector(0)").And.Not.Contain("tree="));
        }
    }

    [TestCase("uncharted-token")]
    [TestCase("missing-panel")]
    [TestCase("empty-scan")]
    public void Panel_map_guard_rejects_perturbed_production_dashboard(string perturbation)
    {
        var failure = Assert.Throws<AssertionException>(() => AssertPanelMapAgrees(ReadPanelReferences(json =>
        {
            if (perturbation == "empty-scan") return "{}";
            if (perturbation == "uncharted-token")
                return json.Replace("repocontext_retrieval_exact_scan_vectors_total", "repocontext_calls_total",
                    StringComparison.Ordinal);

            var root = JsonNode.Parse(json)!.AsObject();
            var panels = root["panels"]!.AsArray();
            var removed = panels.FirstOrDefault(panel => panel?["title"]?.GetValue<string>() == "Exact KNN gather work");
            if (removed is not null) panels.Remove(removed);
            return root.ToJsonString();
        })));
        Assert.That(failure!.Message, Does.Contain(perturbation switch
        {
            "uncharted-token" => "repocontext.calls",
            "missing-panel" => "repocontext.retrieval.exact_scan.pages",
            _ => "Positive control failed",
        }));
    }

    private static IEnumerable<string> PrometheusForms(string name, string row)
    {
        var token = name.Replace('.', '_');
        var unit = row.Contains("(`s`)", StringComparison.Ordinal) ? "_seconds"
            : row.Contains("(`ms`)", StringComparison.Ordinal) ? "_milliseconds"
            : row.Contains("(`By`)", StringComparison.Ordinal) ? "_bytes" : "";
        if (!token.EndsWith(unit, StringComparison.Ordinal)) token += unit;
        if (row.Contains("histogram", StringComparison.OrdinalIgnoreCase))
            return [token + "_sum", token + "_count", token + "_bucket"];
        return [row.Contains("| counter", StringComparison.OrdinalIgnoreCase) ? token + "_total" : token];
    }

    private static HashSet<(string Token, string Dashboard, string Panel)> ReadPanelReferences(
        Func<string, string>? perturb = null)
    {
        var referenced = new HashSet<(string Token, string Dashboard, string Panel)>();
        var positiveControl = new SortedSet<string>(StringComparer.Ordinal);
        var dashboardCount = 0;

        foreach (var kind in LatticeDashboards.All)
        {
            var json = LatticeDashboards.GetGrafanaDashboardJson(kind);
            if (perturb is not null) json = perturb(json);
            dashboardCount++;

            using var document = JsonDocument.Parse(json);
            CollectExpressions(document.RootElement, kind.ToString(), "", referenced, positiveControl);
        }

        Assert.That(
            dashboardCount,
            Is.GreaterThan(0),
            "No bundled dashboards were enumerated, so this guard examined nothing.");

        Assert.That(
            positiveControl,
            Has.Count.AtLeast(MinimumPositiveControlTokens),
            $"Positive control failed: the same token-extraction regex family found only "
                + $"{positiveControl.Count} distinct 'orleans_lattice_' token(s) across {dashboardCount} "
                + "bundled dashboard(s), which is below the floor. Token extraction is broken, so the "
                + "repocontext half of this test is measuring nothing and its silence means nothing.");

        return referenced;
    }

    private static void CollectExpressions(
        JsonElement element,
        string dashboard,
        string panel,
        HashSet<(string Token, string Dashboard, string Panel)> references,
        SortedSet<string> positiveControl)
    {
        if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in element.EnumerateArray())
                CollectExpressions(child, dashboard, panel, references, positiveControl);
        }
        else if (element.ValueKind == JsonValueKind.Object)
        {
            if (element.TryGetProperty("title", out var title)) panel = title.GetString()!;
            foreach (var property in element.EnumerateObject())
            {
                if (property.Name == "expr" && property.Value.ValueKind == JsonValueKind.String)
                {
                    var expression = property.Value.GetString()!;
                    foreach (Match match in RepoContextTokenRegex.Matches(expression))
                        references.Add((match.Value, dashboard, panel));
                    foreach (Match match in PositiveControlTokenRegex.Matches(expression))
                        positiveControl.Add(match.Value);
                }
                else CollectExpressions(property.Value, dashboard, panel, references, positiveControl);
            }
        }
    }

    private static Dictionary<string, string> DiscoverDeclaredInstrumentNames()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var packageRoot = Path.Combine(repoRoot, PackageRelativePath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(
            Directory.Exists(packageRoot),
            Is.True,
            $"The package root '{PackageRelativePath}' does not exist, so this guard scanned nothing.");

        var declared = new Dictionary<string, string>(StringComparer.Ordinal);
        var examined = 0;

        foreach (var file in Directory.EnumerateFiles(packageRoot, "*.cs", SearchOption.AllDirectories))
        {
            var normalized = file.Replace('\\', '/');
            if (normalized.Contains("/bin/", StringComparison.Ordinal)
                || normalized.Contains("/obj/", StringComparison.Ordinal))
            {
                continue;
            }

            examined++;
            var text = File.ReadAllText(file);
            var relative = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');

            foreach (Match match in InstrumentNameLiteralRegex.Matches(text))
            {
                declared.TryAdd(match.Groups[1].Value, relative);
            }
        }

        Assert.That(
            examined,
            Is.GreaterThan(0),
            $"No C# files were examined under '{PackageRelativePath}', so this guard scanned nothing.");

        Assert.That(
            declared,
            Has.Count.AtLeast(MinimumDeclaredInstruments),
            $"Only {declared.Count} distinct 'repocontext.' instrument name(s) were found under "
                + $"'{PackageRelativePath}' across {examined} file(s), which is below the floor of "
                + $"{MinimumDeclaredInstruments}. The scan has stopped matching what it is supposed to "
                + "match, so every assertion built on it is vacuous.");

        return declared;
    }

    private static Dictionary<string, PanelMapRow> ReadPanelMapRows()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();
        var panelMap = Path.Combine(repoRoot, PanelMapRelativePath.Replace('/', Path.DirectorySeparatorChar));

        Assert.That(
            File.Exists(panelMap),
            Is.True,
            $"The panel map '{PanelMapRelativePath}' does not exist, so this guard read nothing.");

        var rows = new Dictionary<string, PanelMapRow>(StringComparer.Ordinal);
        var lines = File.ReadAllLines(panelMap);

        for (var i = 0; i < lines.Length; i++)
        {
            var match = PanelMapRowRegex.Match(lines[i]);
            if (match.Success)
            {
                rows.TryAdd(match.Groups[1].Value, new PanelMapRow(i + 1, lines[i]));
            }
        }

        Assert.That(
            rows,
            Has.Count.AtLeast(MinimumDeclaredInstruments),
            $"Only {rows.Count} 'repocontext.' row(s) were parsed out of {PanelMapRelativePath}, which is "
                + $"below the floor of {MinimumDeclaredInstruments}. Either the table was restructured and "
                + "the row pattern no longer matches it, or the rows were removed. Both make this fixture "
                + "vacuous.");

        return rows;
    }

    private readonly record struct PanelMapRow(int LineNumber, string Text);
}
