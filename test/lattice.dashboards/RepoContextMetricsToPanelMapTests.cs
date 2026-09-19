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
/// rename. It also pins the recorded state itself - each row reads
/// <c>not charted</c> - and asserts no bundled dashboard references a
/// <c>repocontext_</c> token, so the day a panel does land the mismatch fails
/// here rather than shipping a map that contradicts the dashboards.
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
    public void Every_repocontext_panel_map_row_records_the_instrument_as_not_charted()
    {
        var rows = ReadPanelMapRows();

        var charted = rows
            .Where(row => !row.Value.Text.Contains("not charted", StringComparison.OrdinalIgnoreCase))
            .OrderBy(row => row.Key, StringComparer.Ordinal)
            .Select(row => $"- {row.Key}  ({PanelMapRelativePath}:{row.Value.LineNumber})")
            .ToList();

        Assert.That(
            charted,
            Is.Empty,
            "The following panel-map rows no longer record their instrument as '**not charted**'. That is a "
                + "welcome change, but it must land together with the panel: update "
                + $"{nameof(No_bundled_dashboard_references_a_repocontext_instrument)} and the surrounding "
                + $"prose in {PanelMapRelativePath}, which both still state that no bundled dashboard charts "
                + "this meter."
                + Environment.NewLine
                + string.Join(Environment.NewLine, charted));
    }

    [Test]
    public void No_bundled_dashboard_references_a_repocontext_instrument()
    {
        var referenced = new SortedSet<string>(StringComparer.Ordinal);
        var positiveControl = new SortedSet<string>(StringComparer.Ordinal);
        var dashboardCount = 0;

        foreach (var kind in LatticeDashboards.All)
        {
            var json = LatticeDashboards.GetGrafanaDashboardJson(kind);
            dashboardCount++;

            foreach (Match match in RepoContextTokenRegex.Matches(json))
            {
                referenced.Add($"{match.Value}  (dashboard {kind})");
            }

            foreach (Match match in PositiveControlTokenRegex.Matches(json))
            {
                positiveControl.Add(match.Value);
            }
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

        Assert.That(
            referenced,
            Is.Empty,
            "A bundled dashboard now references a repocontext instrument token. That contradicts the "
                + $"'**not charted**' rows and the surrounding prose in {PanelMapRelativePath}, which this "
                + "fixture holds in place. Update the map's Panel(s) column and its prose in the same change:"
                + Environment.NewLine
                + string.Join(Environment.NewLine, referenced.Select(token => $"- {token}")));
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
