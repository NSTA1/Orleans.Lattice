using Orleans.Lattice.Testing.Hygiene;
using Orleans.Lattice.Tests.Formal;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Gates the area census in <c>docs/lattice/formal-coverage.md</c> against the
/// modules discovered under <c>spec/</c> (#5168), with negative controls that
/// prove a wrong figure and an unaccounted module each turn it red.
/// Toolchain-free: it reads manifests and refinement notes and never runs TLC.
/// </summary>
[TestFixture]
[Category("Docs")]
public sealed class FormalCoverageAreaCensusTests
{
    private const string ControlArea = "Shard ownership";
    private const string ControlCompanion = "MaterialisedViewFence";

    private static readonly Lazy<IReadOnlyDictionary<string, FormalCoverageModuleCensus>> Census = new(() =>
        SpecModuleCatalogue.Repository().ToDictionary(m => m.Name, FormalCoverageModuleCensus.Of, StringComparer.Ordinal));

    private static string Page() =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), FormalCoverageAreaCensus.PagePath));

    [Test]
    public void Areas_table_figures_match_the_module_manifests_and_refinement_notes()
    {
        var page = Page();
        var rows = MarkdownTable.Read(page, FormalCoverageAreaCensus.AreasSection, FormalCoverageAreaCensus.PagePath).Rows;

        Assert.That(rows, Has.Count.EqualTo(FormalCoverageAreaCensus.AreaModules.Count).And.Count.GreaterThan(0),
            "The areas table must have one row per declared area; an empty table would check nothing.");
        Assert.That(FormalCoverageAreaCensus.AreaFindings(page, Census.Value, FormalCoverageAreaCensus.AreaModules), Is.Empty,
            $"Figures in the '## {FormalCoverageAreaCensus.AreasSection}' table of {FormalCoverageAreaCensus.PagePath} "
            + "must equal those derived from the module manifests and refinement notes.");
    }

    [Test]
    public void Companion_mutation_figures_match_the_module_manifests()
    {
        Assert.That(FormalCoverageAreaCensus.CompanionFindings(Page(), Census.Value), Is.Empty);
    }

    [Test]
    public void Every_discovered_module_is_accounted_for_exactly_once()
    {
        Assert.That(Census.Value, Has.Count.GreaterThanOrEqualTo(SpecModuleCatalogue.MinimumRepositoryModules));
        Assert.That(
            FormalCoverageAreaCensus.AccountingFindings(Page(), Census.Value.Keys.ToList(), FormalCoverageAreaCensus.AreaModules),
            Is.Empty);
    }

    [TestCase("Modules")]
    [TestCase("Mutations")]
    [TestCase("Behaviour rows")]
    [TestCase("Detector tests")]
    public void A_wrong_area_figure_is_rejected(string header)
    {
        var page = Page();
        var stated = Cell(page, FormalCoverageAreaCensus.AreasSection, ControlArea, header);
        var wrong = (int.Parse(stated.Replace(",", string.Empty, StringComparison.Ordinal), System.Globalization.CultureInfo.InvariantCulture) + 1)
            .ToString(System.Globalization.CultureInfo.InvariantCulture);
        var perturbed = ReplaceCell(page, FormalCoverageAreaCensus.AreasSection, ControlArea, header, wrong);

        Assert.That(perturbed, Is.Not.EqualTo(page), "The control must change a real cell.");
        Assert.That(
            FormalCoverageAreaCensus.AreaFindings(perturbed, Census.Value, FormalCoverageAreaCensus.AreaModules),
            Has.Some.Contains($"'{ControlArea}' states {wrong} {header.ToLowerInvariant()}"));
    }

    [Test]
    public void A_wrong_companion_figure_is_rejected()
    {
        var page = Page();
        var perturbed = ReplaceCell(
            page, FormalCoverageAreaCensus.CompanionsSection, ControlCompanion, FormalCoverageAreaCensus.CompanionMutationsHeader, "999");

        Assert.That(perturbed, Is.Not.EqualTo(page), "The control must change a real cell.");
        Assert.That(FormalCoverageAreaCensus.CompanionFindings(perturbed, Census.Value),
            Has.Some.Contains($"companion '{ControlCompanion}' states '999'"));
    }

    [Test]
    public void A_module_dropped_from_its_area_is_reported_unaccounted()
    {
        var areas = WithArea(ControlArea, FormalCoverageAreaCensus.AreaModules[ControlArea].Where(m => m != "ShardOwnershipFold").ToArray());

        Assert.That(
            FormalCoverageAreaCensus.AccountingFindings(Page(), Census.Value.Keys.ToList(), areas),
            Has.Some.Contains("module 'ShardOwnershipFold' is accounted for by no census"));
    }

    [Test]
    public void A_companion_row_removed_from_the_page_is_reported_unaccounted()
    {
        var page = Page();
        var perturbed = RemoveRow(page, FormalCoverageAreaCensus.CompanionsSection, ControlCompanion);

        Assert.That(perturbed, Is.Not.EqualTo(page), "The control must remove a real row.");
        Assert.That(
            FormalCoverageAreaCensus.AccountingFindings(perturbed, Census.Value.Keys.ToList(), FormalCoverageAreaCensus.AreaModules),
            Has.Some.Contains($"module '{ControlCompanion}' is accounted for by no census"));
    }

    [Test]
    public void A_module_counted_by_two_censuses_is_rejected()
    {
        var tail = "Materialised-view tail and lifecycle";
        var areas = WithArea(tail, [.. FormalCoverageAreaCensus.AreaModules[tail], ControlCompanion]);

        Assert.That(
            FormalCoverageAreaCensus.AccountingFindings(Page(), Census.Value.Keys.ToList(), areas),
            Has.Some.Contains($"module '{ControlCompanion}' is counted 2 times"));
    }

    [Test]
    public void A_census_naming_an_undiscovered_module_is_rejected()
    {
        var areas = WithArea(ControlArea, [.. FormalCoverageAreaCensus.AreaModules[ControlArea], "NoSuchModule"]);

        Assert.That(
            FormalCoverageAreaCensus.AccountingFindings(Page(), Census.Value.Keys.ToList(), areas),
            Has.Some.Contains("names 'NoSuchModule', which is not a module discovered under spec/"));
    }

    [Test]
    public void An_area_row_with_no_declared_module_set_is_rejected()
    {
        var page = Page();
        var row = MarkdownTable.Read(page, FormalCoverageAreaCensus.AreasSection, FormalCoverageAreaCensus.PagePath).Rows[0];
        var lines = page.ReplaceLineEndings("\n").Split('\n').ToList();
        lines.Insert(row.LineNumber, "| Undeclared area | 1 | 1 | 1 | 1 | none |");
        var perturbed = string.Join("\n", lines);

        Assert.That(
            FormalCoverageAreaCensus.AreaFindings(perturbed, Census.Value, FormalCoverageAreaCensus.AreaModules),
            Has.Some.Contains("the area 'Undeclared area' has no declared module set"));
    }

    [Test]
    public void A_declared_area_missing_from_the_table_is_rejected()
    {
        var page = Page();
        var perturbed = RemoveRow(page, FormalCoverageAreaCensus.AreasSection, ControlArea);

        Assert.That(perturbed, Is.Not.EqualTo(page), "The control must remove a real row.");
        Assert.That(
            FormalCoverageAreaCensus.AreaFindings(perturbed, Census.Value, FormalCoverageAreaCensus.AreaModules),
            Has.Some.Contains($"the declared area '{ControlArea}' has no row"));
    }

    private static Dictionary<string, IReadOnlyList<string>> WithArea(string area, IReadOnlyList<string> modules) =>
        new(FormalCoverageAreaCensus.AreaModules, StringComparer.Ordinal) { [area] = modules };

    private static MarkdownTableRow Row(MarkdownTable table, string label) =>
        table.Rows.Single(r => string.Equals(r.Cells[0].Trim('`'), label, StringComparison.Ordinal));

    private static string Cell(string page, string section, string label, string header)
    {
        var table = MarkdownTable.Read(page, section, FormalCoverageAreaCensus.PagePath);
        return Row(table, label).Cells[table.ColumnOf(header)];
    }

    private static string ReplaceCell(string page, string section, string label, string header, string value)
    {
        var table = MarkdownTable.Read(page, section, FormalCoverageAreaCensus.PagePath);
        var row = Row(table, label);
        var cells = row.Cells.ToArray();
        cells[table.ColumnOf(header)] = value;

        var lines = page.ReplaceLineEndings("\n").Split('\n');
        lines[row.LineNumber - 1] = "| " + string.Join(" | ", cells) + " |";
        return string.Join("\n", lines);
    }

    private static string RemoveRow(string page, string section, string label)
    {
        var row = Row(MarkdownTable.Read(page, section, FormalCoverageAreaCensus.PagePath), label);
        var lines = page.ReplaceLineEndings("\n").Split('\n').ToList();
        lines.RemoveAt(row.LineNumber - 1);
        return string.Join("\n", lines);
    }
}
