using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Reports;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// The runtime feature-coverage gate (#5361): unions every <c>coverage-hits*.json</c> under
/// <c>LATTICE_E2E_COVERAGE_HITS</c> (default: this project's <c>artifacts/</c>) with this run's
/// own hits, and reports every catalogue feature with neither a hit nor an exemption. It reports
/// without failing until I1 (#5400) turns <see cref="CoverageGate.Hard"/> on.
/// </summary>
/// <remarks>
/// It carries <c>CoverageGate</c> so a lane can run it alone, after the isolated and story legs,
/// over their downloaded hit files; a journey leg excludes it.
/// </remarks>
[TestFixture]
[Category("EndToEnd")]
[Category("CoverageGate")]
public sealed class CoverageGateTests
{
    /// <summary>The directory whose hit files the gate unions.</summary>
    public static string HitsDirectory =>
        Environment.GetEnvironmentVariable(CoverageHitsDocument.DirectoryVariable) is { Length: > 0 } directory
            ? directory
            : Path.Combine(JourneyPaths.ProjectDirectory, "artifacts");

    /// <summary>Evaluates the gate, publishes the report, and fails on a stale exemption (and, once hard, on an uncovered feature).</summary>
    [Test]
    public void Every_catalogue_feature_has_a_runtime_hit_or_an_exemption()
    {
        var catalogue = FeatureCatalogue.Load();
        var hits = new HashSet<string>(CoverageHitsDocument.UnionUnder(HitsDirectory), StringComparer.Ordinal);
        hits.UnionWith(CoverageRecorder.Default.Hits.Keys);
        var report = CoverageGate.Evaluate(catalogue, FeatureCatalogue.LoadExemptions(), hits);
        var markdown = report.ToMarkdown(CoverageGate.Hard);
        Directory.CreateDirectory(Path.Combine(JourneyPaths.ProjectDirectory, "artifacts"));
        File.WriteAllText(Path.Combine(JourneyPaths.ProjectDirectory, "artifacts", "coverage-report.md"), markdown);
        JobSummary.Append(markdown);
        TestContext.Out.WriteLine(markdown);

        Assert.That(report.UnknownExemptions, Is.Empty, "coverage-exemptions.tsv exempts features the catalogue lacks");
        if (CoverageGate.Hard)
        {
            Assert.That(report.Uncovered.Select(row => row.Id), Is.Empty, "catalogue features with no runtime hit");
        }
    }
}
