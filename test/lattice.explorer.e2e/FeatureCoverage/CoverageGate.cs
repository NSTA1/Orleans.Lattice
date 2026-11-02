namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// The runtime feature-coverage gate (#5361): unions the hits of the story runs and the isolated
/// runs, and finds every catalogue feature with no hit that <c>coverage-exemptions.tsv</c> does
/// not exempt.
/// </summary>
public static class CoverageGate
{
    /// <summary>
    /// Whether an uncovered feature fails the gate. Off until I1 (#5400), which turns it on once
    /// every journey has landed; until then the gate reports without failing.
    /// </summary>
    public static bool Hard => false;

    /// <summary>Evaluates <paramref name="hits"/> against the catalogue and its exemptions.</summary>
    /// <param name="catalogue">The catalogue.</param>
    /// <param name="exemptions">The exemptions.</param>
    /// <param name="hits">The union of the runs' hits.</param>
    public static CoverageReport Evaluate(FeatureCatalogue catalogue, IReadOnlyList<CoverageExemption> exemptions, IReadOnlySet<string> hits)
    {
        ArgumentNullException.ThrowIfNull(catalogue);
        ArgumentNullException.ThrowIfNull(exemptions);
        ArgumentNullException.ThrowIfNull(hits);
        var exempt = exemptions.Select(exemption => exemption.Id).Where(catalogue.Contains).ToHashSet(StringComparer.Ordinal);
        var covered = hits.Where(catalogue.Contains).ToHashSet(StringComparer.Ordinal);
        return new CoverageReport(
            catalogue.Rows,
            covered,
            exempt,
            [.. catalogue.Rows.Where(row => !covered.Contains(row.Id) && !exempt.Contains(row.Id))],
            [.. exemptions.Select(exemption => exemption.Id).Where(id => !catalogue.Contains(id))],
            [.. hits.Where(id => !catalogue.Contains(id)).Order(StringComparer.Ordinal)]);
    }
}
