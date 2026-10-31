using System.Text;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>What the coverage gate found: every catalogue feature as covered, exempt or uncovered.</summary>
/// <param name="Rows">The catalogue's rows.</param>
/// <param name="Covered">Ids with a runtime hit.</param>
/// <param name="Exempt">Ids exempted, with no hit required.</param>
/// <param name="Uncovered">Rows with neither a hit nor an exemption, in catalogue order.</param>
/// <param name="UnknownExemptions">Exempted ids the catalogue lacks.</param>
/// <param name="UnknownHits">Hit ids the catalogue lacks (a file from an older catalogue).</param>
public sealed record CoverageReport(
    IReadOnlyList<FeatureRow> Rows,
    IReadOnlySet<string> Covered,
    IReadOnlySet<string> Exempt,
    IReadOnlyList<FeatureRow> Uncovered,
    IReadOnlyList<string> UnknownExemptions,
    IReadOnlyList<string> UnknownHits)
{
    /// <summary>Whether every row is covered or exempt, and no exemption is stale.</summary>
    public bool IsComplete => Uncovered.Count == 0 && UnknownExemptions.Count == 0;

    /// <summary>Renders the report for a CI job summary.</summary>
    /// <param name="hard">Whether the gate fails on an uncovered row.</param>
    public string ToMarkdown(bool hard)
    {
        var markdown = new StringBuilder();
        markdown.AppendLine("## Explorer feature coverage");
        markdown.AppendLine();
        markdown.AppendLine(hard
            ? "The gate is **hard**: an uncovered feature fails the run."
            : "The gate **reports without failing** until I1 (#5400) makes it hard.");
        markdown.AppendLine();
        markdown.AppendLine($"**{Covered.Count} of {Rows.Count}** features covered, {Exempt.Count} exempt, **{Uncovered.Count} uncovered**.");
        markdown.AppendLine();
        markdown.AppendLine("| Kind | Features | Covered | Exempt | Uncovered |");
        markdown.AppendLine("|---|---:|---:|---:|---:|");
        foreach (var kind in Rows.GroupBy(row => row.Kind).OrderBy(group => group.Key, StringComparer.Ordinal))
        {
            markdown.AppendLine($"| {kind.Key} | {kind.Count()} | {kind.Count(row => Covered.Contains(row.Id))} | {kind.Count(row => Exempt.Contains(row.Id))} | {kind.Count(row => Uncovered.Contains(row))} |");
        }

        markdown.AppendLine();
        markdown.AppendLine("| Owning chapter | Uncovered |");
        markdown.AppendLine("|---|---:|");
        foreach (var journey in Uncovered.GroupBy(row => row.Journey).OrderBy(group => group.Key, StringComparer.Ordinal))
        {
            markdown.AppendLine($"| {journey.Key} | {journey.Count()} |");
        }

        markdown.AppendLine();
        foreach (var journey in Uncovered.GroupBy(row => row.Journey).OrderBy(group => group.Key, StringComparer.Ordinal))
        {
            markdown.AppendLine($"<details><summary>{journey.Key}: {journey.Count()} uncovered</summary>");
            markdown.AppendLine();
            foreach (var row in journey)
            {
                markdown.AppendLine($"- `{row.Id}` - {row.Description}");
            }

            markdown.AppendLine();
            markdown.AppendLine("</details>");
        }

        if (Exempt.Count > 0)
        {
            markdown.AppendLine();
            markdown.AppendLine("Exempt: " + string.Join(", ", Exempt.Order(StringComparer.Ordinal).Select(id => $"`{id}`")) + ".");
        }

        foreach (var (label, ids) in new[] { ("Stale exemptions (not in the catalogue)", UnknownExemptions), ("Hits for features the catalogue lacks", UnknownHits) })
        {
            if (ids.Count > 0)
            {
                markdown.AppendLine();
                markdown.AppendLine($"**{label}:** " + string.Join(", ", ids.Select(id => $"`{id}`")) + ".");
            }
        }

        return markdown.ToString();
    }
}
