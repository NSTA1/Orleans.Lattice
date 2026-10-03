using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One data row of a mapping table in a module's refinement note.
/// </summary>
/// <param name="Section">The <c>##</c> heading the row sits under.</param>
/// <param name="LineNumber">The 1-based line in the note, for failure messages.</param>
/// <param name="Cells">The row's cells, in column order, trimmed.</param>
internal sealed record RefinementRow(string Section, int LineNumber, IReadOnlyList<string> Cells)
{
    /// <summary>
    /// The row's first cell. Every table in the note uses column one as the
    /// name of the spec construct the row is about, so this is the handle a
    /// failure message should quote.
    /// </summary>
    public string Label => Cells.Count > 0 ? Cells[0] : string.Empty;
}

/// <summary>
/// One markdown table in a module's refinement note, with the section heading
/// it appeared under.
/// </summary>
/// <param name="Section">The <c>##</c> heading the table sits under.</param>
/// <param name="Headers">The header row's cells, in column order.</param>
/// <param name="Rows">The data rows, excluding the alignment separator.</param>
internal sealed record RefinementTable(
    string Section,
    IReadOnlyList<string> Headers,
    IReadOnlyList<RefinementRow> Rows);

/// <summary>
/// Reads a module's refinement note (see <see cref="SpecModule"/>) and returns its mapping tables as
/// structured rows.
/// <para>
/// This is deliberately a shared reader rather than parsing inlined into one
/// test. More than one gate over the note is planned (this one checks that the
/// code symbols the rows name still exist; a follow-on checks that the rows'
/// behavioural claims have a detector), and two independently-drifting parsers
/// of the same markdown is the sort of duplication that ends with two gates
/// disagreeing about what the note says.
/// </para>
/// <para>
/// Two robustness properties are load-bearing, both learned in the mutation
/// harness that preceded this one. It is CRLF-safe: the text is normalised
/// before any line or string operation, because a checkout with CRLF endings
/// otherwise turns every anchored comparison into a silent non-match. And it
/// FAILS LOUDLY on an empty parse: a section that yields zero rows throws
/// rather than returning an empty list, because a gate driven by an empty list
/// passes while checking nothing, which is precisely the failure mode this
/// whole area exists to eliminate.
/// </para>
/// </summary>
internal static class RefinementNote
{
    /// <summary>Heading of the spec-variable to code mapping table.</summary>
    public const string VariableSection = "Variable mapping";

    /// <summary>Heading of the spec-action to code mapping table.</summary>
    public const string ActionSection = "Action mapping";

    /// <summary>Heading of the spec-property to code mapping table.</summary>
    public const string PropertySection = "Property mapping";

    /// <summary>The three sections every reader expects to be present.</summary>
    public static IReadOnlyList<string> MappingSections { get; } =
        new[] { VariableSection, ActionSection, PropertySection };

    private static readonly Regex AlignmentCell = new(@"^:?-{1,}:?$", RegexOptions.Compiled);

    /// <summary>
    /// Reads every mapping table in <paramref name="module"/>'s refinement
    /// note, keyed by section heading. Throws when one of
    /// <see cref="MappingSections"/> is missing or yields no rows.
    /// </summary>
    public static IReadOnlyDictionary<string, RefinementTable> ReadTables(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);
        return module.ReadRefinementTables();
    }

    /// <summary>The rows of <paramref name="module"/>'s spec-action mapping table.</summary>
    public static IReadOnlyList<RefinementRow> ReadActionRows(SpecModule module) =>
        ReadTables(module)[ActionSection].Rows;

    /// <summary>
    /// The labels of <paramref name="module"/>'s action rows for the actions
    /// its manifest declares non-behavioural. Throws unless each declared
    /// action has exactly one row, so a typo in the manifest cannot quietly
    /// exempt nothing - or a renamed row quietly stop being exempt.
    /// </summary>
    public static IReadOnlyList<string> NonBehaviouralRowLabels(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);

        var rows = ReadActionRows(module);
        var labels = new List<string>();
        foreach (var action in module.Manifest.NonBehaviouralActions)
        {
            var matches = rows.Where(r => SpecActions.ActionNameOf(r) == action).ToArray();
            if (matches.Length != 1)
            {
                throw new InvalidOperationException(
                    $"{module.Describe(module.ManifestPath)} declares '{action}' non-behavioural, but "
                    + $"{module.Describe(module.RefinementNotePath)}'s '{ActionSection}' table has "
                    + $"{matches.Length} rows for it. Exactly one is required.");
            }

            labels.Add(matches[0].Label);
        }

        return labels;
    }

    /// <summary>
    /// Parses mapping tables out of arbitrary markdown. Exposed separately
    /// from <see cref="ReadTables"/> so the parser can be tested against
    /// hand-written input, including the inputs it is supposed to reject.
    /// </summary>
    /// <param name="markdown">The note's text.</param>
    /// <param name="noteLabel">How to name the note in an error, such as its path.</param>
    public static IReadOnlyDictionary<string, RefinementTable> ParseTables(string markdown, string noteLabel = "the refinement note")
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentException.ThrowIfNullOrEmpty(noteLabel);

        var lines = markdown.ReplaceLineEndings("\n").Split('\n');
        var tables = new Dictionary<string, RefinementTable>(StringComparer.Ordinal);

        var section = string.Empty;
        List<string>? headers = null;
        List<RefinementRow>? rows = null;

        void Close()
        {
            if (headers is null || rows is null)
            {
                return;
            }

            if (tables.ContainsKey(section))
            {
                throw new InvalidOperationException(
                    $"{noteLabel} section '{section}' contains more than one markdown table. "
                    + "The readers here assume one mapping table per section; either merge them or "
                    + "give the second table its own '##' heading.");
            }

            tables[section] = new RefinementTable(section, headers, rows);
            headers = null;
            rows = null;
        }

        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].Trim();

            if (line.StartsWith("##", StringComparison.Ordinal))
            {
                Close();
                section = line.TrimStart('#').Trim();
                continue;
            }

            if (!line.StartsWith('|'))
            {
                Close();
                continue;
            }

            var cells = SplitCells(line);

            if (headers is null)
            {
                headers = cells;
                rows = new List<RefinementRow>();
                continue;
            }

            if (cells.All(c => AlignmentCell.IsMatch(c)))
            {
                continue;
            }

            rows!.Add(new RefinementRow(section, i + 1, cells));
        }

        Close();

        foreach (var expected in MappingSections)
        {
            if (!tables.TryGetValue(expected, out var table))
            {
                throw new InvalidOperationException(
                    $"{noteLabel} has no markdown table under a '## {expected}' heading. "
                    + "Either the heading was renamed or the table was removed; the gates over this "
                    + "note read it structurally and cannot continue. Update RefinementNote's section "
                    + "constants to match the note, or restore the table.");
            }

            if (table.Rows.Count == 0)
            {
                throw new InvalidOperationException(
                    $"{noteLabel}'s '{expected}' table parsed to zero rows. That would make every "
                    + "gate driven by this reader pass while checking nothing, so it is treated as a "
                    + "parser or authoring fault rather than as an empty mapping.");
            }
        }

        return tables;
    }

    private static List<string> SplitCells(string line)
    {
        var trimmed = line.Trim();
        if (trimmed.StartsWith('|'))
        {
            trimmed = trimmed[1..];
        }

        if (trimmed.EndsWith('|'))
        {
            trimmed = trimmed[..^1];
        }

        return trimmed.Split('|').Select(c => c.Trim()).ToList();
    }
}
