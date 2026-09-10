using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One data row of a mapping table in <c>spec/Refinement.md</c>.
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
/// One markdown table in <c>spec/Refinement.md</c>, with the section heading
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
/// Reads <c>spec/Refinement.md</c> and returns its mapping tables as
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

    /// <summary>Absolute path of the refinement note.</summary>
    public static string NotePath =>
        Path.Combine(HygieneRepository.FindRepoRoot(), "spec", "Refinement.md");

    /// <summary>Reads the note's raw markdown.</summary>
    public static string ReadText() => File.ReadAllText(NotePath);

    /// <summary>
    /// Reads every mapping table in the note on disk, keyed by section
    /// heading. Throws when one of <see cref="MappingSections"/> is missing or
    /// yields no rows.
    /// </summary>
    public static IReadOnlyDictionary<string, RefinementTable> ReadTables() =>
        ParseTables(ReadText());

    /// <summary>The rows of the spec-variable mapping table.</summary>
    public static IReadOnlyList<RefinementRow> ReadVariableRows() => ReadRows(VariableSection);

    /// <summary>The rows of the spec-action mapping table.</summary>
    public static IReadOnlyList<RefinementRow> ReadActionRows() => ReadRows(ActionSection);

    /// <summary>The rows of the spec-property mapping table.</summary>
    public static IReadOnlyList<RefinementRow> ReadPropertyRows() => ReadRows(PropertySection);

    /// <summary>The rows of every mapping table, in document order.</summary>
    public static IReadOnlyList<RefinementRow> ReadAllRows()
    {
        var tables = ReadTables();
        return MappingSections.SelectMany(section => tables[section].Rows).ToArray();
    }

    private static IReadOnlyList<RefinementRow> ReadRows(string section) => ReadTables()[section].Rows;

    /// <summary>
    /// Parses mapping tables out of arbitrary markdown. Exposed separately
    /// from <see cref="ReadTables"/> so the parser can be tested against
    /// hand-written input, including the inputs it is supposed to reject.
    /// </summary>
    public static IReadOnlyDictionary<string, RefinementTable> ParseTables(string markdown)
    {
        ArgumentNullException.ThrowIfNull(markdown);

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
                    $"spec/Refinement.md section '{section}' contains more than one markdown table. "
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
                    $"spec/Refinement.md has no markdown table under a '## {expected}' heading. "
                    + "Either the heading was renamed or the table was removed; the gates over this "
                    + "note read it structurally and cannot continue. Update RefinementNote's section "
                    + "constants to match the note, or restore the table.");
            }

            if (table.Rows.Count == 0)
            {
                throw new InvalidOperationException(
                    $"spec/Refinement.md's '{expected}' table parsed to zero rows. That would make every "
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
