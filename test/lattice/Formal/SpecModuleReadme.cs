using System.Globalization;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Reads the two machine-checked tables the <c>spec/</c> READMEs carry: the
/// <c>## Counts</c> table in each module directory's README, and the
/// <c>## Modules</c> index in <c>spec/README.md</c>.
/// <para>
/// The counts table is the one place a module's README states its current
/// totals, so it is the prose the manifest is checked against. Prose
/// elsewhere in a README that states a current total is unchecked and should
/// cite the table instead; historical counts ("when it was added, TLC found
/// ...") are history, not claims about today.
/// </para>
/// </summary>
public static class SpecModuleReadme
{
    /// <summary>Heading of the per-module counts table in a module directory's README.</summary>
    public const string CountsSection = "Counts";

    /// <summary>Heading of the module index in <c>spec/README.md</c>.</summary>
    public const string IndexSection = "Modules";

    /// <summary>The counts table's header row, in order.</summary>
    public static IReadOnlyList<string> CountsHeaders { get; } =
        ["Module", "Invariants", "Properties", "Actions", "Mutations", "Behaviour rows", "Distinct states"];

    /// <summary>
    /// The counts table of a module directory's README, keyed by module name.
    /// Throws when the section is absent, its header differs from
    /// <see cref="CountsHeaders"/>, a cell is not a number, or a module has two
    /// rows.
    /// </summary>
    /// <param name="readme">The README's text.</param>
    /// <param name="source">The README's path, for error messages.</param>
    public static IReadOnlyDictionary<string, SpecModuleCounts> ReadCounts(string readme, string source)
    {
        var (headers, rows) = ReadTable(readme, CountsSection, source);
        if (!headers.SequenceEqual(CountsHeaders, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                $"{source}'s '## {CountsSection}' table must have the columns "
                + $"[{string.Join(" | ", CountsHeaders)}], not [{string.Join(" | ", headers)}].");
        }

        var counts = new Dictionary<string, SpecModuleCounts>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            if (row.Count != CountsHeaders.Count)
            {
                throw new InvalidOperationException(
                    $"{source}'s '## {CountsSection}' table has a row with {row.Count} cells: [{string.Join(" | ", row)}].");
            }

            var name = row[0].Trim('`');
            if (!counts.TryAdd(
                    name,
                    new SpecModuleCounts(
                        (int)Number(row[1], source),
                        (int)Number(row[2], source),
                        (int)Number(row[3], source),
                        (int)Number(row[4], source),
                        (int)Number(row[5], source),
                        Number(row[6], source))))
            {
                throw new InvalidOperationException($"{source}'s '## {CountsSection}' table lists '{name}' twice.");
            }
        }

        return counts;
    }

    /// <summary>
    /// The module names the <c>spec/README.md</c> index lists, from the
    /// backticked name in each row's second column. Throws when the section is
    /// absent or empty.
    /// </summary>
    /// <param name="readme">The index README's text.</param>
    /// <param name="source">Its path, for error messages.</param>
    public static IReadOnlyList<string> ReadIndex(string readme, string source)
    {
        var (headers, rows) = ReadTable(readme, IndexSection, source);
        if (headers.Count < 2 || !string.Equals(headers[1], "Module", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"{source}'s '## {IndexSection}' table must name the module in its second column, headed 'Module'.");
        }

        return rows.Select(r => r.Count > 1 ? r[1].Trim('`') : string.Empty).ToArray();
    }

    private static long Number(string cell, string source) =>
        long.TryParse(cell.Replace(",", string.Empty, StringComparison.Ordinal), NumberStyles.None, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new InvalidOperationException($"{source}: '{cell}' in the '## {CountsSection}' table is not a whole number.");

    private static (IReadOnlyList<string> Headers, IReadOnlyList<IReadOnlyList<string>> Rows) ReadTable(
        string markdown,
        string section,
        string source)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentException.ThrowIfNullOrEmpty(source);

        var lines = markdown.ReplaceLineEndings("\n").Split('\n');
        var heading = Array.FindIndex(lines, l => string.Equals(l.Trim(), $"## {section}", StringComparison.Ordinal));
        if (heading < 0)
        {
            throw new InvalidOperationException($"{source} has no '## {section}' section.");
        }

        List<string>? headers = null;
        var rows = new List<IReadOnlyList<string>>();
        for (var i = heading + 1; i < lines.Length; i++)
        {
            var line = lines[i].Trim();
            if (line.StartsWith("#", StringComparison.Ordinal))
            {
                break;
            }

            if (!line.StartsWith('|'))
            {
                if (headers is not null)
                {
                    break;
                }

                continue;
            }

            var cells = line.Trim('|').Split('|').Select(c => c.Trim()).ToList();
            if (headers is null)
            {
                headers = cells;
            }
            else if (!cells.All(c => c.Length > 0 && c.All(ch => ch is '-' or ':')))
            {
                rows.Add(cells);
            }
        }

        if (headers is null || rows.Count == 0)
        {
            throw new InvalidOperationException(
                $"{source}'s '## {section}' section has no table rows, so the gate reading it would check nothing.");
        }

        return (headers, rows);
    }
}
