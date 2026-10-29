namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// The first markdown table under a <c>##</c> heading of a page, read
/// structurally so a census gate can address cells by header text.
/// <para>
/// It fails loudly rather than returning an empty or ragged table: a missing
/// heading, a section with no table, a table with no data rows, and a row whose
/// cell count differs from the header's all throw. A gate driven by an empty or
/// misaligned table would pass while checking nothing, or check the wrong cell.
/// </para>
/// </summary>
/// <param name="Section">The heading the table sits under.</param>
/// <param name="Headers">The header row's cells.</param>
/// <param name="Rows">The data rows, excluding the alignment row.</param>
internal sealed record MarkdownTable(string Section, IReadOnlyList<string> Headers, IReadOnlyList<MarkdownTableRow> Rows)
{
    /// <summary>The index of the column headed <paramref name="header"/>; throws when there is none.</summary>
    /// <param name="header">The exact header text.</param>
    public int ColumnOf(string header)
    {
        for (var i = 0; i < Headers.Count; i++)
        {
            if (string.Equals(Headers[i], header, StringComparison.Ordinal))
            {
                return i;
            }
        }

        throw new InvalidOperationException(
            $"the '## {Section}' table has no '{header}' column; its columns are [{string.Join(" | ", Headers)}].");
    }

    /// <summary>Reads the first table under the <c>## <paramref name="section"/></c> heading.</summary>
    /// <param name="markdown">The page's text.</param>
    /// <param name="section">The heading text, without the leading hashes.</param>
    /// <param name="source">The page's path, for error messages.</param>
    public static MarkdownTable Read(string markdown, string section, string source)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentException.ThrowIfNullOrEmpty(section);
        ArgumentException.ThrowIfNullOrEmpty(source);

        var lines = markdown.ReplaceLineEndings("\n").Split('\n');
        var heading = Array.FindIndex(lines, l => string.Equals(l.Trim(), $"## {section}", StringComparison.Ordinal));
        if (heading < 0)
        {
            throw new InvalidOperationException($"{source} has no '## {section}' section.");
        }

        List<string>? headers = null;
        var rows = new List<MarkdownTableRow>();
        for (var i = heading + 1; i < lines.Length; i++)
        {
            var line = lines[i].Trim();
            if (line.StartsWith("## ", StringComparison.Ordinal))
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
                continue;
            }

            if (cells.All(c => c.Length > 0 && c.All(ch => ch is '-' or ':')))
            {
                continue;
            }

            if (cells.Count != headers.Count)
            {
                throw new InvalidOperationException(
                    $"{source} line {i + 1}: a '## {section}' table row has {cells.Count} cells where the header "
                    + $"has {headers.Count}: [{string.Join(" | ", cells)}].");
            }

            rows.Add(new MarkdownTableRow(i + 1, cells));
        }

        if (headers is null || rows.Count == 0)
        {
            throw new InvalidOperationException(
                $"{source}'s '## {section}' section has no table rows, so a gate reading it would check nothing.");
        }

        return new MarkdownTable(section, headers, rows);
    }
}
