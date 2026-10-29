using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Builds and writes the refinement index, contract C6 of the formal
/// verification pipeline epic (#5148, item #5155):
/// <code>
/// { "schema": 1, "modules": [ { "module", "note",
///   "rows": [ { "section", "label", "production": [...], "detectors": [...] } ],
///   "gaps": [ { "title", "symbols": [...] } ] } ] }
/// </code>
/// <para>
/// WHY IT EXISTS. The formal-impact report (#5162) runs without .NET and has to
/// know which production symbols and detectors each refinement row names, and
/// which symbols each deliberate abstraction gap names. A second parser of the
/// notes in Python would drift from the one the gates use, so the index is
/// built here, from the gates' own readers: <see cref="RefinementNote"/> for the
/// tables, <see cref="RefinementCodeSymbols"/> for the dotted references, and
/// <see cref="RefinementSymbolResolver"/> to decide whether a bare backticked
/// name is a production type.
/// </para>
/// <para>
/// TLC-NEUTRAL BY CONSTRUCTION. Every <c>RefinementIndex*</c> file is listed
/// neutral in <c>.github/workflows/tlc-inputs.json</c>: it depends on the
/// harness, and nothing the TLC path runs depends on it, so a change here
/// cannot alter a verdict and does not buy an exhaustive TLC run.
/// </para>
/// <para>
/// DETERMINISM. Modules are ordered by name, rows by section (in the order of
/// <see cref="RowSections"/>) then label, gaps by title, and every symbol list
/// ordinally without duplicates, all with ordinal comparison. The same tree
/// therefore always serialises to the same bytes.
/// </para>
/// </summary>
internal static class RefinementIndex
{
    /// <summary>The environment variable naming the file the index is written to.</summary>
    public const string EnvironmentVariable = "LATTICE_REFINEMENT_INDEX";

    /// <summary>The contract version written to <c>schema</c>.</summary>
    public const int SchemaVersion = 1;

    /// <summary>Heading of a note's optional section of deliberate abstraction gaps.</summary>
    public const string GapSection = "Deliberate abstraction gaps";

    /// <summary>
    /// The sections whose table rows the index records, in output order: the
    /// three mapping tables every note has, and the optional exclusion table.
    /// </summary>
    public static IReadOnlyList<string> RowSections { get; } =
    [
        RefinementNote.VariableSection,
        RefinementNote.ActionSection,
        RefinementNote.PropertySection,
        RefinementPropertyCoverageTests.ExclusionSection,
    ];

    private static readonly Regex BacktickedSpan = new("`([^`]+)`", RegexOptions.Compiled);

    private static readonly Regex BareTypeName = new(@"^[A-Z][A-Za-z0-9_]*$", RegexOptions.Compiled);

    private static readonly Regex BoldLeadIn = new(@"^\*\*(?<title>.+?)\*\*", RegexOptions.Compiled);

    private static readonly Regex TopLevelBullet = new(@"^[-*]\s+", RegexOptions.Compiled);

    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.Compiled);

    /// <summary>Builds the index over <paramref name="modules"/>, ordered by module name.</summary>
    public static IReadOnlyList<RefinementIndexModule> Build(
        IEnumerable<SpecModule> modules,
        RefinementSymbolResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(modules);
        ArgumentNullException.ThrowIfNull(resolver);

        return modules
            .Select(module => BuildModule(module, resolver))
            .OrderBy(entry => entry.Module, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>Builds one module's entry from its refinement note on disk.</summary>
    public static RefinementIndexModule BuildModule(SpecModule module, RefinementSymbolResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(module);
        return BuildModule(module.Name, module.Describe(module.RefinementNotePath), module.ReadRefinementNote(), resolver);
    }

    /// <summary>
    /// Builds one module's entry from a note's text. Exposed separately so the
    /// extraction can be driven over hand-written notes.
    /// </summary>
    /// <param name="moduleName">The module name.</param>
    /// <param name="notePath">The note's repository-relative path.</param>
    /// <param name="markdown">The note's text.</param>
    /// <param name="resolver">Decides which backticked names are production types.</param>
    public static RefinementIndexModule BuildModule(
        string moduleName,
        string notePath,
        string markdown,
        RefinementSymbolResolver resolver)
    {
        ArgumentException.ThrowIfNullOrEmpty(moduleName);
        ArgumentException.ThrowIfNullOrEmpty(notePath);
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentNullException.ThrowIfNull(resolver);

        var tables = RefinementNote.ParseTables(markdown, notePath);
        var rows = new List<RefinementIndexRow>();

        for (var order = 0; order < RowSections.Count; order++)
        {
            if (tables.TryGetValue(RowSections[order], out var table))
            {
                rows.AddRange(ReadRows(table, resolver));
            }
        }

        var gaps = ParseGapBlocks(markdown)
            .Select(block => new RefinementIndexGap(block.Title, GapSymbols(block.Text, resolver)))
            .OrderBy(gap => gap.Title, StringComparer.Ordinal)
            .ToArray();

        return new RefinementIndexModule(moduleName, notePath, rows, gaps);
    }

    /// <summary>
    /// Splits a note's <c>## Deliberate abstraction gaps</c> section into its
    /// entries. When the section has top-level bullets, each bullet (with its
    /// indented continuation lines) is one entry and any other paragraph is
    /// framing; otherwise each paragraph is one entry. A note without the
    /// section yields nothing.
    /// </summary>
    /// <param name="markdown">The note's text.</param>
    public static IReadOnlyList<(string Title, string Text)> ParseGapBlocks(string markdown)
    {
        ArgumentNullException.ThrowIfNull(markdown);

        var section = new List<string>();
        var inside = false;
        foreach (var line in markdown.ReplaceLineEndings("\n").Split('\n'))
        {
            if (line.TrimStart().StartsWith("##", StringComparison.Ordinal))
            {
                inside = string.Equals(line.Trim().TrimStart('#').Trim(), GapSection, StringComparison.Ordinal);
                continue;
            }

            if (inside)
            {
                section.Add(line);
            }
        }

        var bulleted = section.Any(line => TopLevelBullet.IsMatch(line));
        var blocks = new List<List<string>>();
        List<string>? current = null;
        var blankSinceLast = false;

        foreach (var line in section)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                blankSinceLast = true;
                if (!bulleted)
                {
                    current = null;
                }

                continue;
            }

            var indented = char.IsWhiteSpace(line[0]);
            if (bulleted)
            {
                if (TopLevelBullet.IsMatch(line))
                {
                    current = [TopLevelBullet.Replace(line, string.Empty, 1)];
                    blocks.Add(current);
                }
                else if (current is not null && (indented || !blankSinceLast))
                {
                    current.Add(line);
                }
                else
                {
                    current = null;
                }
            }
            else
            {
                if (current is null)
                {
                    current = [];
                    blocks.Add(current);
                }

                current.Add(line);
            }

            blankSinceLast = false;
        }

        return blocks
            .Select(lines => Whitespace.Replace(string.Join(" ", lines.Select(l => l.Trim())), " ").Trim())
            .Where(text => text.Length > 0)
            .Select(text => (TitleOf(text), text))
            .ToArray();
    }

    /// <summary>Serialises <paramref name="modules"/> as contract C6, indented, LF line endings, trailing newline.</summary>
    public static string ToJson(IReadOnlyList<RefinementIndexModule> modules)
    {
        ArgumentNullException.ThrowIfNull(modules);

        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions
        {
            Indented = true,
            NewLine = "\n",
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        }))
        {
            writer.WriteStartObject();
            writer.WriteNumber("schema", SchemaVersion);
            writer.WriteStartArray("modules");
            foreach (var module in modules)
            {
                writer.WriteStartObject();
                writer.WriteString("module", module.Module);
                writer.WriteString("note", module.Note);
                writer.WriteStartArray("rows");
                foreach (var row in module.Rows)
                {
                    writer.WriteStartObject();
                    writer.WriteString("section", row.Section);
                    writer.WriteString("label", row.Label);
                    WriteStrings(writer, "production", row.Production);
                    WriteStrings(writer, "detectors", row.Detectors);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteStartArray("gaps");
                foreach (var gap in module.Gaps)
                {
                    writer.WriteStartObject();
                    writer.WriteString("title", gap.Title);
                    WriteStrings(writer, "symbols", gap.Symbols);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray()) + "\n";
    }

    /// <summary>
    /// Writes the index to the file <see cref="EnvironmentVariable"/> names,
    /// creating its directory. Returns the path written, or null when the
    /// variable is unset or blank, in which case nothing is written.
    /// </summary>
    /// <param name="json">The serialised index.</param>
    public static string? WriteIfRequested(string json) =>
        WriteTo(json, Environment.GetEnvironmentVariable(EnvironmentVariable));

    /// <summary>
    /// Writes <paramref name="json"/> to <paramref name="target"/> as UTF-8
    /// without a byte-order mark, creating its directory. Returns the full path
    /// written, or null when <paramref name="target"/> is null or blank.
    /// </summary>
    /// <param name="json">The serialised index.</param>
    /// <param name="target">The file to write, or null to write nothing.</param>
    public static string? WriteTo(string json, string? target)
    {
        ArgumentNullException.ThrowIfNull(json);

        if (string.IsNullOrWhiteSpace(target))
        {
            return null;
        }

        var path = Path.GetFullPath(target);
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(path, json, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        return path;
    }

    /// <summary>
    /// Checks a serialised index against contract C6 and its ordering rules,
    /// returning every problem found; empty means the document conforms.
    /// </summary>
    /// <param name="json">The document to check.</param>
    public static IReadOnlyList<string> SchemaProblems(string json)
    {
        ArgumentNullException.ThrowIfNull(json);

        var problems = new List<string>();
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException exception)
        {
            return [$"the document is not JSON: {exception.Message}"];
        }

        using (document)
        {
            var root = document.RootElement;
            if (!HasExactly(root, "$", problems, "schema", "modules"))
            {
                return problems;
            }

            if (root.GetProperty("schema").ValueKind != JsonValueKind.Number
                || !root.GetProperty("schema").TryGetInt32(out var schema)
                || schema != SchemaVersion)
            {
                problems.Add($"$.schema must be the number {SchemaVersion}");
            }

            var modules = root.GetProperty("modules");
            if (modules.ValueKind != JsonValueKind.Array)
            {
                problems.Add("$.modules must be an array");
                return problems;
            }

            var names = new List<string>();
            var m = 0;
            foreach (var module in modules.EnumerateArray())
            {
                var at = $"$.modules[{m++}]";
                if (!HasExactly(module, at, problems, "module", "note", "rows", "gaps"))
                {
                    continue;
                }

                var name = RequireString(module, "module", at, problems);
                if (name is not null)
                {
                    names.Add(name);
                }

                RequireString(module, "note", at, problems);
                CheckRows(module.GetProperty("rows"), at + ".rows", problems);
                CheckGaps(module.GetProperty("gaps"), at + ".gaps", problems);
            }

            RequireSorted(names, "$.modules[*].module", problems, strict: true);
        }

        return problems;
    }

    private static IEnumerable<RefinementIndexRow> ReadRows(RefinementTable table, RefinementSymbolResolver resolver)
    {
        var detectorColumn = -1;
        for (var i = 0; i < table.Headers.Count; i++)
        {
            if (string.Equals(table.Headers[i], RefinementCodeSymbols.DetectorHeader, StringComparison.OrdinalIgnoreCase))
            {
                detectorColumn = i;
                break;
            }
        }

        var dotted = RefinementCodeSymbols.Extract([table]).ToLookup(s => s.LineNumber);
        var detectors = RefinementCodeSymbols.ExtractDetectors([table]).ToLookup(s => s.LineNumber);

        return table.Rows
            .Select(row =>
            {
                var bare = row.Cells
                    .Where((_, column) => column != 0 && column != detectorColumn)
                    .SelectMany(cell => BareTypes(cell, resolver));
                return new RefinementIndexRow(
                    row.Section,
                    row.Label,
                    Distinct(dotted[row.LineNumber].Select(s => s.Text).Concat(bare)),
                    Distinct(detectors[row.LineNumber].Select(s => s.Text)));
            })
            .OrderBy(row => row.Label, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>
    /// The production symbols a gap names. Its text is presented to the
    /// existing extractor as a one-cell table, so the dotted form is read by
    /// exactly the pattern the mapping rows use; because gap prose also cites
    /// tests in that form, a dotted reference is kept only when its type is
    /// declared under <c>src/</c>.
    /// </summary>
    private static IReadOnlyList<string> GapSymbols(string text, RefinementSymbolResolver resolver)
    {
        var table = new RefinementTable(GapSection, ["Gap"], [new RefinementRow(GapSection, 1, [text])]);
        var dotted = RefinementCodeSymbols.Extract([table])
            .Where(symbol => resolver.TypeExists(symbol.TypeName))
            .Select(symbol => symbol.Text);
        return Distinct(dotted.Concat(BareTypes(text, resolver)));
    }

    private static IEnumerable<string> BareTypes(string text, RefinementSymbolResolver resolver) =>
        BacktickedSpan.Matches(text)
            .Select(span => span.Groups[1].Value.Trim())
            .Where(name => BareTypeName.IsMatch(name) && resolver.TypeExists(name));

    private static string TitleOf(string text)
    {
        var bold = BoldLeadIn.Match(text);
        if (bold.Success)
        {
            return bold.Groups["title"].Value.Trim().TrimEnd('.', ':').Trim();
        }

        var end = text.IndexOf(". ", StringComparison.Ordinal);
        return (end >= 0 ? text[..end] : text).TrimEnd('.').Trim();
    }

    private static IReadOnlyList<string> Distinct(IEnumerable<string> values) =>
        values.Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();

    private static void WriteStrings(Utf8JsonWriter writer, string name, IReadOnlyList<string> values)
    {
        writer.WriteStartArray(name);
        foreach (var value in values)
        {
            writer.WriteStringValue(value);
        }

        writer.WriteEndArray();
    }

    private static void CheckRows(JsonElement rows, string at, List<string> problems)
    {
        if (rows.ValueKind != JsonValueKind.Array)
        {
            problems.Add($"{at} must be an array");
            return;
        }

        var keys = new List<string>();
        var r = 0;
        foreach (var row in rows.EnumerateArray())
        {
            var rowAt = $"{at}[{r++}]";
            if (!HasExactly(row, rowAt, problems, "section", "label", "production", "detectors"))
            {
                continue;
            }

            var section = RequireString(row, "section", rowAt, problems);
            var label = RequireString(row, "label", rowAt, problems);
            var order = section is null ? -1 : IndexOf(RowSections, section);
            if (section is not null && order < 0)
            {
                problems.Add($"{rowAt}.section '{section}' is not one of: {string.Join(", ", RowSections)}");
            }

            if (order >= 0 && label is not null)
            {
                keys.Add($"{order:D2}|{label}");
            }

            CheckStrings(row.GetProperty("production"), rowAt + ".production", problems);
            CheckStrings(row.GetProperty("detectors"), rowAt + ".detectors", problems);
        }

        RequireSorted(keys, at + " (by section, then label)", problems, strict: false);
    }

    private static void CheckGaps(JsonElement gaps, string at, List<string> problems)
    {
        if (gaps.ValueKind != JsonValueKind.Array)
        {
            problems.Add($"{at} must be an array");
            return;
        }

        var titles = new List<string>();
        var g = 0;
        foreach (var gap in gaps.EnumerateArray())
        {
            var gapAt = $"{at}[{g++}]";
            if (!HasExactly(gap, gapAt, problems, "title", "symbols"))
            {
                continue;
            }

            var title = RequireString(gap, "title", gapAt, problems);
            if (title is not null)
            {
                titles.Add(title);
            }

            CheckStrings(gap.GetProperty("symbols"), gapAt + ".symbols", problems);
        }

        RequireSorted(titles, at + " (by title)", problems, strict: false);
    }

    private static void CheckStrings(JsonElement values, string at, List<string> problems)
    {
        if (values.ValueKind != JsonValueKind.Array)
        {
            problems.Add($"{at} must be an array of strings");
            return;
        }

        var strings = new List<string>();
        foreach (var value in values.EnumerateArray())
        {
            if (value.ValueKind != JsonValueKind.String || string.IsNullOrEmpty(value.GetString()))
            {
                problems.Add($"{at} must hold only non-empty strings");
                return;
            }

            strings.Add(value.GetString()!);
        }

        RequireSorted(strings, at, problems, strict: true);
    }

    private static bool HasExactly(JsonElement element, string at, List<string> problems, params string[] names)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            problems.Add($"{at} must be an object");
            return false;
        }

        var present = element.EnumerateObject().Select(p => p.Name).ToArray();
        var missing = names.Except(present, StringComparer.Ordinal).ToArray();
        var extra = present.Except(names, StringComparer.Ordinal).ToArray();
        if (missing.Length > 0)
        {
            problems.Add($"{at} is missing {string.Join(", ", missing)}");
        }

        if (extra.Length > 0)
        {
            problems.Add($"{at} has properties the contract does not define: {string.Join(", ", extra)}");
        }

        return missing.Length == 0;
    }

    private static string? RequireString(JsonElement element, string name, string at, List<string> problems)
    {
        var value = element.GetProperty(name);
        if (value.ValueKind != JsonValueKind.String || string.IsNullOrEmpty(value.GetString()))
        {
            problems.Add($"{at}.{name} must be a non-empty string");
            return null;
        }

        return value.GetString();
    }

    private static void RequireSorted(List<string> values, string at, List<string> problems, bool strict)
    {
        for (var i = 1; i < values.Count; i++)
        {
            var comparison = string.CompareOrdinal(values[i - 1], values[i]);
            if (comparison > 0 || (strict && comparison == 0))
            {
                problems.Add(
                    $"{at} must be sorted ordinally{(strict ? " without duplicates" : string.Empty)}: "
                    + $"'{values[i - 1]}' precedes '{values[i]}'");
                return;
            }
        }
    }

    private static int IndexOf(IReadOnlyList<string> values, string value)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (string.Equals(values[i], value, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }
}
