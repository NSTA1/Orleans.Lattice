using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// One production perturbation, <c>spec/&lt;area&gt;/perturbations/&lt;Name&gt;.perturbation</c>
/// (contract C7 of epic #5148): an anchored edit to one production file that
/// breaks the behaviour a refinement row describes, and the detectors that row
/// cites, at least one of which must go red when the edit is applied.
/// <para>
/// WHY THIS EXISTS. A spec mutation proves a TLA+ property can fire. Nothing
/// proved the converse on the code side: that the detector a refinement row
/// cites actually goes red when the production behaviour it abstracts breaks.
/// A perturbation is that experiment written down, so it can be re-run by
/// <c>tools/perturbations/run-perturbations.py</c> instead of being claimed
/// once by hand and left to decay as the code moves.
/// </para>
/// <para>
/// THE GRAMMAR IS THE SPEC-MUTATION GRAMMAR, deliberately: <c>KEY: value</c>
/// headers, then one or more <c>--- FIND</c> / <c>--- REPLACE</c> /
/// <c>--- END</c> blocks of verbatim text, with blank lines and <c>#</c>
/// comments allowed between blocks and every stray marker or stray text
/// refused rather than skipped. It is parsed here rather than through
/// <see cref="SpecMutationCatalogue.Parse"/> because the headers differ and
/// unknown headers are refused, and because this file must stay TLC-neutral:
/// it may use verdict-affecting Formal types, but none may use it.
/// </para>
/// <para>
/// The runner carries its own parser of the same grammar.
/// <c>RefinementPerturbationTests</c> holds the two to one answer by comparing
/// this parse with the runner's <c>--check --json</c> output for every
/// perturbation in the repository.
/// </para>
/// </summary>
public sealed record RefinementPerturbation
{
    /// <summary>The file extension of a perturbation.</summary>
    public const string Extension = ".perturbation";

    /// <summary>The name of the directory holding an area's perturbations.</summary>
    public const string DirectoryName = "perturbations";

    private const string FindMarker = "--- FIND";
    private const string ReplaceMarker = "--- REPLACE";
    private const string EndMarker = "--- END";

    /// <summary>The headers a perturbation must declare, each exactly once.</summary>
    public static IReadOnlyList<string> RequiredHeaders { get; } = ["MODULE", "ROW", "FILE", "DETECTORS", "SUMMARY"];

    private static readonly Regex DetectorShape = new(
        @"^[A-Z][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The perturbation's name: its file stem.</summary>
    public required string Name { get; init; }

    /// <summary>The TLA+ module whose refinement note holds <see cref="Row"/>.</summary>
    public required string Module { get; init; }

    /// <summary>
    /// The refinement row label the perturbation breaks, written as the row's
    /// first cell without its backticks, for example <c>DecideTx(t)</c>.
    /// </summary>
    public required string Row { get; init; }

    /// <summary>The repository-relative, forward-slash path of the one production file edited.</summary>
    public required string File { get; init; }

    /// <summary>The cited detectors to run, each <c>Fixture.Method</c>, in declared order.</summary>
    public required IReadOnlyList<string> Detectors { get; init; }

    /// <summary>One line saying what production behaviour the edit breaks.</summary>
    public required string Summary { get; init; }

    /// <summary>The anchored edits, applied in order.</summary>
    public required IReadOnlyList<SpecEdit> Edits { get; init; }

    /// <summary>
    /// Parses one perturbation from its text.
    /// </summary>
    /// <param name="name">The perturbation's name, used in error messages.</param>
    /// <param name="content">The full text of the <c>.perturbation</c> file.</param>
    /// <exception cref="InvalidOperationException">The text is malformed.</exception>
    public static RefinementPerturbation Parse(string name, string content)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(content);

        var label = name + Extension;
        var lines = content.ReplaceLineEndings("\n").Split('\n');
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        var edits = new List<SpecEdit>();

        var index = 0;
        for (; index < lines.Length; index++)
        {
            var line = lines[index];
            if (line.StartsWith(FindMarker, StringComparison.Ordinal))
            {
                break;
            }

            if (string.IsNullOrWhiteSpace(line) || line.StartsWith('#'))
            {
                continue;
            }

            if (IsBlockMarker(line))
            {
                throw new InvalidOperationException(
                    $"{label} line {index + 1} is the marker '{line}' before any block; expected '{FindMarker}' "
                    + "to open the first edit.");
            }

            var separator = line.IndexOf(':', StringComparison.Ordinal);
            if (separator <= 0)
            {
                throw new InvalidOperationException(
                    $"{label} line {index + 1} is neither a header ('KEY: value'), a comment, nor a block "
                    + $"marker: '{line}'");
            }

            var key = line[..separator].Trim();
            if (!RequiredHeaders.Contains(key, StringComparer.Ordinal))
            {
                throw new InvalidOperationException(
                    $"{label} line {index + 1} declares the unknown header '{key}'. The headers are "
                    + $"{string.Join(", ", RequiredHeaders)}.");
            }

            if (!headers.TryAdd(key, line[(separator + 1)..].Trim()))
            {
                throw new InvalidOperationException($"{label} line {index + 1} declares the header '{key}' twice.");
            }
        }

        while (index < lines.Length)
        {
            var line = lines[index];
            if (line.StartsWith(FindMarker, StringComparison.Ordinal))
            {
                var find = Collect(lines, ref index, FindMarker, ReplaceMarker, label);
                var replace = Collect(lines, ref index, ReplaceMarker, EndMarker, label);
                if (find.Length == 0)
                {
                    throw new InvalidOperationException($"{label} edit {edits.Count + 1} has an empty FIND block.");
                }

                edits.Add(new SpecEdit(find, replace));
                index++;
                continue;
            }

            if (string.IsNullOrWhiteSpace(line) || line.StartsWith('#'))
            {
                index++;
                continue;
            }

            throw new InvalidOperationException(IsBlockMarker(line)
                ? $"{label} line {index + 1} is the marker '{line}' outside any block; expected "
                    + $"'{FindMarker}' to open the next edit."
                : $"{label} line {index + 1} is text outside any block; expected '{FindMarker}', a "
                    + $"blank line or a '#' comment: '{line}'");
        }

        if (edits.Count == 0)
        {
            throw new InvalidOperationException($"{label} declares no edits.");
        }

        foreach (var key in RequiredHeaders)
        {
            if (!headers.TryGetValue(key, out var value) || value.Length == 0)
            {
                throw new InvalidOperationException($"{label} is missing the required header '{key}:'.");
            }
        }

        var file = headers["FILE"];
        if (file.Contains('\\', StringComparison.Ordinal)
            || !file.StartsWith("src/", StringComparison.Ordinal)
            || file.Split('/').Any(segment => segment is "" or "." or ".."))
        {
            throw new InvalidOperationException(
                $"{label} declares 'FILE: {file}'. FILE must be a repository-relative path under 'src/', "
                + "written with forward slashes and no '.' or '..' segments.");
        }

        var detectors = headers["DETECTORS"]
            .Split(',', StringSplitOptions.TrimEntries)
            .ToList();
        foreach (var detector in detectors)
        {
            if (!DetectorShape.IsMatch(detector))
            {
                throw new InvalidOperationException(
                    $"{label} declares the detector '{detector}'. Each DETECTORS entry must be 'Fixture.Method', "
                    + "separated by commas.");
            }
        }

        var repeated = detectors.GroupBy(d => d, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
        if (repeated is not null)
        {
            throw new InvalidOperationException($"{label} declares the detector '{repeated.Key}' twice.");
        }

        return new RefinementPerturbation
        {
            Name = name,
            Module = headers["MODULE"],
            Row = headers["ROW"],
            File = file,
            Detectors = detectors,
            Summary = headers["SUMMARY"],
            Edits = edits,
        };
    }

    /// <summary>
    /// Applies every edit, in order, to <paramref name="source"/>. Each anchor
    /// must match exactly once in the text as it stands after the previous
    /// edit; the text is compared with <c>\n</c> line endings, and the result
    /// keeps them.
    /// </summary>
    /// <param name="source">The current text of <see cref="File"/>.</param>
    /// <exception cref="InvalidOperationException">
    /// An anchor matched zero or several times, or the edits changed nothing.
    /// </exception>
    public string Apply(string source)
    {
        ArgumentNullException.ThrowIfNull(source);

        var normalised = source.ReplaceLineEndings("\n");
        var text = normalised;
        for (var i = 0; i < Edits.Count; i++)
        {
            var occurrences = CountOccurrences(text, Edits[i].Find);
            if (occurrences != 1)
            {
                throw new InvalidOperationException(
                    $"perturbation '{Name}' could not apply edit {i + 1} to {File}: expected exactly one match, "
                    + $"found {occurrences}. "
                    + (occurrences == 0
                        ? "The production file no longer contains this text, so the perturbation has drifted "
                          + "and must be re-derived against the current code."
                        : "The anchor is ambiguous; widen it with surrounding context until it is unique.")
                    + $"{Environment.NewLine}Anchor text was:{Environment.NewLine}{Edits[i].Find}");
            }

            text = text.Replace(Edits[i].Find, Edits[i].Replace, StringComparison.Ordinal);
        }

        if (text == normalised)
        {
            throw new InvalidOperationException(
                $"perturbation '{Name}' leaves {File} unchanged, so it cannot break anything.");
        }

        return text;
    }

    /// <summary>The name, which is what NUnit renders for this argument in a test-case label.</summary>
    public override string ToString() => Name;

    private static int CountOccurrences(string text, string needle)
    {
        var count = 0;
        var index = text.IndexOf(needle, StringComparison.Ordinal);
        while (index >= 0)
        {
            count++;
            index = text.IndexOf(needle, index + needle.Length, StringComparison.Ordinal);
        }

        return count;
    }

    private static string Collect(string[] lines, ref int index, string open, string close, string label)
    {
        index++;
        var body = new List<string>();
        while (index < lines.Length && !lines[index].StartsWith(close, StringComparison.Ordinal))
        {
            if (IsBlockMarker(lines[index]))
            {
                throw new InvalidOperationException(
                    $"{label} line {index + 1} is the marker '{lines[index]}' inside a '{open}' block; expected "
                    + $"'{close}' first.");
            }

            body.Add(lines[index]);
            index++;
        }

        if (index >= lines.Length)
        {
            throw new InvalidOperationException($"{label} has an unterminated block; expected '{close}'.");
        }

        return string.Join("\n", body);
    }

    private static bool IsBlockMarker(string line) =>
        line.StartsWith(FindMarker, StringComparison.Ordinal)
        || line.StartsWith(ReplaceMarker, StringComparison.Ordinal)
        || line.StartsWith(EndMarker, StringComparison.Ordinal);
}
