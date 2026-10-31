using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>
/// The feature catalogue, <c>explorer-features.tsv</c>: the definition of "every feature" (#5361).
/// One tab-separated row per feature - <c>id</c>, <c>kind</c>, <c>source</c>, <c>description</c>,
/// <c>journey</c> - under a header row. Its reflected half is kept complete against the Explorer
/// assemblies (<see cref="ExplorerSurface"/>); its curated half cites a doc anchor per row.
/// </summary>
public sealed class FeatureCatalogue
{
    /// <summary>The catalogue's file name.</summary>
    public const string FileName = "explorer-features.tsv";

    /// <summary>The exemptions' file name.</summary>
    public const string ExemptionsFileName = "coverage-exemptions.tsv";

    /// <summary>The catalogue's header row.</summary>
    public static IReadOnlyList<string> Header { get; } = ["id", "kind", "source", "description", "journey"];

    /// <summary>The exemptions' header row.</summary>
    public static IReadOnlyList<string> ExemptionsHeader { get; } = ["id", "reason"];

    private readonly Dictionary<string, FeatureRow> _byId;

    private FeatureCatalogue(IReadOnlyList<FeatureRow> rows)
    {
        Rows = rows;
        _byId = new Dictionary<string, FeatureRow>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            _byId.TryAdd(row.Id, row);
        }
    }

    /// <summary>The directory holding the catalogue and its exemptions.</summary>
    public static string Directory => Path.Combine(JourneyPaths.ProjectDirectory, "FeatureCoverage");

    /// <summary>The catalogue's path in the repository.</summary>
    public static string DefaultPath => Path.Combine(Directory, FileName);

    /// <summary>The exemptions' path in the repository.</summary>
    public static string ExemptionsPath => Path.Combine(Directory, ExemptionsFileName);

    /// <summary>Every row, in file order.</summary>
    public IReadOnlyList<FeatureRow> Rows { get; }

    /// <summary>Whether a row has <paramref name="id"/>.</summary>
    /// <param name="id">The feature id.</param>
    public bool Contains(string id) => _byId.ContainsKey(id);

    /// <summary>The row with <paramref name="id"/>, or <see langword="null"/>.</summary>
    /// <param name="id">The feature id.</param>
    public FeatureRow? Find(string id) => _byId.GetValueOrDefault(id);

    /// <summary>Loads the catalogue from <paramref name="path"/>, by default the repository's.</summary>
    /// <param name="path">The file.</param>
    public static FeatureCatalogue Load(string? path = null)
    {
        path ??= DefaultPath;
        return Parse(File.ReadAllText(path), path);
    }

    /// <summary>Parses catalogue text. Structural faults (the header, a row's column count) fail with the line.</summary>
    /// <param name="text">The TSV text.</param>
    /// <param name="origin">The file, for messages.</param>
    /// <exception cref="FormatException">The text is not a catalogue.</exception>
    public static FeatureCatalogue Parse(string text, string origin)
    {
        ArgumentNullException.ThrowIfNull(text);
        var rows = new List<FeatureRow>();
        foreach (var (cells, line) in Records(text, origin, Header))
        {
            rows.Add(new FeatureRow(cells[0], cells[1], cells[2], cells[3], cells[4], line));
        }

        return new FeatureCatalogue(rows);
    }

    /// <summary>Loads the exemptions from <paramref name="path"/>, by default the repository's.</summary>
    /// <param name="path">The file.</param>
    public static IReadOnlyList<CoverageExemption> LoadExemptions(string? path = null)
    {
        path ??= ExemptionsPath;
        return ParseExemptions(File.ReadAllText(path), path);
    }

    /// <summary>Parses exemption text.</summary>
    /// <param name="text">The TSV text.</param>
    /// <param name="origin">The file, for messages.</param>
    /// <exception cref="FormatException">The text is not an exemption list.</exception>
    public static IReadOnlyList<CoverageExemption> ParseExemptions(string text, string origin)
    {
        ArgumentNullException.ThrowIfNull(text);
        return [.. Records(text, origin, ExemptionsHeader).Select(record => new CoverageExemption(record.Cells[0], record.Cells[1], record.Line))];
    }

    private static IEnumerable<(string[] Cells, int Line)> Records(string text, string origin, IReadOnlyList<string> header)
    {
        var lines = text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        if (lines.Length == 0 || !lines[0].Split('\t').SequenceEqual(header))
        {
            throw new FormatException($"{origin}:1: the header must be '{string.Join("<TAB>", header)}'.");
        }

        for (var index = 1; index < lines.Length; index++)
        {
            if (lines[index].Length == 0)
            {
                continue;
            }

            var cells = lines[index].Split('\t');
            if (cells.Length != header.Count || cells.Any(cell => cell.Trim().Length == 0 || cell != cell.Trim()))
            {
                throw new FormatException(
                    $"{origin}:{index + 1}: a row has exactly {header.Count} tab-separated cells, none empty or padded with spaces; this one has {cells.Length}.");
            }

            yield return (cells, index + 1);
        }
    }
}
