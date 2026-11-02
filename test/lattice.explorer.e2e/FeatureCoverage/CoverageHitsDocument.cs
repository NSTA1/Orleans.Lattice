using System.Text.Json;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;

/// <summary>A run's <c>coverage-hits.json</c>: every feature it proved, and by which scopes.</summary>
/// <param name="Mode">The run mode (<c>isolated</c> or <c>story</c>).</param>
/// <param name="Run">The CI run id, when run in CI.</param>
/// <param name="Commit">The commit, when run in CI.</param>
/// <param name="WrittenAt">When the file was written.</param>
/// <param name="Hits">The features and their scopes.</param>
public sealed record CoverageHitsDocument(string Mode, string? Run, string? Commit, DateTimeOffset WrittenAt, IReadOnlyList<CoverageHitEntry> Hits)
{
    /// <summary>The file name every run writes.</summary>
    public const string FileName = "coverage-hits.json";

    /// <summary>The environment variable naming a directory of hit files for the gate to union, recursively.</summary>
    public const string DirectoryVariable = "LATTICE_E2E_COVERAGE_HITS";

    /// <summary>The serializer options of the file.</summary>
    public static JsonSerializerOptions JsonOptions { get; } = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    /// <summary>
    /// The feature ids proved by every <c>coverage-hits*.json</c> under <paramref name="directory"/>,
    /// recursively: the union of the story runs' and the isolated runs' evidence.
    /// </summary>
    /// <param name="directory">The directory; absent means no evidence.</param>
    public static IReadOnlySet<string> UnionUnder(string directory)
    {
        var union = new HashSet<string>(StringComparer.Ordinal);
        if (!System.IO.Directory.Exists(directory))
        {
            return union;
        }

        foreach (var file in System.IO.Directory.EnumerateFiles(directory, "coverage-hits*.json", SearchOption.AllDirectories))
        {
            var document = JsonSerializer.Deserialize<CoverageHitsDocument>(File.ReadAllText(file), JsonOptions)
                ?? throw new InvalidDataException($"{file} is not a coverage hits document.");
            union.UnionWith(document.Hits.Select(hit => hit.Feature));
        }

        return union;
    }
}
