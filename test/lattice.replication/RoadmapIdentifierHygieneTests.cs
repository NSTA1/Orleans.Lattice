using System.IO;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Regression: replication feature-tracker identifiers (<c>R-XXX</c>) must
/// appear only in the replication <c>roadmap.md</c>. They are meaningless
/// outside of that file - docs, XML doc comments, inline comments, test
/// fixture names, and string literals must describe the behaviour by name
/// and effect instead. See the "Documentation" section of
/// <c>.github/copilot-instructions.md</c>.
/// </summary>
[TestFixture]
public class RoadmapIdentifierHygieneTests
{
    // The pattern is assembled from fragments so this source file itself does
    // not contain a literal tracker-id and therefore is not self-flagged.
    private const string H = "-";
    private static readonly Regex TrackerIdPattern = new(
        @$"\bR{H}\d{{3}}[a-z]?\b",
        RegexOptions.Compiled);

    /// <summary>
    /// Scans every <c>.cs</c> file under <c>src/lattice.replication/</c> and
    /// every <c>.md</c> file under <c>docs/lattice.replication/</c> and fails
    /// if any tracker identifier is present. The replication
    /// <c>roadmap.md</c> and this test file are the only permitted locations.
    /// </summary>
    [Test]
    public void Replication_tracker_identifiers_appear_only_in_roadmap()
    {
        var repoRoot = FindRepoRoot();
        var thisFile = Path.GetFullPath(
            Path.Combine(repoRoot, "test", "lattice.replication", "RoadmapIdentifierHygieneTests.cs"));

        var scanned = new List<string>();
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, "src", "lattice.replication"), "*.cs"));
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, "docs", "lattice.replication"), "*.md"));

        var violations = new List<string>();
        foreach (var file in scanned)
        {
            var full = Path.GetFullPath(file);
            if (string.Equals(full, thisFile, StringComparison.OrdinalIgnoreCase)) continue;
            if (string.Equals(Path.GetFileName(full), "roadmap.md", StringComparison.OrdinalIgnoreCase)) continue;

            var lines = File.ReadAllLines(full);
            for (int i = 0; i < lines.Length; i++)
            {
                var match = TrackerIdPattern.Match(lines[i]);
                if (match.Success)
                {
                    var rel = Path.GetRelativePath(repoRoot, full).Replace('\\', '/');
                    violations.Add($"{rel}:{i + 1}: '{match.Value}' in: {lines[i].Trim()}");
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "Replication tracker identifiers must appear only in the replication roadmap.md. "
            + "Rewrite these references to describe the behaviour by name and effect "
            + "(see .github/copilot-instructions.md -> Documentation)." + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }

    private static IEnumerable<string> EnumerateFiles(string root, string pattern)
    {
        if (!Directory.Exists(root)) yield break;
        foreach (var file in Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories))
        {
            var parts = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (parts.Any(p => p.Equals("bin", StringComparison.OrdinalIgnoreCase)
                            || p.Equals("obj", StringComparison.OrdinalIgnoreCase)
                            || p.Equals("node_modules", StringComparison.OrdinalIgnoreCase)))
                continue;
            yield return file;
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "README.md"))
                && Directory.Exists(Path.Combine(dir.FullName, "docs"))
                && Directory.Exists(Path.Combine(dir.FullName, "src")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        throw new InvalidOperationException(
            "Could not find repository root from " + AppContext.BaseDirectory);
    }
}
