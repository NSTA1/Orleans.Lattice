using System.IO;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression: feature-tracker identifiers (<c>F-XXX</c>, <c>FX-XXX</c>,
/// <c>G-XXX</c>, and the compact <c>FxNNN</c> / <c>fxNNN</c> identifier forms)
/// must not appear anywhere except <c>CHANGELOG.md</c> and the
/// <c>features.md</c> issue indexes. They are meaningless outside of those
/// locations - docs, XML doc comments, inline comments, test fixture names,
/// and tree-id string literals must describe the behaviour by name and effect,
/// or link directly to the GitHub issue, instead. Feature planning lives on
/// GitHub Issues; see the "Documentation" section of
/// <c>.github/copilot-instructions.md</c>.
/// </summary>
[TestFixture]
public class RoadmapIdentifierHygieneTests
{
    // The pattern is assembled from fragments so this source file itself does
    // not contain a literal tracker-id and therefore is not self-flagged.
    private const string H = "-";
    private static readonly Regex TrackerIdPattern = new(
        @$"F{H}\d{{3}}[a-z]?|FX{H}\d{{3}}|G{H}\d{{3}}|\bFx\d{{3}}\b|\bfx\d{{3}}\b",
        RegexOptions.Compiled);

    /// <summary>
    /// Scans every <c>.cs</c> file under <c>src/</c> and <c>test/</c> plus
    /// every <c>.md</c> file under <c>docs/</c> and <c>.github/</c> and
    /// fails if any tracker identifier is present. <c>CHANGELOG.md</c>, the
    /// <c>features.md</c> issue indexes, and this test file are the only
    /// permitted locations.
    /// </summary>
    [Test]
    public void Tracker_identifiers_appear_only_in_changelog_and_feature_index()
    {
        var repoRoot = FindRepoRoot();
        var thisFile = Path.GetFullPath(
            Path.Combine(repoRoot, "test", "lattice", "RoadmapIdentifierHygieneTests.cs"));

        var scanned = new List<string>();
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, "src"), "*.cs"));
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, "test"), "*.cs"));
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, "docs"), "*.md"));
        scanned.AddRange(EnumerateFiles(Path.Combine(repoRoot, ".github"), "*.md"));

        var violations = new List<string>();
        foreach (var file in scanned)
        {
            var full = Path.GetFullPath(file);
            if (string.Equals(full, thisFile, StringComparison.OrdinalIgnoreCase)) continue;
            // CHANGELOG.md, the issue trackers, and the per-package
            // `features.md` issue indexes are the only legitimate homes for
            // tracker identifiers. In the feature indexes the id appears only
            // as the link text on its GitHub issue link; the whole file is
            // exempted rather than asserting link structure line-by-line.
            var fileName = Path.GetFileName(full);
            if (fileName.Equals("CHANGELOG.md", StringComparison.OrdinalIgnoreCase)) continue;
            if (fileName.Equals("features.md", StringComparison.OrdinalIgnoreCase)) continue;

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
            "Feature-tracker identifiers must appear only in CHANGELOG.md and the "
            + "features.md issue indexes. Rewrite these references to describe the "
            + "behaviour by name and effect, or link directly to the GitHub issue "
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
