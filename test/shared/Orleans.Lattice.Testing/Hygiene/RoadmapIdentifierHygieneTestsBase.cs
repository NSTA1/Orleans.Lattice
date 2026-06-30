using System.IO;
using System.Text.RegularExpressions;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Regression: feature-tracker identifiers (<c>F-XXX</c>, <c>FX-XXX</c>,
/// <c>G-XXX</c>, the compact <c>FxNNN</c> / <c>fxNNN</c> forms, and the
/// replication <c>R-XXX</c> form) must not appear anywhere except
/// <c>CHANGELOG.md</c> and the <c>features.md</c> issue indexes. They are
/// meaningless outside of those locations - docs, XML doc comments, inline
/// comments, test fixture names, and tree-id string literals must describe
/// the behaviour by name and effect, or link directly to the GitHub issue,
/// instead. Feature planning lives on GitHub Issues; see the "Documentation"
/// section of <c>.github/copilot-instructions.md</c>.
/// <para>
/// A concrete subclass supplies a <see cref="HygieneScanScope"/>: the
/// <c>.cs</c> files under its slice are always scanned, and the core project
/// additionally scans the repo-level <c>.md</c> files under <c>docs/</c> and
/// <c>.github/</c>.
/// </para>
/// </summary>
public abstract class RoadmapIdentifierHygieneTestsBase
{
    // The pattern is assembled from fragments so this source file itself does
    // not contain a literal tracker-id and therefore is not self-flagged. It
    // covers the core identifier families (F / FX / G / Fx / fx) plus the
    // replication family (R).
    private const string H = "-";
    private static readonly Regex TrackerIdPattern = new(
        @$"F{H}\d{{3}}[a-z]?|FX{H}\d{{3}}|G{H}\d{{3}}|\bFx\d{{3}}\b|\bfx\d{{3}}\b|\bR{H}\d{{3}}[a-z]?\b",
        RegexOptions.Compiled);

    /// <summary>The repository slice this fixture is responsible for scanning.</summary>
    protected abstract HygieneScanScope Scope { get; }

    /// <summary>
    /// Scans the <c>.cs</c> files in this fixture's slice (and, for the core
    /// fixture, the <c>.md</c> files under <c>docs/</c> and <c>.github/</c>)
    /// and fails if any tracker identifier is present. <c>CHANGELOG.md</c>,
    /// the <c>features.md</c> issue indexes, and this base file are the only
    /// permitted locations.
    /// </summary>
    [Test]
    public void Tracker_identifiers_appear_only_in_changelog_and_feature_index()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var scanned = new List<string>();
        scanned.AddRange(HygieneRepository.EnumerateSliceFiles(repoRoot, Scope, "*.cs"));
        if (Scope.OwnsRepoLevelFiles)
        {
            scanned.AddRange(HygieneRepository.EnumerateFiles(Path.Combine(repoRoot, "docs"), "*.md"));
            scanned.AddRange(HygieneRepository.EnumerateFiles(Path.Combine(repoRoot, ".github"), "*.md"));
        }

        var violations = new List<string>();
        foreach (var file in scanned)
        {
            var full = Path.GetFullPath(file);
            var fileName = Path.GetFileName(full);
            // This base file assembles the patterns from fragments, but exclude
            // it by name as a defensive measure against future literal examples.
            if (fileName.Equals("RoadmapIdentifierHygieneTestsBase.cs", StringComparison.OrdinalIgnoreCase)) continue;
            // CHANGELOG.md, the issue trackers, and the per-package
            // `features.md` issue indexes are the only legitimate homes for
            // tracker identifiers. In the feature indexes the id appears only
            // as the link text on its GitHub issue link; the whole file is
            // exempted rather than asserting link structure line-by-line.
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
}
