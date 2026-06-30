using System.IO;
using NUnit.Framework;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Regression: em-dash characters (U+2014) must not appear in any tracked
/// text file - source, tests, docs, build scripts, samples, or configuration
/// alike. The repository convention is to use plain ASCII hyphens (<c>-</c>)
/// so diffs, search, and copy/paste behave predictably across editors and
/// terminals. Authors routinely paste prose from word processors that
/// auto-convert <c>--</c> to an em-dash; this gate catches the leak at PR
/// time.
/// <para>
/// A concrete subclass supplies a <see cref="HygieneScanScope"/> so the scan
/// covers only that project's slice (plus, for the core project, the
/// repo-level files no package owns). The union of every project's scope
/// still covers the entire repository.
/// </para>
/// </summary>
public abstract class EmDashHygieneTestsBase
{
    // Construct the em-dash via its code point so this source file itself
    // does not contain a literal em-dash and therefore is not self-flagged.
    private const char EmDash = '\u2014';

    /// <summary>The repository slice this fixture is responsible for scanning.</summary>
    protected abstract HygieneScanScope Scope { get; }

    /// <summary>
    /// Scans every tracked text file in this fixture's scope and fails if any
    /// em-dash is present, listing every offending file and line so the fix
    /// is mechanical.
    /// </summary>
    [Test]
    public void No_em_dashes_in_tracked_files()
    {
        var repoRoot = HygieneRepository.FindRepoRoot();

        var violations = new List<string>();
        foreach (var file in HygieneFiles.EnumerateTextFiles(repoRoot, Scope))
        {
            var lines = File.ReadAllLines(file);
            for (int i = 0; i < lines.Length; i++)
            {
                var idx = lines[i].IndexOf(EmDash);
                if (idx >= 0)
                {
                    var rel = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');
                    violations.Add($"{rel}:{i + 1}:{idx + 1}: {lines[i].Trim()}");
                }
            }
        }

        Assert.That(violations, Is.Empty,
            "Em-dash characters (U+2014) are not permitted in tracked files. "
            + "Replace each occurrence with a plain ASCII hyphen ('-')."
            + Environment.NewLine
            + string.Join(Environment.NewLine, violations));
    }
}
