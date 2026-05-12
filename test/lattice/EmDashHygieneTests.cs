using System.IO;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Regression: em-dash characters (U+2014) must not appear in any tracked
/// text file in the repository - source, tests, docs, build scripts, samples,
/// or configuration alike. The repository convention is to use plain ASCII
/// hyphens (<c>-</c>) so diffs, search, and copy/paste behave predictably
/// across editors and terminals. Authors routinely paste prose from word
/// processors that auto-convert <c>--</c> to an em-dash; this gate catches
/// the leak at PR time.
/// </summary>
[TestFixture]
public class EmDashHygieneTests
{
    // Construct the em-dash via its code point so this source file itself
    // does not contain a literal em-dash and therefore is not self-flagged.
    private const char EmDash = '\u2014';

    // Directory segments under the repo root that are never in scope:
    // build output, IDE/VCS metadata, gitignored scratch and run output,
    // benchmark artifacts, third-party module trees, and test result dumps.
    private static readonly string[] ExcludedSegments = new[]
    {
        "bin", "obj", "node_modules",
        ".git", ".vs",
        ".run", ".scratch",
        "BenchmarkDotNet.Artifacts",
        "TestResults",
    };

    // File extensions that hold binary payloads where a U+2014 byte sequence
    // is meaningless and would only produce noise. Everything else is treated
    // as text and scanned.
    private static readonly HashSet<string> BinaryExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".png", ".jpg", ".jpeg", ".gif", ".ico", ".bmp", ".pdf",
        ".dll", ".exe", ".pdb", ".so", ".dylib",
        ".zip", ".tar", ".gz", ".7z", ".nupkg", ".snk",
        ".dmp", ".bin",
    };

    /// <summary>
    /// Scans every tracked text file under the repository and fails if any
    /// em-dash is present, listing every offending file and line so the fix
    /// is mechanical.
    /// </summary>
    [Test]
    public void No_em_dashes_in_tracked_files()
    {
        var repoRoot = FindRepoRoot();

        var violations = new List<string>();
        foreach (var file in EnumerateTextFiles(repoRoot))
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

    private static IEnumerable<string> EnumerateTextFiles(string root)
    {
        foreach (var file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
        {
            var parts = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (parts.Any(IsExcludedSegment)) continue;
            if (BinaryExtensions.Contains(Path.GetExtension(file))) continue;
            yield return file;
        }
    }

    private static bool IsExcludedSegment(string segment)
    {
        foreach (var excluded in ExcludedSegments)
        {
            if (segment.Equals(excluded, StringComparison.OrdinalIgnoreCase)) return true;
        }
        return false;
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
