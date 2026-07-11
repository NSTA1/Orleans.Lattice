using System.IO;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Shared filesystem helpers for the repository-wide hygiene gates. The
/// helpers walk up from <see cref="AppContext.BaseDirectory"/> to find the
/// repository root and enumerate tracked files, so a single copy of each
/// content scanner (em-dash, mojibake, deletion-mandate,
/// performance-report markers) can run from any consuming test assembly.
/// </summary>
public static class HygieneRepository
{
    // Directory segments under the repo root that are never in scope:
    // build output, IDE/VCS metadata, gitignored scratch and run output,
    // benchmark artifacts, third-party module trees, and test result dumps.
    private static readonly string[] ExcludedSegments =
    {
        "bin", "obj", "node_modules",
        ".git", ".vs",
        ".run", ".scratch",
        "BenchmarkDotNet.Artifacts",
        "TestResults",
    };

    /// <summary>
    /// Locates the repository root by walking up from the test assembly's
    /// base directory until a directory containing <c>README.md</c>,
    /// <c>docs/</c>, and <c>src/</c> is found.
    /// </summary>
    public static string FindRepoRoot()
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

    /// <summary>
    /// Returns true when any path segment is an excluded build/metadata
    /// directory (case-insensitive).
    /// </summary>
    public static bool HasExcludedSegment(string path)
    {
        var parts = path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        foreach (var part in parts)
        {
            foreach (var excluded in ExcludedSegments)
            {
                if (part.Equals(excluded, StringComparison.OrdinalIgnoreCase)) return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Enumerates files under <paramref name="root"/> matching
    /// <paramref name="pattern"/>, skipping build/metadata directories. The
    /// enumeration is empty when the directory does not exist.
    /// </summary>
    public static IEnumerable<string> EnumerateFiles(string root, string pattern)
    {
        if (!Directory.Exists(root)) yield break;
        foreach (var file in Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories))
        {
            if (HasExcludedSegment(file)) continue;
            yield return file;
        }
    }

    /// <summary>
    /// Enumerates files matching <paramref name="pattern"/> under each of the
    /// scope's slice roots (resolved relative to <paramref name="repoRoot"/>),
    /// skipping build/metadata directories.
    /// </summary>
    public static IEnumerable<string> EnumerateSliceFiles(
        string repoRoot, HygieneScanScope scope, string pattern)
    {
        foreach (var relative in scope.SliceRelativeRoots)
        {
            var root = Path.Combine(repoRoot, relative.Replace('/', Path.DirectorySeparatorChar));
            foreach (var file in EnumerateFiles(root, pattern))
            {
                yield return file;
            }
        }
    }

    /// <summary>
    /// Enumerates files matching <paramref name="pattern"/> anywhere under
    /// <paramref name="repoRoot"/> that are NOT inside one of the package
    /// slice directories in <paramref name="otherSliceRoots"/> (those are
    /// owned by their respective per-package slices), skipping build/metadata
    /// directories. This is the repo-level remainder owned exclusively by the
    /// core fixture; it includes orphan directories under <c>test/</c> (such
    /// as shared test infrastructure) that belong to no package.
    /// </summary>
    public static IEnumerable<string> EnumerateRepoLevelFiles(
        string repoRoot, string pattern, IReadOnlyList<string> otherSliceRoots)
    {
        var excludedPrefixes = otherSliceRoots
            .Select(r => Path.Combine(repoRoot, r.Replace('/', Path.DirectorySeparatorChar)) + Path.DirectorySeparatorChar)
            .ToArray();

        foreach (var file in Directory.EnumerateFiles(repoRoot, pattern, SearchOption.AllDirectories))
        {
            if (HasExcludedSegment(file)) continue;
            if (excludedPrefixes.Any(prefix => file.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))) continue;
            yield return file;
        }
    }
}
