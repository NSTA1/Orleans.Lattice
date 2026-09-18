using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Enumeration;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Shared repository helpers for the repository-wide hygiene gates. The
/// helpers walk up from <see cref="AppContext.BaseDirectory"/> to find the
/// repository root and enumerate tracked files, so a single copy of each
/// content scanner (em-dash, mojibake, deletion-mandate,
/// performance-report markers) can run from any consuming test assembly.
/// </summary>
/// <remarks>
/// <para>
/// "Tracked" is sourced from git (<c>git ls-files</c>) rather than inferred
/// from the filesystem, which is what the gate names have always claimed and
/// what the implementation did not do until issue #3134. A raw directory walk
/// filtered by a hardcoded directory deny-list put every untracked and
/// gitignored file in the worktree in scope, so a gate named
/// <c>No_em_dashes_in_tracked_files</c> could fail on a file git has been
/// explicitly told to ignore - in practice the running container sample's
/// Azurite store under <c>samples/RepoContextContainer/backup-sink/</c>, which
/// is both gitignored and locked.
/// </para>
/// <para>
/// A <c>.gitignore</c> reimplementation is deliberately not attempted: it
/// would be a second, drifting source of truth for a question git already
/// answers exactly. One process launch per repository root is amortised across
/// every gate in the assembly, and it replaces a recursive directory walk per
/// call, so the enumeration is cheaper than what it supersedes rather than
/// more expensive.
/// </para>
/// </remarks>
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

    private static readonly ConcurrentDictionary<string, string[]> TrackedFilesByRoot =
        new(StringComparer.OrdinalIgnoreCase);

    private static readonly Lazy<string> CachedRepoRoot = new(LocateRepoRoot);

    /// <summary>
    /// Locates the repository root by walking up from the test assembly's
    /// base directory until a directory containing <c>README.md</c>,
    /// <c>docs/</c>, and <c>src/</c> is found.
    /// </summary>
    public static string FindRepoRoot() => CachedRepoRoot.Value;

    /// <summary>
    /// Every file git currently tracks under <paramref name="repoRoot"/>, as
    /// absolute paths sorted with <see cref="StringComparer.OrdinalIgnoreCase"/>
    /// so a directory prefix occupies one contiguous run and can be located by
    /// binary search.
    /// </summary>
    /// <param name="repoRoot">The repository root to enumerate.</param>
    /// <returns>The sorted absolute paths of every tracked file.</returns>
    /// <exception cref="InvalidOperationException">
    /// git could not be run, reported a failure, or reported no tracked files
    /// at all. All three are raised loudly rather than degraded into an empty
    /// enumeration, because a hygiene gate that silently examines nothing
    /// reports a clean repository it never read - the exact failure the
    /// anti-vacuity control of issue #2275 exists to prevent.
    /// </exception>
    public static IReadOnlyList<string> TrackedFiles(string repoRoot)
    {
        ArgumentNullException.ThrowIfNull(repoRoot);
        return TrackedFilesByRoot.GetOrAdd(repoRoot, static root => LoadTrackedFiles(root));
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
    /// Enumerates the tracked files under <paramref name="root"/> matching
    /// <paramref name="pattern"/>, skipping build/metadata directories. The
    /// enumeration is empty when nothing tracked lives under the root.
    /// </summary>
    /// <remarks>
    /// A root outside the repository has no tracked set to draw on, so it
    /// falls back to a filesystem walk. That is not a loophole in the tracked
    /// contract: a path outside the repository is not tracked by definition,
    /// and the callers that pass one are scanning scratch trees they created
    /// themselves.
    /// </remarks>
    public static IEnumerable<string> EnumerateFiles(string root, string pattern)
    {
        var repoRoot = FindRepoRoot();
        var full = Path.GetFullPath(root);

        if (!IsUnder(full, repoRoot))
        {
            return EnumerateFilesFromDisk(full, pattern);
        }

        return EnumerateTracked(repoRoot, DirectoryPrefix(full), pattern, excludedPrefixes: null);
    }

    /// <summary>
    /// Enumerates the tracked files matching <paramref name="pattern"/> under
    /// each of the scope's slice roots (resolved relative to
    /// <paramref name="repoRoot"/>), skipping build/metadata directories.
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
    /// Enumerates the tracked files matching <paramref name="pattern"/>
    /// anywhere under <paramref name="repoRoot"/> that are NOT inside one of
    /// the package slice directories in <paramref name="otherSliceRoots"/>
    /// (those are owned by their respective per-package slices), skipping
    /// build/metadata directories. This is the repo-level remainder owned
    /// exclusively by the core fixture; it includes orphan directories under
    /// <c>test/</c> (such as shared test infrastructure) that belong to no
    /// package.
    /// </summary>
    public static IEnumerable<string> EnumerateRepoLevelFiles(
        string repoRoot, string pattern, IReadOnlyList<string> otherSliceRoots)
    {
        var excludedPrefixes = otherSliceRoots
            .Select(r => DirectoryPrefix(Path.Combine(repoRoot, r.Replace('/', Path.DirectorySeparatorChar))))
            .ToArray();

        return EnumerateTracked(repoRoot, DirectoryPrefix(repoRoot), pattern, excludedPrefixes);
    }

    private static IEnumerable<string> EnumerateTracked(
        string repoRoot, string prefix, string pattern, string[]? excludedPrefixes)
    {
        var tracked = (string[])TrackedFiles(repoRoot);
        var start = LowerBound(tracked, prefix);

        for (var i = start; i < tracked.Length; i++)
        {
            var file = tracked[i];
            if (!file.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                yield break;
            }

            if (HasExcludedSegment(file)) continue;

            if (excludedPrefixes is not null
                && Array.Exists(excludedPrefixes, p => file.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            if (!FileSystemName.MatchesSimpleExpression(pattern, Path.GetFileName(file.AsSpan()), ignoreCase: true))
            {
                continue;
            }

            yield return file;
        }
    }

    private static IEnumerable<string> EnumerateFilesFromDisk(string root, string pattern)
    {
        if (!Directory.Exists(root)) yield break;
        foreach (var file in Directory.EnumerateFiles(root, pattern, SearchOption.AllDirectories))
        {
            if (HasExcludedSegment(file)) continue;
            yield return file;
        }
    }

    /// <summary>
    /// The index of the first entry that could start with <paramref name="prefix"/>.
    /// The array is sorted with the same comparer used here, so every entry
    /// carrying the prefix forms one contiguous run beginning at this index.
    /// </summary>
    private static int LowerBound(string[] sorted, string prefix)
    {
        var found = Array.BinarySearch(sorted, prefix, StringComparer.OrdinalIgnoreCase);
        return found >= 0 ? found : ~found;
    }

    private static string DirectoryPrefix(string directory)
    {
        var trimmed = directory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        return trimmed + Path.DirectorySeparatorChar;
    }

    private static bool IsUnder(string candidate, string root) =>
        candidate.Equals(root, StringComparison.OrdinalIgnoreCase)
        || candidate.StartsWith(DirectoryPrefix(root), StringComparison.OrdinalIgnoreCase);

    private static string[] LoadTrackedFiles(string repoRoot)
    {
        var startInfo = new ProcessStartInfo("git")
        {
            WorkingDirectory = repoRoot,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add("ls-files");
        startInfo.ArgumentList.Add("--cached");
        startInfo.ArgumentList.Add("-z");

        string standardOutput;
        string standardError;
        int exitCode;
        try
        {
            using var process = Process.Start(startInfo)
                ?? throw new InvalidOperationException("Starting 'git' returned no process.");
            standardOutput = process.StandardOutput.ReadToEnd();
            standardError = process.StandardError.ReadToEnd();
            process.WaitForExit();
            exitCode = process.ExitCode;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "The hygiene gates enumerate tracked files with 'git ls-files', which could not be run in '"
                + repoRoot + "'. git must be on PATH and the directory must be a git working tree.", ex);
        }

        if (exitCode != 0)
        {
            throw new InvalidOperationException(
                "'git ls-files' failed with exit code " + exitCode + " in '" + repoRoot + "': " + standardError.Trim());
        }

        var relativePaths = standardOutput.Split('\0', StringSplitOptions.RemoveEmptyEntries);
        if (relativePaths.Length == 0)
        {
            throw new InvalidOperationException(
                "'git ls-files' reported no tracked files in '" + repoRoot
                + "'. The hygiene gates would examine nothing and report a clean repository they never read.");
        }

        var absolute = new string[relativePaths.Length];
        for (var i = 0; i < relativePaths.Length; i++)
        {
            absolute[i] = Path.Combine(repoRoot, relativePaths[i].Replace('/', Path.DirectorySeparatorChar));
        }

        Array.Sort(absolute, StringComparer.OrdinalIgnoreCase);
        return absolute;
    }

    private static string LocateRepoRoot()
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
