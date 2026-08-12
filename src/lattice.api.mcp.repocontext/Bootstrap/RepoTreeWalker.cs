using System.IO;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Walks a repository working tree and yields one <see cref="RepoFileEntry"/> per
/// included file, with a content digest computed for each. The walk is pure over
/// the filesystem: given the same tree and filters it produces the same ordered
/// result, so it composes with the idempotent diff in
/// <see cref="RepoContextBootstrapPlan"/>.
/// <para>
/// <b>Filtering.</b> The version-control metadata directory <c>.git</c> is always
/// skipped. When any include globs are supplied a file is kept only if it matches
/// at least one of them; when none are supplied every file is a candidate.
/// Exclude globs are applied last and always win. All matching is done on the
/// repository-relative, <c>'/'</c>-separated path (see <see cref="GlobMatcher"/>).
/// </para>
/// <para>
/// <b>Symlink safety.</b> The walk descends real directories only: any reparse
/// point (a symbolic link or junction, file or directory) is skipped rather than
/// followed. This keeps ingestion inside the workspace boundary the
/// <see cref="RepoContextWorkspaceGuard"/> established for the root and makes the
/// walk immune to cycles a self-referential link would otherwise create.
/// </para>
/// <para>
/// <b>Parallelism.</b> The walk runs in two phases: a serial, symlink-safe
/// directory traversal discovers the included files (a cheap, stat-only pass),
/// then the read-and-hash of those files - the dominant cost - is fanned out
/// across cores into a pre-sized result array (one write per slot, so no lock or
/// concurrent collection). The final ordinal sort makes the output identical
/// regardless of completion order, so determinism is preserved. Peak memory is
/// bounded to roughly the degree of parallelism times the largest file, not the
/// whole tree, because each file's bytes are released as soon as it is hashed.
/// </para>
/// </summary>
internal static class RepoTreeWalker
{
    private const string GitDirectorySegment = ".git";

    /// <summary>
    /// Walks <paramref name="rootPath"/> and returns the included files in
    /// ascending ordinal path order.
    /// </summary>
    /// <param name="rootPath">The absolute path to the repository root. Must not
    /// be <see langword="null"/> and must be an existing directory.</param>
    /// <param name="includeGlobs">Optional include patterns; when non-empty a file
    /// must match at least one to be kept.</param>
    /// <param name="excludeGlobs">Optional exclude patterns; a match removes the
    /// file even when it also matched an include.</param>
    /// <param name="cancellationToken">Cancels the walk between files.</param>
    /// <returns>The included files, ordered by repository-relative path.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="rootPath"/> is null.</exception>
    /// <exception cref="DirectoryNotFoundException"><paramref name="rootPath"/> does not exist.</exception>
    internal static IReadOnlyList<RepoFileEntry> Walk(
        string rootPath,
        IReadOnlyList<string>? includeGlobs,
        IReadOnlyList<string>? excludeGlobs,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rootPath);
        if (!Directory.Exists(rootPath))
        {
            throw new DirectoryNotFoundException(
                $"The repository root '{rootPath}' does not exist or is not a directory.");
        }

        var includes = Compile(includeGlobs);
        var excludes = Compile(excludeGlobs);

        var root = Path.GetFullPath(rootPath);

        // Phase 1: serial, symlink-safe discovery of the included files. This is a
        // cheap stat-only pass; the expensive read-and-hash is deferred to phase 2.
        var candidates = DiscoverIncludedFiles(root, includes, excludes, cancellationToken);
        if (candidates.Count == 0)
        {
            return Array.Empty<RepoFileEntry>();
        }

        // Phase 2: fan the read-and-hash out across cores into a pre-sized array.
        // Each index is written exactly once, so no synchronisation is needed; the
        // subsequent sort restores the deterministic ordinal order.
        var results = new RepoFileEntry[candidates.Count];
        var options = new ParallelOptions
        {
            CancellationToken = cancellationToken,
            MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount),
        };

        try
        {
            Parallel.For(0, candidates.Count, options, index =>
            {
                var (absolutePath, relativePath) = candidates[index];
                var content = File.ReadAllBytes(absolutePath);
                results[index] = new RepoFileEntry(
                    relativePath,
                    FileDigest.Compute(content),
                    content.LongLength,
                    LanguageClassifier.Classify(relativePath));
            });
        }
        catch (AggregateException aggregate) when (aggregate.InnerException is not null)
        {
            // Surface the underlying filesystem error (for example a file removed
            // mid-walk) with its original type and stack, not a wrapper.
            System.Runtime.ExceptionServices.ExceptionDispatchInfo
                .Capture(aggregate.InnerException).Throw();
        }

        Array.Sort(results, static (left, right) =>
            string.CompareOrdinal(left.RelativePath, right.RelativePath));
        return results;
    }

    /// <summary>
    /// Runs the serial, symlink-safe directory traversal and returns the absolute
    /// and repository-relative path of every file that passes the <c>.git</c>,
    /// include, and exclude filters, in discovery order.
    /// </summary>
    private static List<(string Absolute, string Relative)> DiscoverIncludedFiles(
        string root,
        List<GlobMatcher> includes,
        List<GlobMatcher> excludes,
        CancellationToken cancellationToken)
    {
        var included = new List<(string Absolute, string Relative)>();

        // Explicit depth-first walk over real directories only, so a symlinked or
        // junctioned directory is never descended (escape prevention + cycle
        // safety) and a symlinked file is never read.
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.Count != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var directory = pending.Pop();

            foreach (var child in new DirectoryInfo(directory).EnumerateFileSystemInfos())
            {
                cancellationToken.ThrowIfCancellationRequested();

                if ((child.Attributes & FileAttributes.ReparsePoint) != 0)
                {
                    // Symbolic link or junction: skip without following.
                    continue;
                }

                if ((child.Attributes & FileAttributes.Directory) != 0)
                {
                    var relativeDir = ToRelativePosixPath(root, child.FullName);
                    if (relativeDir.Equals(GitDirectorySegment, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    pending.Push(child.FullName);
                    continue;
                }

                var relativePath = ToRelativePosixPath(root, child.FullName);
                if (IsUnderGitDirectory(relativePath))
                {
                    continue;
                }

                if (includes.Count != 0 && !MatchesAny(includes, relativePath))
                {
                    continue;
                }

                if (MatchesAny(excludes, relativePath))
                {
                    continue;
                }

                included.Add((child.FullName, relativePath));
            }
        }

        return included;
    }

    private static List<GlobMatcher> Compile(IReadOnlyList<string>? globs)
    {
        if (globs is null || globs.Count == 0)
        {
            return [];
        }

        var matchers = new List<GlobMatcher>(globs.Count);
        foreach (var glob in globs)
        {
            if (!string.IsNullOrWhiteSpace(glob))
            {
                matchers.Add(GlobMatcher.Compile(glob));
            }
        }

        return matchers;
    }

    private static bool MatchesAny(List<GlobMatcher> matchers, string relativePath)
    {
        foreach (var matcher in matchers)
        {
            if (matcher.IsMatch(relativePath))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsUnderGitDirectory(string relativePath) =>
        relativePath.Equals(GitDirectorySegment, StringComparison.Ordinal)
        || relativePath.StartsWith(GitDirectorySegment + "/", StringComparison.Ordinal);

    private static string ToRelativePosixPath(string root, string absolutePath)
    {
        var relative = Path.GetRelativePath(root, absolutePath);
        return relative.Replace(Path.DirectorySeparatorChar, '/').Replace('\\', '/');
    }
}
