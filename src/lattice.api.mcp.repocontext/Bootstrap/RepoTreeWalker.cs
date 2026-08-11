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
        var entries = new List<RepoFileEntry>();

        foreach (var absolutePath in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var relativePath = ToRelativePosixPath(root, absolutePath);
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

            var content = File.ReadAllBytes(absolutePath);
            entries.Add(new RepoFileEntry(
                relativePath,
                FileDigest.Compute(content),
                content.LongLength,
                LanguageClassifier.Classify(relativePath)));
        }

        entries.Sort(static (left, right) =>
            string.CompareOrdinal(left.RelativePath, right.RelativePath));
        return entries;
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
