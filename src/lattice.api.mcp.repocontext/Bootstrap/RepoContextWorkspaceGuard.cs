using System.IO;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The fail-closed workspace boundary for repository ingestion. Given the set of
/// allowed workspace roots the host mounts, it resolves a caller-supplied path to
/// its real on-disk location - defeating both lexical <c>..</c> traversal and
/// symlink escape - and asserts the result sits inside an allowed root, throwing
/// <see cref="RepoContextWorkspaceViolationException"/> otherwise.
/// <para>
/// <b>Where it runs.</b> The guard is enforced at the single narrowest seam every
/// ingestion path funnels through - <see cref="RepoContextBootstrapService"/> -
/// so the <c>repocontext_bootstrap</c> and <c>repocontext_add_repo</c> tools can
/// never diverge on what they will read. When the host configures no roots the
/// guard is inert (it only normalises the path), preserving the behaviour of
/// hosts that intentionally ingest arbitrary local paths; the container host
/// always configures the mounted workspace root, so the product is fail-closed.
/// </para>
/// <para>
/// <b>Allocation.</b> Every allowed root is canonicalised once in the constructor;
/// the inert path returns after a single <see cref="Path.GetFullPath(string)"/>.
/// The enforcing path canonicalises the request and performs an ordinal prefix
/// comparison - no per-call caches, no regex, no reflection.
/// </para>
/// </summary>
internal sealed class RepoContextWorkspaceGuard
{
    // A defensive cap on symlink indirection so a pathological chain or cycle
    // fails closed instead of looping.
    private const int MaxSymlinkHops = 40;

    private static readonly char[] Separators =
        [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar];

    // Windows paths are case-insensitive; POSIX (the container) is case-sensitive.
    private static readonly StringComparison PathComparison =
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    private readonly string[] _canonicalRoots;

    /// <summary>
    /// Creates a guard over <paramref name="allowedRoots"/>. Each root is
    /// canonicalised (its <c>..</c> segments and symlinks resolved) once, so a
    /// request is later matched against the real mounted location. A null, empty,
    /// or whitespace root entry is ignored; when no usable root remains the guard
    /// is inert.
    /// </summary>
    /// <param name="allowedRoots">The workspace roots ingestion may read under.</param>
    /// <exception cref="ArgumentNullException"><paramref name="allowedRoots"/> is null.</exception>
    public RepoContextWorkspaceGuard(IEnumerable<string> allowedRoots)
    {
        ArgumentNullException.ThrowIfNull(allowedRoots);

        var roots = new List<string>();
        foreach (var root in allowedRoots)
        {
            if (string.IsNullOrWhiteSpace(root))
            {
                continue;
            }

            var canonical = Canonicalize(root.Trim());
            // Store without a trailing separator so a root equals its own path and
            // a child is matched with an explicit separator (avoids the
            // "/work" vs "/work-other" prefix pitfall).
            roots.Add(TrimTrailingSeparators(canonical));
        }

        _canonicalRoots = roots.ToArray();
    }

    /// <summary>
    /// <see langword="true"/> when at least one workspace root is configured, so
    /// the guard rejects paths outside it. When <see langword="false"/> the guard
    /// only normalises paths and admits everything (the opt-out for hosts that
    /// ingest arbitrary local paths).
    /// </summary>
    public bool IsEnforcing => _canonicalRoots.Length != 0;

    /// <summary>
    /// Resolves <paramref name="requestedPath"/> to its real on-disk location and,
    /// when the guard is enforcing, asserts it sits inside an allowed workspace
    /// root. Returns the canonical path the caller should walk.
    /// </summary>
    /// <param name="requestedPath">The caller-supplied repository path.</param>
    /// <returns>The canonical, in-bounds path.</returns>
    /// <exception cref="ArgumentException"><paramref name="requestedPath"/> is null, empty, or whitespace.</exception>
    /// <exception cref="RepoContextWorkspaceViolationException">
    /// The resolved path is outside every configured workspace root.
    /// </exception>
    public string Resolve(string requestedPath)
    {
        if (string.IsNullOrWhiteSpace(requestedPath))
        {
            throw new ArgumentException("The repository path must be provided.", nameof(requestedPath));
        }

        if (!IsEnforcing)
        {
            return Path.GetFullPath(requestedPath);
        }

        var canonical = TrimTrailingSeparators(Canonicalize(requestedPath));
        foreach (var root in _canonicalRoots)
        {
            if (canonical.Equals(root, PathComparison)
                || canonical.StartsWith(root + Path.DirectorySeparatorChar, PathComparison))
            {
                return canonical;
            }
        }

        throw new RepoContextWorkspaceViolationException(
            "The requested repository path resolves outside the mounted workspace and was refused. "
            + "Register a directory inside the workspace root.");
    }

    /// <summary>
    /// Resolves a path to its real location: normalises <c>..</c> lexically via
    /// <see cref="Path.GetFullPath(string)"/>, then follows any symlink components
    /// (including a symlinked root or intermediate directory) to their targets.
    /// A non-existent tail is left as-is - the walker reports the missing
    /// directory - so a path is never admitted merely because it does not exist.
    /// </summary>
    private static string Canonicalize(string path)
    {
        var current = Path.GetFullPath(path);
        for (var hop = 0; hop <= MaxSymlinkHops; hop++)
        {
            if (!TryExpandFirstLink(current, out var expanded))
            {
                return current;
            }

            current = expanded;
        }

        throw new RepoContextWorkspaceViolationException(
            "The requested repository path has too many symbolic-link indirections and was refused.");
    }

    /// <summary>
    /// Scans <paramref name="fullPath"/> from the filesystem root toward the leaf
    /// for the first component that is a symbolic link. When one is found it is
    /// replaced by its (possibly relative) target and the remaining components are
    /// re-appended, yielding a path with that one link expanded; the caller loops
    /// until no link remains. Returns <see langword="false"/> when no component is
    /// a link.
    /// </summary>
    private static bool TryExpandFirstLink(string fullPath, out string expanded)
    {
        var root = Path.GetPathRoot(fullPath) ?? string.Empty;
        var remainder = fullPath.Length > root.Length ? fullPath[root.Length..] : string.Empty;
        var segments = remainder.Split(Separators, StringSplitOptions.RemoveEmptyEntries);

        var accumulated = root;
        for (var i = 0; i < segments.Length; i++)
        {
            accumulated = accumulated.Length == 0
                ? segments[i]
                : Path.Combine(accumulated, segments[i]);

            var target = ReadLinkTarget(accumulated);
            if (target is null)
            {
                continue;
            }

            var resolvedBase = Path.IsPathRooted(target)
                ? target
                : Path.Combine(Path.GetDirectoryName(accumulated) ?? root, target);

            var rebuilt = Path.GetFullPath(resolvedBase);
            for (var j = i + 1; j < segments.Length; j++)
            {
                rebuilt = Path.Combine(rebuilt, segments[j]);
            }

            expanded = rebuilt;
            return true;
        }

        expanded = fullPath;
        return false;
    }

    /// <summary>
    /// Returns the raw target of <paramref name="path"/> when it is a symbolic
    /// link, or <see langword="null"/> when it is an ordinary file or directory,
    /// does not exist, or cannot be inspected. Uses
    /// <see cref="FileSystemInfo.LinkTarget"/> (the recorded target string) so a
    /// relative target is resolved against the link's own directory, not the
    /// process working directory.
    /// </summary>
    private static string? ReadLinkTarget(string path)
    {
        try
        {
            FileSystemInfo? info =
                Directory.Exists(path) ? new DirectoryInfo(path)
                : File.Exists(path) ? new FileInfo(path)
                : null;
            return info?.LinkTarget;
        }
        catch (IOException)
        {
            return null;
        }
        catch (UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static string TrimTrailingSeparators(string path)
    {
        var trimmed = path.TrimEnd(Separators);
        // Preserve a bare filesystem root (for example "/" or "C:\").
        return trimmed.Length == 0 ? path : trimmed;
    }
}
