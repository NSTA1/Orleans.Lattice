using System.IO;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The read-time half of the symlink boundary for repository ingestion.
/// <see cref="RepoTreeWalker"/> refuses to follow a symbolic link or junction when
/// it enumerates the tree, but the content, symbol and embedding passes re-open each
/// file later by joining its stored relative path onto the repository root, and an
/// ordinary open follows links. A file (or one of its parent directories) swapped
/// for a link between the walk and the read - or a path carried over from an older
/// index - would otherwise have the read follow it out of the workspace and the
/// retrieval tools serve the target's contents back to any caller. Every read site
/// consults <see cref="TraversesLink"/> immediately before opening, so a path is
/// refused whenever any component below the root is a reparse point, or when it
/// resolves outside the root lexically.
/// <para>
/// <b>Allocation.</b> One attribute stat per path component below the root; no
/// <see cref="FileSystemInfo"/> objects, caches, or regex.
/// </para>
/// </summary>
internal static class RepoContextLinkGuard
{
    private static readonly StringComparison PathComparison =
        OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    /// <summary>
    /// Returns <see langword="true"/> when reading <paramref name="fullPath"/> would
    /// leave <paramref name="repoRoot"/>: the path lies outside the root lexically, or
    /// the file or any directory between it and the root is a symbolic link,
    /// junction, or other reparse point. The root itself is not inspected - it was
    /// canonicalised by the workspace guard when the repository was registered. A
    /// component that does not exist ends the check with <see langword="false"/>, so
    /// the caller's own open reports the file missing exactly as before.
    /// </summary>
    /// <param name="repoRoot">The repository root the read must stay inside.</param>
    /// <param name="fullPath">The path about to be opened.</param>
    /// <returns><see langword="true"/> when the read must be refused.</returns>
    /// <exception cref="IOException">An attribute read failed for a reason other than absence.</exception>
    /// <exception cref="UnauthorizedAccessException">An attribute read was denied.</exception>
    public static bool TraversesLink(string repoRoot, string fullPath)
    {
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(repoRoot));
        var current = Path.TrimEndingDirectorySeparator(Path.GetFullPath(fullPath));

        if (!current.StartsWith(root + Path.DirectorySeparatorChar, PathComparison))
        {
            return true;
        }

        while (current.Length > root.Length)
        {
            FileAttributes attributes;
            try
            {
                attributes = File.GetAttributes(current);
            }
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
            {
                return false;
            }

            if ((attributes & FileAttributes.ReparsePoint) != 0)
            {
                return true;
            }

            var parent = Path.GetDirectoryName(current);
            if (parent is null)
            {
                return true;
            }

            current = parent;
        }

        return false;
    }
}
