using System.IO;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Shared text-file enumeration for the content scanners (em-dash, mojibake).
/// Resolves a fixture's <see cref="HygieneScanScope"/> into the concrete set
/// of scannable text files: the scope's slice directories plus, for the core
/// fixture, the repo-level files that no package owns.
/// </summary>
public static class HygieneFiles
{
    // File extensions that hold binary payloads where a scanned byte sequence
    // is meaningless and would only produce noise. Everything else is treated
    // as text and scanned. `.log` files are local-only run artefacts
    // (gitignored) and are never tracked, so a leak there cannot reach a PR.
    private static readonly HashSet<string> BinaryExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".png", ".jpg", ".jpeg", ".gif", ".ico", ".bmp", ".pdf",
        ".dll", ".exe", ".pdb", ".so", ".dylib",
        ".zip", ".tar", ".gz", ".7z", ".nupkg", ".snk",
        ".dmp", ".bin",
        ".log",
    };

    /// <summary>
    /// Enumerates every non-binary text file within the supplied scope.
    /// </summary>
    public static IEnumerable<string> EnumerateTextFiles(string repoRoot, HygieneScanScope scope)
    {
        foreach (var file in HygieneRepository.EnumerateSliceFiles(repoRoot, scope, "*"))
        {
            if (BinaryExtensions.Contains(Path.GetExtension(file))) continue;
            yield return file;
        }

        if (!scope.OwnsRepoLevelFiles) yield break;

        foreach (var file in HygieneRepository.EnumerateRepoLevelFiles(repoRoot, "*", scope.OtherSliceRoots))
        {
            if (BinaryExtensions.Contains(Path.GetExtension(file))) continue;
            yield return file;
        }
    }
}
