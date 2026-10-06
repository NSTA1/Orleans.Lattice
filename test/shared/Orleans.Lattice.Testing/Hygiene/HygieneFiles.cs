using System.IO;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// Shared text-file enumeration for the content scanners (em-dash, mojibake).
/// Resolves a fixture's <see cref="HygieneScanScope"/> into the concrete set
/// of scannable text files: the scope's slice directories plus, for the core
/// fixture, the repo-level files that no package owns.
/// </summary>
/// <remarks>
/// <para>
/// The extension filter is an allow-list, inverted from the deny-list it
/// replaced (issue #3134). The polarity matters: the set of binary extensions
/// is open and unknowable, while the set of text extensions this repository
/// actually contains is small and closed. Under a deny-list an unrecognised or
/// extensionless file fell through and was read as text, so an opaque binary
/// blob was scanned for em-dashes.
/// </para>
/// <para>
/// Inverting a deny-list to an allow-list trades one silent failure for
/// another unless it is guarded, because an extension missing from the
/// allow-list is silently NOT scanned - strictly worse than scanning too much,
/// and precisely the vacuity the gates' denominator control exists to prevent.
/// <see cref="Classify"/> therefore has a third answer,
/// <see cref="HygieneFileKind.Unclassified"/>, and enumeration throws on it.
/// A new extension entering the repository fails loudly and is classified
/// once, rather than quietly dropping out of every content gate.
/// </para>
/// </remarks>
public static class HygieneFiles
{
    // Every extension tracked in this repository that holds text, plus common
    // neighbours that would otherwise trip the unclassified guard on their
    // first appearance. Kept exhaustive by that guard rather than by review.
    // The second row is the text the `videos/` HyperFrames workspace produces
    // or configures: caption tracks, transcript streams, ES modules, and the
    // Node toolchain dotfiles (`.nvmrc` has no stem, so its whole name is its
    // extension, exactly like `.gitignore`).
    private static readonly HashSet<string> TextExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".cs", ".md", ".csproj", ".razor", ".ps1", ".psm1", ".json", ".yml", ".yaml",
        ".srt", ".vtt", ".jsonl", ".mjs", ".cjs", ".nvmrc", ".npmrc",
        ".css", ".env", ".bicep", ".bicepparam", ".mutation", ".sql", ".py",
        ".proto", ".gitignore", ".dockerignore", ".gitattributes", ".gitmodules",
        ".props", ".targets", ".js", ".ts", ".svg", ".sh", ".slnx", ".sln",
        ".xaml", ".txt", ".example", ".tsv", ".csv", ".service", ".html", ".tla",
        ".cfg", ".conf", ".config", ".manifest", ".appxmanifest", ".xml",
        ".editorconfig", ".toml", ".ini", ".resx", ".http", ".bat", ".cmd", ".sample",
    };

    // Extensions that hold binary payloads, where a scanned byte sequence is
    // meaningless and would only produce noise. `.log` files are local-only run
    // artefacts and none is tracked, so listing it here preserves the previous
    // behaviour rather than introducing a new exclusion. The video and audio
    // row is classified ahead of its first tracked file so that the `videos/`
    // workspace cannot fail every content gate the day it commits a render,
    // a narration track, or a still, whichever hosting option is chosen.
    private static readonly HashSet<string> BinaryExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".png", ".jpg", ".jpeg", ".gif", ".ico", ".bmp", ".pdf", ".webp", ".avif",
        ".mp4", ".webm", ".mov", ".m4a", ".mp3", ".wav", ".ogg", ".opus", ".flac",
        ".ttf", ".otf", ".woff", ".woff2", ".eot",
        ".dll", ".exe", ".pdb", ".so", ".dylib",
        ".zip", ".tar", ".gz", ".7z", ".nupkg", ".snk",
        ".dmp", ".bin", ".onnx", ".wasm",
        ".log",
    };

    // Extensionless tracked files. Path.GetExtension returns an empty string
    // for these, so they cannot be classified by extension at all; every one
    // of them in this repository is text.
    private static readonly HashSet<string> TextFileNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "LICENSE", "LICENCE", "NOTICE", "Dockerfile", "Makefile", "CODEOWNERS", "AUTHORS",
    };

    /// <summary>
    /// Classifies a file as text, binary, or neither, by extension and then by
    /// file name for the extensionless case.
    /// </summary>
    /// <param name="path">The file path to classify.</param>
    /// <returns>The classification.</returns>
    public static HygieneFileKind Classify(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        var extension = Path.GetExtension(path);
        if (extension.Length == 0)
        {
            return TextFileNames.Contains(Path.GetFileName(path))
                ? HygieneFileKind.Text
                : HygieneFileKind.Unclassified;
        }

        if (BinaryExtensions.Contains(extension)) return HygieneFileKind.Binary;
        return TextExtensions.Contains(extension) ? HygieneFileKind.Text : HygieneFileKind.Unclassified;
    }

    /// <summary>
    /// Enumerates every tracked text file within the supplied scope.
    /// </summary>
    /// <param name="repoRoot">The repository root.</param>
    /// <param name="scope">The slice this fixture is responsible for.</param>
    /// <returns>The tracked text files in the scope.</returns>
    /// <exception cref="InvalidOperationException">
    /// A tracked file is classified neither text nor binary, so leaving it
    /// unscanned would be a silent gap in the gate.
    /// </exception>
    public static IEnumerable<string> EnumerateTextFiles(string repoRoot, HygieneScanScope scope)
    {
        foreach (var file in HygieneRepository.EnumerateSliceFiles(repoRoot, scope, "*"))
        {
            if (ShouldScan(file, repoRoot)) yield return file;
        }

        if (!scope.OwnsRepoLevelFiles) yield break;

        foreach (var file in HygieneRepository.EnumerateRepoLevelFiles(repoRoot, "*", scope.OtherSliceRoots))
        {
            if (ShouldScan(file, repoRoot)) yield return file;
        }
    }

    /// <summary>
    /// Reads a file's lines, converting an environment-level read failure into
    /// a description rather than letting it abort the whole gate.
    /// </summary>
    /// <remarks>
    /// A locked or unreadable file is an environment condition, not a hygiene
    /// violation, and the two must not be indistinguishable. Before #3134 the
    /// em-dash gate let the <see cref="IOException"/> escape, which presented
    /// as a red hygiene gate and read as "you introduced an em-dash"; the
    /// mojibake gate swallowed it silently, which is the opposite failure. The
    /// caller collects the returned description and asserts on it separately,
    /// so the condition is reported as itself and the scan still completes.
    /// </remarks>
    /// <param name="path">The file to read.</param>
    /// <param name="failure">The failure description, or null on success.</param>
    /// <returns>The file's lines, or null when it could not be read.</returns>
    public static string[]? TryReadLines(string path, out string? failure)
    {
        try
        {
            failure = null;
            return File.ReadAllLines(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            failure = Describe(path, ex);
            return null;
        }
    }

    /// <summary>
    /// Reads a file's whole text as UTF-8, converting an environment-level
    /// read failure into a description rather than aborting the gate.
    /// </summary>
    /// <param name="path">The file to read.</param>
    /// <param name="failure">The failure description, or null on success.</param>
    /// <returns>The file's text, or null when it could not be read.</returns>
    public static string? TryReadText(string path, out string? failure)
    {
        try
        {
            failure = null;
            return File.ReadAllText(path, System.Text.Encoding.UTF8);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            failure = Describe(path, ex);
            return null;
        }
    }

    private static string Describe(string path, Exception ex) =>
        path.Replace('\\', '/') + ": " + ex.GetType().Name + ": " + ex.Message;

    /// <summary>
    /// Decides whether a tracked file is scanned by the content gates, and
    /// refuses to answer for a file that matches no classification rule.
    /// </summary>
    /// <param name="file">The tracked file path.</param>
    /// <param name="repoRoot">The repository root, used only to shorten the message.</param>
    /// <returns>True when the file is text and should be scanned; false when it is binary.</returns>
    /// <exception cref="InvalidOperationException">
    /// The file is classified neither text nor binary. This is deliberately an
    /// error and not a skip: the allow-list's failure mode is a file that is
    /// silently NOT scanned, which no gate result would ever reveal.
    /// </exception>
    public static bool ShouldScan(string file, string repoRoot)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(repoRoot);

        return Classify(file) switch
        {
            HygieneFileKind.Text => true,
            HygieneFileKind.Binary => false,
            _ => throw new InvalidOperationException(
                "The tracked file '" + Path.GetRelativePath(repoRoot, file).Replace('\\', '/')
                + "' is classified neither as text nor as binary, so it would be silently excluded from every "
                + "content scan. Add its extension to HygieneFiles.TextExtensions or HygieneFiles.BinaryExtensions, "
                + "or its name to HygieneFiles.TextFileNames if it has no extension."),
        };
    }
}
