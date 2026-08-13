using System.IO;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A best-effort, extension-based source-language classifier for bootstrap scans.
/// The mapping is deliberately small and additive: an unrecognised extension
/// yields the empty string rather than a guess, and richer detection (shebang
/// lines, content sniffing, per-language deep parsers) is layered in by later
/// work without changing this seam.
/// </summary>
internal static class LanguageClassifier
{
    private static readonly IReadOnlyDictionary<string, string> ByExtension =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [".cs"] = "csharp",
            [".fs"] = "fsharp",
            [".vb"] = "vbnet",
            [".js"] = "javascript",
            [".jsx"] = "javascript",
            [".mjs"] = "javascript",
            [".cjs"] = "javascript",
            [".ts"] = "typescript",
            [".tsx"] = "typescript",
            [".py"] = "python",
            [".go"] = "go",
            [".rs"] = "rust",
            [".java"] = "java",
            [".kt"] = "kotlin",
            [".rb"] = "ruby",
            [".php"] = "php",
            [".c"] = "c",
            [".h"] = "c",
            [".cpp"] = "cpp",
            [".cc"] = "cpp",
            [".hpp"] = "cpp",
            [".cxx"] = "cpp",
            [".swift"] = "swift",
            [".scala"] = "scala",
            [".sh"] = "shell",
            [".bash"] = "shell",
            [".ps1"] = "powershell",
            [".sql"] = "sql",
            [".md"] = "markdown",
            [".markdown"] = "markdown",
            [".json"] = "json",
            [".yaml"] = "yaml",
            [".yml"] = "yaml",
            [".xml"] = "xml",
            [".html"] = "html",
            [".css"] = "css",
            [".toml"] = "toml",
        };

    /// <summary>
    /// Classifies <paramref name="relativePath"/> by its extension, returning a
    /// stable language identifier or the empty string when the extension is not
    /// recognised.
    /// </summary>
    /// <param name="relativePath">The repository-relative path. Must not be <see langword="null"/>.</param>
    internal static string Classify(string relativePath)
    {
        ArgumentNullException.ThrowIfNull(relativePath);
        var extension = Path.GetExtension(relativePath);
        return extension.Length != 0 && ByExtension.TryGetValue(extension, out var language)
            ? language
            : string.Empty;
    }
}
