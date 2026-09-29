using System.Reflection;
using System.Text.Json;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.UI.Design;

/// <summary>
/// Reads the static web assets the build actually publishes for a project - the
/// manifest the Razor SDK writes, which is what a head serves at runtime - so a
/// test can assert on what is served rather than on what is on disk.
/// </summary>
internal static class StaticWebAssetManifest
{
    /// <summary>One published asset.</summary>
    /// <param name="RelativePath">The path under the base path, with its fingerprint pattern removed.</param>
    /// <param name="Identity">The absolute path of the file whose bytes are served.</param>
    /// <param name="BasePath">The asset's base path, such as <c>_content/Orleans.Lattice.Explorer.UI</c>.</param>
    /// <param name="IsCompressed">Whether this is a pre-compressed (gzip) variant of another asset.</param>
    internal sealed record Asset(string RelativePath, string Identity, string BasePath, bool IsCompressed);

    /// <summary>The build configuration this test assembly was compiled in.</summary>
    public static string Configuration =>
        typeof(StaticWebAssetManifest).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration
        ?? "Debug";

    /// <summary>Every asset in the named project's build manifest.</summary>
    /// <param name="projectDirectory">The project's directory, relative to the repository root.</param>
    public static IReadOnlyList<Asset> Assets(string projectDirectory)
    {
        var path = Path.Combine(
            HygieneRepository.FindRepoRoot(),
            projectDirectory.Replace('/', Path.DirectorySeparatorChar),
            "obj",
            Configuration,
            "net10.0",
            "staticwebassets.build.json");

        Assert.That(File.Exists(path), Is.True,
            $"the build must have written {path}; the test project references the Shell, so building the tests builds it");

        using var document = JsonDocument.Parse(File.ReadAllText(path));
        var assets = new List<Asset>();
        foreach (var asset in document.RootElement.GetProperty("Assets").EnumerateArray())
        {
            var relative = asset.GetProperty("RelativePath").GetString() ?? string.Empty;
            var compressed = relative.EndsWith(".gz", StringComparison.Ordinal);
            assets.Add(new Asset(
                StripFingerprint(compressed ? relative[..^3] : relative),
                asset.GetProperty("Identity").GetString() ?? string.Empty,
                asset.GetProperty("BasePath").GetString() ?? string.Empty,
                compressed));
        }

        Assert.That(assets, Is.Not.Empty, "the manifest must list assets");
        return assets;
    }

    /// <summary>
    /// Removes the <c>#[.{fingerprint...}]?</c> pattern the SDK writes into a
    /// relative path, leaving the path a browser requests.
    /// </summary>
    /// <param name="relativePath">The manifest's relative path.</param>
    public static string StripFingerprint(string relativePath)
    {
        var start = relativePath.IndexOf("#[", StringComparison.Ordinal);
        if (start < 0)
        {
            return relativePath;
        }

        var end = relativePath.IndexOf("]?", start, StringComparison.Ordinal);
        return end < 0 ? relativePath : relativePath[..start] + relativePath[(end + 2)..];
    }
}
