using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Builds valid manifests that carry a presentation and a UI bundle, with the digests and bundle digest a
/// validator recomputes, plus the matching asset bytes. Shared with the facade test project by file link.
/// </summary>
internal static class UiTestManifests
{
    public const string EntryPath = "index.html";
    public const string IconPath = "icon.svg";
    public const string ScriptPath = "app.js";

    public static readonly byte[] EntryBytes = Encoding.UTF8.GetBytes("<main id=\"app\">Hello</main>");
    public static readonly byte[] IconBytes = Encoding.UTF8.GetBytes("<svg xmlns=\"http://www.w3.org/2000/svg\"/>");
    public static readonly byte[] ScriptBytes = Encoding.UTF8.GetBytes("export const ready = true;");

    /// <summary>The asset bytes a UI manifest built here pins, by path.</summary>
    public static IReadOnlyDictionary<string, byte[]> Assets { get; } = new Dictionary<string, byte[]>(StringComparer.Ordinal)
    {
        [EntryPath] = EntryBytes,
        [IconPath] = IconBytes,
        [ScriptPath] = ScriptBytes,
    };

    public static string Sha256(byte[] content) => Convert.ToHexStringLower(SHA256.HashData(content));

    /// <summary>Returns <paramref name="manifest"/> with a presentation, an icon and a UI bundle requesting <paramref name="bridge"/>.</summary>
    public static AppManifest WithUi(AppManifest manifest, params AppUiBridgeDeclaration[] bridge)
    {
        AppUiAsset[] assets =
        [
            new() { Path = EntryPath, MediaType = "text/html", Digest = Sha256(EntryBytes) },
            new() { Path = IconPath, MediaType = "image/svg+xml", Digest = Sha256(IconBytes) },
            new() { Path = ScriptPath, MediaType = "text/javascript", Digest = Sha256(ScriptBytes) },
        ];
        return manifest with
        {
            Presentation = new AppPresentation
            {
                DisplayName = "Notes <b>app</b>",
                Summary = "Take notes.",
                Description = "Line one.\nLine two.",
                Icon = new AppIconReference { Path = IconPath, Digest = Sha256(IconBytes) },
                Categories = ["productivity"],
                DocumentationUrl = "https://example.com/docs",
                PublisherDisplayName = "Contoso",
            },
            Ui = new AppUiDeclaration
            {
                Entry = EntryPath,
                Scripts = [new AppUiScript { Path = ScriptPath, Module = true }],
                Assets = assets,
                BundleDigest = AppUiBundle.ComputeBundleDigest(assets),
                Bridge = bridge.Length == 0 ? null : bridge,
            },
        };
    }

    public static AppUiBridgeDeclaration Bridge(string operation, params string[] trees) =>
        new() { Operation = operation, Trees = trees.Length == 0 ? null : trees };
}
