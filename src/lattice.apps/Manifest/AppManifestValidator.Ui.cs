using System.Text;
using System.Text.Unicode;

namespace Orleans.Lattice.Apps;

public static partial class AppManifestValidator
{
    private const string UiEntryPath = "$.ui.entry";

    /// <summary>
    /// Checks the bytes of a UI entry fragment, once they have been read: at most
    /// <see cref="AppUiBundle.MaxAssetBytes"/> of well-formed UTF-8, and a fragment rather than a
    /// document, so it contains no <c>&lt;script</c> (in any letter case, anywhere, including inside
    /// comments or foreign content) and no <c>&lt;html&gt;</c> or <c>&lt;head&gt;</c> tag. Returns
    /// the diagnostics, empty when the fragment is acceptable; never throws.
    /// </summary>
    /// <param name="utf8Fragment">The entry asset's bytes.</param>
    public static IReadOnlyList<AppManifestError> ValidateUiEntryFragment(ReadOnlySpan<byte> utf8Fragment)
    {
        if (utf8Fragment.Length > AppUiBundle.MaxAssetBytes)
            return [new("limit", UiEntryPath, $"An asset may be at most {AppUiBundle.MaxAssetBytes} bytes.")];
        List<AppManifestError>? errors = null;
        if (!Utf8.IsValid(utf8Fragment))
            (errors ??= []).Add(new("encoding", UiEntryPath, "The entry fragment must be well-formed UTF-8."));
        if (ContainsTag(utf8Fragment, "script"u8, prefixOnly: true))
            (errors ??= []).Add(new("fragment", UiEntryPath, "The entry fragment may not contain a <script element; ship scripts as bundle assets."));
        if (ContainsTag(utf8Fragment, "html"u8, prefixOnly: false) || ContainsTag(utf8Fragment, "head"u8, prefixOnly: false))
            (errors ??= []).Add(new("fragment", UiEntryPath, "The entry is an HTML fragment inserted into the frame body, not a document; remove <html> and <head>."));
        return errors ?? (IReadOnlyList<AppManifestError>)[];
    }

    private static bool ContainsTag(ReadOnlySpan<byte> html, ReadOnlySpan<byte> name, bool prefixOnly)
    {
        for (var start = html.IndexOf((byte)'<'); start >= 0;)
        {
            var rest = html[(start + 1)..];
            if (rest.Length >= name.Length && Ascii.EqualsIgnoreCase(rest[..name.Length], name) &&
                (prefixOnly || rest.Length == name.Length || rest[name.Length] is (byte)' ' or (byte)'\t' or (byte)'\n' or (byte)'\f' or (byte)'\r' or (byte)'/' or (byte)'>'))
                return true;
            var next = rest.IndexOf((byte)'<');
            if (next < 0)
                return false;
            start += next + 1;
        }
        return false;
    }

    private static Dictionary<string, AppUiAsset>? ValidateUi(AppUiDeclaration? ui, HashSet<string> trees, List<AppManifestError> errors)
    {
        if (ui is null)
            return null;
        void Error(string code, string path, string message) => errors.Add(new(code, path, message));

        Dictionary<string, AppUiAsset>? assets = null;
        var digestInputsValid = false;
        if (ui.Assets is null || ui.Assets.Length == 0)
            Error("required", "$.ui.assets", "At least one asset, the entry, is required.");
        else if (ui.Assets.Length > AppUiBundle.MaxAssets)
            Error("limit", "$.ui.assets", $"A bundle may list at most {AppUiBundle.MaxAssets} assets.");
        else
        {
            assets = new(ui.Assets.Length, StringComparer.Ordinal);
            digestInputsValid = true;
            for (var i = 0; i < ui.Assets.Length; i++)
            {
                var path = $"$.ui.assets[{i}]";
                if (ui.Assets[i] is not { } asset)
                {
                    Error("required", path, "An asset cannot be null.");
                    digestInputsValid = false;
                    continue;
                }
                if (!AppUiBundle.IsValidPath(asset.Path))
                {
                    Error("path", path + ".path", PathMessage);
                    digestInputsValid = false;
                }
                else if (!assets.TryAdd(asset.Path, asset))
                {
                    Error("duplicate", path + ".path", "Asset paths must be unique.");
                    digestInputsValid = false;
                }
                if (asset.MediaType is null || !AppUiBundle.AllowedMediaTypes.Contains(asset.MediaType))
                    Error("media-type", path + ".mediaType", "The media type is not in the bundle allow-list.");
                if (!AppUiBundle.IsValidDigest(asset.Digest))
                {
                    Error("digest", path + ".digest", DigestMessage);
                    digestInputsValid = false;
                }
            }
        }

        if (!AppUiBundle.IsValidDigest(ui.BundleDigest))
            Error("digest", "$.ui.bundleDigest", DigestMessage);
        else if (digestInputsValid && !string.Equals(AppUiBundle.ComputeBundleDigest(ui.Assets!), ui.BundleDigest, StringComparison.Ordinal))
            Error("bundle-digest", "$.ui.bundleDigest", "The bundle digest does not match the digest recomputed from the assets.");

        void Reference(string? value, string path, string mediaType)
        {
            if (string.IsNullOrEmpty(value))
                Error("required", path, "A bundle path is required.");
            else if (!AppUiBundle.IsValidPath(value))
                Error("path", path, PathMessage);
            else if (assets is not null)
            {
                if (!assets.TryGetValue(value, out var asset))
                    Error("reference", path, "The path must be listed in ui.assets.");
                else if (!string.Equals(asset.MediaType, mediaType, StringComparison.Ordinal))
                    Error("media-type", path, $"The asset must have media type {mediaType}.");
            }
        }

        Reference(ui.Entry, UiEntryPath, "text/html");

        if (ui.Styles is not null)
        {
            if (ui.Styles.Length > AppUiBundle.MaxAssets)
                Error("limit", "$.ui.styles", $"A bundle may list at most {AppUiBundle.MaxAssets} stylesheets.");
            else
            {
                HashSet<string> seen = new(StringComparer.Ordinal);
                for (var i = 0; i < ui.Styles.Length; i++)
                {
                    var path = $"$.ui.styles[{i}]";
                    Reference(ui.Styles[i], path, "text/css");
                    if (ui.Styles[i] is { } style && !seen.Add(style))
                        Error("duplicate", path, "A stylesheet may be listed only once.");
                }
            }
        }

        if (ui.Scripts is not null)
        {
            if (ui.Scripts.Length > AppUiBundle.MaxAssets)
                Error("limit", "$.ui.scripts", $"A bundle may list at most {AppUiBundle.MaxAssets} scripts.");
            else
            {
                HashSet<string> seen = new(StringComparer.Ordinal);
                for (var i = 0; i < ui.Scripts.Length; i++)
                {
                    var path = $"$.ui.scripts[{i}]";
                    if (ui.Scripts[i] is not { } script)
                    {
                        Error("required", path, "A script cannot be null.");
                        continue;
                    }
                    Reference(script.Path, path + ".path", "text/javascript");
                    if (script.Path is not null && !seen.Add(script.Path))
                        Error("duplicate", path + ".path", "A script may be listed only once.");
                }
            }
        }

        if (ui.MinProtocol < 1 || ui.MinProtocol > AppUiProtocol.Current)
            Error("protocol", "$.ui.minProtocol", $"The minimum protocol must be between 1 and {AppUiProtocol.Current}.");

        if (ui.Bridge is not null)
        {
            if (ui.Bridge.Length > AppManifestLimits.MaxSectionItems)
                Error("limit", "$.ui.bridge", $"A section may hold at most {AppManifestLimits.MaxSectionItems} entries.");
            else
            {
                HashSet<string> operations = new(StringComparer.Ordinal);
                for (var i = 0; i < ui.Bridge.Length; i++)
                {
                    var path = $"$.ui.bridge[{i}]";
                    if (ui.Bridge[i] is not { } declaration)
                    {
                        Error("required", path, "A bridge declaration cannot be null.");
                        continue;
                    }
                    if (!AppUiBridgeOperations.IsKnown(declaration.Operation))
                        Error("bridge", path + ".operation", "Unknown bridge operation.");
                    else if (!operations.Add(declaration.Operation))
                        Error("duplicate", path + ".operation", "A bridge operation may be requested only once.");
                    if (declaration.Trees is null)
                        continue;
                    if (!AppUiBridgeOperations.IsDataOperation(declaration.Operation))
                        Error("bridge", path + ".trees", "Only data operations may name trees.");
                    else if (declaration.Trees.Length == 0)
                        Error("bridge", path + ".trees", "Omit trees to cover every declared tree.");
                    else if (declaration.Trees.Length > AppManifestLimits.MaxSectionItems)
                        Error("limit", path + ".trees", $"A section may hold at most {AppManifestLimits.MaxSectionItems} entries.");
                    else
                    {
                        HashSet<string> named = new(StringComparer.Ordinal);
                        for (var t = 0; t < declaration.Trees.Length; t++)
                        {
                            var tree = declaration.Trees[t];
                            var treePath = $"{path}.trees[{t}]";
                            if (!IsName(tree))
                                Error("name", treePath, "Expected a local tree name, not a path or wildcard.");
                            else if (!trees.Contains(tree))
                                Error("reference", treePath, "The tree must be declared by this app.");
                            else if (!named.Add(tree))
                                Error("duplicate", treePath, "A tree may be named only once per operation.");
                        }
                    }
                }
            }
        }

        return assets;
    }

    private static void ValidatePresentation(AppPresentation? presentation, Dictionary<string, AppUiAsset>? uiAssets, List<AppManifestError> errors)
    {
        if (presentation is null)
            return;
        void Error(string code, string path, string message) => errors.Add(new(code, path, message));

        void Text(string? value, string path, int maxLength, bool required, bool multiline)
        {
            if (value is null)
            {
                if (required)
                    Error("required", path, "Non-empty text is required.");
                return;
            }
            if (string.IsNullOrWhiteSpace(value))
            {
                Error("required", path, "Non-empty text is required when present; omit the member instead.");
                return;
            }
            if (value.Length > maxLength)
                Error("limit", path, $"Text may be at most {maxLength} characters.");
            foreach (var c in value)
                if (IsForbiddenPresentationChar(c, multiline))
                {
                    Error("text", path, multiline
                        ? "Text may not contain control characters other than tabs and line breaks, or bidirectional overrides."
                        : "Single-line text may not contain control characters, line breaks or bidirectional overrides.");
                    break;
                }
        }

        Text(presentation.DisplayName, "$.presentation.displayName", AppManifestLimits.MaxDisplayNameLength, required: true, multiline: false);
        Text(presentation.Summary, "$.presentation.summary", AppManifestLimits.MaxSummaryLength, required: false, multiline: false);
        Text(presentation.Description, "$.presentation.description", AppManifestLimits.MaxPresentationDescriptionLength, required: false, multiline: true);
        Text(presentation.PublisherDisplayName, "$.presentation.publisherDisplayName", AppManifestLimits.MaxPublisherDisplayNameLength, required: false, multiline: false);

        if (presentation.Categories is not null)
        {
            if (presentation.Categories.Length > AppManifestLimits.MaxCategories)
                Error("limit", "$.presentation.categories", $"At most {AppManifestLimits.MaxCategories} categories are allowed.");
            else
            {
                HashSet<string> seen = new(StringComparer.Ordinal);
                for (var i = 0; i < presentation.Categories.Length; i++)
                {
                    var category = presentation.Categories[i];
                    var path = $"$.presentation.categories[{i}]";
                    if (!IsCategory(category))
                        Error("category", path, "Expected 2-31 characters matching ^[a-z][a-z0-9-]{1,30}$.");
                    else if (!seen.Add(category))
                        Error("duplicate", path, "Categories must be unique.");
                }
            }
        }

        if (presentation.DocumentationUrl is not null && !IsHttpsUrl(presentation.DocumentationUrl))
            Error("url", "$.presentation.documentationUrl",
                $"Expected an absolute https URL of at most {AppManifestLimits.MaxUrlLength} characters, without white space or user information.");

        if (presentation.Icon is { } icon)
        {
            const string iconPath = "$.presentation.icon";
            var pathValid = AppUiBundle.IsValidPath(icon.Path);
            if (!pathValid)
                Error("path", iconPath + ".path", PathMessage);
            else if (!HasIconExtension(icon.Path))
                Error("media-type", iconPath + ".path", "The icon must be an .svg, .png or .webp asset.");
            var digestValid = AppUiBundle.IsValidDigest(icon.Digest);
            if (!digestValid)
                Error("digest", iconPath + ".digest", DigestMessage);
            if (pathValid && uiAssets is not null)
            {
                if (!uiAssets.TryGetValue(icon.Path, out var asset))
                    Error("reference", iconPath + ".path", "The icon must be listed in ui.assets.");
                else
                {
                    if (asset.MediaType is null || !AppUiBundle.IconMediaTypes.Contains(asset.MediaType))
                        Error("media-type", iconPath + ".path", "The icon asset must be image/svg+xml, image/png or image/webp.");
                    if (digestValid && !string.Equals(asset.Digest, icon.Digest, StringComparison.Ordinal))
                        Error("digest", iconPath + ".digest", "The icon digest does not match its ui.assets entry.");
                }
            }
        }
    }

    private const string PathMessage =
        "Expected a normalised, relative, lower-case bundle path of [a-z0-9._-] segments separated by '/', without '.' or '..' segments, a query or a fragment.";

    private const string DigestMessage = "Expected a SHA-256 digest of 64 lower-case hexadecimal characters.";

    private static bool IsForbiddenPresentationChar(char c, bool multiline) => c switch
    {
        '\n' or '\r' or '\t' or '\u2028' or '\u2029' => !multiline,
        >= '\u202A' and <= '\u202E' or >= '\u2066' and <= '\u2069' => true,
        _ => char.IsControl(c),
    };

    private static bool HasIconExtension(string path) =>
        path.EndsWith(".svg", StringComparison.Ordinal) || path.EndsWith(".png", StringComparison.Ordinal) || path.EndsWith(".webp", StringComparison.Ordinal);

    internal static bool IsCategory(string? value)
    {
        if (value is null || value.Length is < 2 or > 31 || value[0] is < 'a' or > 'z')
            return false;
        foreach (var c in value)
            if (c is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '-')
                return false;
        return true;
    }

    internal static bool IsHttpsUrl(string value)
    {
        if (value.Length > AppManifestLimits.MaxUrlLength)
            return false;
        foreach (var c in value)
            if (char.IsWhiteSpace(c) || char.IsControl(c))
                return false;
        return Uri.TryCreate(value, UriKind.Absolute, out var uri) &&
               uri.Scheme == Uri.UriSchemeHttps &&
               uri.UserInfo.Length == 0 &&
               uri.Host.Length > 0;
    }
}
