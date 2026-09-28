namespace Orleans.Lattice.Apps.Sources;

/// <summary>
/// Validation of app bundle asset paths and their media types, private to the app sources. A valid path is
/// already normalised: relative, <c>/</c>-separated, lower-case, with no empty, <c>.</c> or <c>..</c> segment,
/// no backslash, no drive or scheme separator, and no query or fragment. A path that is not already in that
/// form is rejected, never rewritten, so two spellings can never address the same asset.
/// </summary>
internal static class AppAssetPath
{
    /// <summary>The longest asset path accepted, in characters.</summary>
    public const int MaxLength = 512;

    /// <summary>Whether <paramref name="path"/> is a valid, already-normalised relative asset path.</summary>
    public static bool IsValid(string? path)
    {
        if (string.IsNullOrEmpty(path) || path.Length > MaxLength)
            return false;

        var segmentStart = 0;
        for (var i = 0; i <= path.Length; i++)
        {
            if (i == path.Length || path[i] == '/')
            {
                var length = i - segmentStart;
                if (length == 0)
                    return false;
                if (path[segmentStart] == '.' && (length == 1 || (length == 2 && path[segmentStart + 1] == '.')))
                    return false;
                segmentStart = i + 1;
                continue;
            }

            if (path[i] is not ((>= 'a' and <= 'z') or (>= '0' and <= '9') or '-' or '_' or '.'))
                return false;
        }

        return true;
    }

    /// <summary>
    /// The media type for a valid asset path, taken from its extension, restricted to the bundle media types
    /// an app manifest may declare. Returns null for any other extension.
    /// </summary>
    public static string? MediaTypeOf(string path)
    {
        var dot = path.LastIndexOf('.');
        var slash = path.LastIndexOf('/');
        if (dot <= slash + 1)
            return null;
        return path.AsSpan(dot + 1) switch
        {
            "html" or "htm" => "text/html",
            "css" => "text/css",
            "js" or "mjs" => "text/javascript",
            "svg" => "image/svg+xml",
            "png" => "image/png",
            "webp" => "image/webp",
            "woff2" => "font/woff2",
            "json" => "application/json",
            _ => null,
        };
    }
}
