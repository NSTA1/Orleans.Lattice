using System.Buffers.Text;
using System.Text;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// The opaque continuation of a merged catalogue listing: one cursor per selected source, recording the
/// source continuation of the page being read, how many of that page's entries were already consumed, and
/// whether the source is exhausted. It is untrusted caller input on the way back in, so decoding validates
/// every field and a malformed token decodes to nothing rather than throwing.
/// </summary>
/// <remarks>
/// The wire form is base64url over <c>1;{key}:{offset}:{state}:{cursor};...</c>, where the state is
/// <c>d</c> for an exhausted source, <c>n</c> for a source whose current page is its first, and <c>c</c> when
/// the base64url-encoded source cursor follows. Each source validates its own cursor when it is replayed.
/// </remarks>
internal static class AppCatalogContinuation
{
    private const string Version = "1";
    private const int MaxTokenLength = 16 * 1024;

    /// <summary>One source's position in a merged listing.</summary>
    /// <param name="Key">The source key.</param>
    /// <param name="PageToken">The source continuation of the page being read, or null for its first page.</param>
    /// <param name="Offset">The number of entries of that page already consumed.</param>
    /// <param name="Done">Whether the source is exhausted.</param>
    public readonly record struct SourceCursor(string Key, string? PageToken, int Offset, bool Done);

    /// <summary>Encodes the cursors, or returns null when every source is exhausted.</summary>
    /// <param name="cursors">The cursors, in listing order.</param>
    /// <returns>The opaque token, or null.</returns>
    public static string? Encode(IReadOnlyList<SourceCursor> cursors)
    {
        var any = false;
        var text = new StringBuilder(Version);
        foreach (var cursor in cursors)
        {
            any |= !cursor.Done;
            text.Append(';').Append(cursor.Key).Append(':').Append(cursor.Offset).Append(':');
            if (cursor.Done)
                text.Append("d:");
            else if (cursor.PageToken is null)
                text.Append("n:");
            else
                text.Append("c:").Append(Base64Url.EncodeToString(Encoding.UTF8.GetBytes(cursor.PageToken)));
        }

        return any ? Base64Url.EncodeToString(Encoding.UTF8.GetBytes(text.ToString())) : null;
    }

    /// <summary>
    /// Decodes a token against the sources being listed. The token must name exactly those source keys, in
    /// order, with non-negative offsets; anything else is malformed.
    /// </summary>
    /// <param name="token">The caller-supplied token.</param>
    /// <param name="sources">The sources being listed, in listing order.</param>
    /// <param name="cursors">The decoded cursors when the token is well formed.</param>
    /// <returns><c>true</c> when the token is well formed.</returns>
    public static bool TryDecode(string token, IReadOnlyList<IAppCatalogSource> sources, out SourceCursor[] cursors)
    {
        cursors = [];
        if (string.IsNullOrEmpty(token) || token.Length > MaxTokenLength)
            return false;

        string text;
        try
        {
            text = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(token));
        }
        catch (FormatException)
        {
            return false;
        }

        var parts = text.Split(';');
        if (parts.Length != sources.Count + 1 || parts[0] != Version)
            return false;

        var decoded = new SourceCursor[sources.Count];
        for (var i = 0; i < decoded.Length; i++)
        {
            var fields = parts[i + 1].Split(':');
            if (fields.Length != 4
                || !string.Equals(fields[0], sources[i].Descriptor.Key, StringComparison.Ordinal)
                || !int.TryParse(fields[1], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var offset))
            {
                return false;
            }

            switch (fields[2])
            {
                case "d" when fields[3].Length == 0:
                    decoded[i] = new SourceCursor(fields[0], null, offset, Done: true);
                    break;
                case "n" when fields[3].Length == 0:
                    decoded[i] = new SourceCursor(fields[0], null, offset, Done: false);
                    break;
                case "c" when fields[3].Length > 0:
                    try
                    {
                        decoded[i] = new SourceCursor(fields[0], Encoding.UTF8.GetString(Base64Url.DecodeFromChars(fields[3])), offset, Done: false);
                    }
                    catch (FormatException)
                    {
                        return false;
                    }

                    break;
                default:
                    return false;
            }
        }

        cursors = decoded;
        return true;
    }
}
