using System.Text;

namespace Orleans.Lattice.Explorer.Shell.Navigation.Address;

/// <summary>
/// The address grammar's character rules: which characters a path segment or a
/// query value carries literally, how everything else is percent-encoded, and how
/// an encoded component is decoded strictly.
/// </summary>
/// <remarks>
/// <para>
/// A <b>segment</b> (a tenant id, an area path part) keeps only
/// <c>a-z 0-9 - . _ ~</c> literally and percent-encodes every other character,
/// <em>including upper-case letters</em>, as UTF-8 with upper-case hex. That is
/// what keeps every route segment lower case (epic decision E11) without losing
/// the case of a tree named <c>Orders</c>: it is addressed as <c>%4Frders</c>.
/// The dot segments <c>.</c> and <c>..</c> are encoded too, because a browser
/// would otherwise remove them from the path.
/// </para>
/// <para>
/// A <b>query value</b> is data rather than a route, so it keeps the whole RFC 3986
/// unreserved set, upper-case letters included, and percent-encodes the rest.
/// </para>
/// </remarks>
internal static class ExplorerAddressEncoding
{
    private const string HexDigits = "0123456789ABCDEF";

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// Whether <paramref name="value"/> is a keyword of the grammar - an area key or
    /// a query key: a lower-case ASCII letter followed by lower-case letters, digits
    /// and hyphens.
    /// </summary>
    /// <param name="value">The candidate keyword.</param>
    public static bool IsKeyword(string? value)
    {
        if (string.IsNullOrEmpty(value) || value[0] is < 'a' or > 'z')
        {
            return false;
        }

        foreach (var c in value)
        {
            if (c is not ((>= 'a' and <= 'z') or (>= '0' and <= '9') or '-'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether <paramref name="value"/> is well-formed UTF-16 (no unpaired
    /// surrogate), and so has exactly one UTF-8 encoding.
    /// </summary>
    /// <param name="value">The text to check.</param>
    public static bool IsWellFormed(string value)
    {
        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 >= value.Length || !char.IsLowSurrogate(value[i + 1]))
                {
                    return false;
                }

                i++;
            }
            else if (char.IsLowSurrogate(c))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Encodes one path segment in the canonical lower-case form.</summary>
    /// <param name="value">The decoded segment. Must be non-empty, well-formed text.</param>
    public static string EncodeSegment(string value) => value switch
    {
        "." => "%2E",
        ".." => "%2E%2E",
        _ => Encode(value, allowUpperCase: false),
    };

    /// <summary>Encodes a query value, keeping the RFC 3986 unreserved set literal.</summary>
    /// <param name="value">The decoded value. Must be well-formed text.</param>
    public static string EncodeQueryValue(string value) => Encode(value, allowUpperCase: true);

    /// <summary>
    /// Decodes a percent-encoded component. Every <c>%</c> must introduce two hex
    /// digits (either case) and the decoded bytes must be valid UTF-8; a raw
    /// character is taken as itself.
    /// </summary>
    /// <param name="encoded">The encoded component.</param>
    /// <param name="decoded">The decoded text, or <see langword="null"/> when decoding fails.</param>
    /// <returns><see langword="true"/> when the component decodes.</returns>
    public static bool TryDecode(ReadOnlySpan<char> encoded, out string? decoded)
    {
        if (encoded.IndexOf('%') < 0)
        {
            decoded = encoded.ToString();
            return IsWellFormed(decoded) || Fail(out decoded);
        }

        var builder = new StringBuilder(encoded.Length);
        var bytes = new List<byte>();

        for (var i = 0; i < encoded.Length; i++)
        {
            if (encoded[i] == '%')
            {
                if (i + 2 >= encoded.Length
                    || !TryHex(encoded[i + 1], out var high)
                    || !TryHex(encoded[i + 2], out var low))
                {
                    return Fail(out decoded);
                }

                bytes.Add((byte)((high << 4) | low));
                i += 2;
                continue;
            }

            if (!FlushBytes(bytes, builder))
            {
                return Fail(out decoded);
            }

            builder.Append(encoded[i]);
        }

        if (!FlushBytes(bytes, builder))
        {
            return Fail(out decoded);
        }

        decoded = builder.ToString();
        return IsWellFormed(decoded) || Fail(out decoded);
    }

    private static string Encode(string value, bool allowUpperCase)
    {
        StringBuilder? builder = null;
        Span<byte> utf8 = stackalloc byte[4];

        for (var i = 0; i < value.Length; i++)
        {
            var c = value[i];
            if (IsLiteral(c, allowUpperCase))
            {
                builder?.Append(c);
                continue;
            }

            builder ??= new StringBuilder(value.Length + 8).Append(value, 0, i);

            var width = char.IsHighSurrogate(c) ? 2 : 1;
            var length = StrictUtf8.GetBytes(value.AsSpan(i, width), utf8);
            i += width - 1;

            for (var b = 0; b < length; b++)
            {
                builder.Append('%').Append(HexDigits[utf8[b] >> 4]).Append(HexDigits[utf8[b] & 0xF]);
            }
        }

        return builder?.ToString() ?? value;
    }

    private static bool IsLiteral(char c, bool allowUpperCase) =>
        c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-' or '.' or '_' or '~'
        || (allowUpperCase && c is >= 'A' and <= 'Z');

    private static bool FlushBytes(List<byte> bytes, StringBuilder builder)
    {
        if (bytes.Count == 0)
        {
            return true;
        }

        try
        {
            builder.Append(StrictUtf8.GetString(bytes.ToArray()));
            bytes.Clear();
            return true;
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
    }

    private static bool TryHex(char c, out int value)
    {
        value = c switch
        {
            >= '0' and <= '9' => c - '0',
            >= 'a' and <= 'f' => c - 'a' + 10,
            >= 'A' and <= 'F' => c - 'A' + 10,
            _ => -1,
        };

        return value >= 0;
    }

    private static bool Fail(out string? decoded)
    {
        decoded = null;
        return false;
    }
}
