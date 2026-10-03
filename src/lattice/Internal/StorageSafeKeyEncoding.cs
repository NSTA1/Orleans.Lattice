using System.Globalization;
using System.Text;

namespace Orleans.Lattice;

/// <summary>
/// The percent-encoding shared by the compound grain keys whose grains persist
/// state. A durable grain-storage provider derives its persisted key from the
/// grain key, so such a key has to be safe for the lowest common denominator of
/// durable stores rather than any single provider - Azure Table storage rejects
/// the control characters U+0000-U+001F and U+007F-U+009F and the characters
/// '/', '\', '#' and '?' in a partition/row key, and Azure Cosmos DB forbids the
/// same set in a document id. Fields are delimited by <see cref="FieldSeparator"/>
/// and every character that is unsafe in such a key - plus the delimiter and the
/// <see cref="EscapeChar"/> escape marker themselves - is percent-encoded as four
/// hex digits. That keeps the key valid on any of these providers while staying
/// collision-free: the delimiter can never appear inside an encoded field, so
/// distinct field values never share a key.
/// </summary>
internal static class StorageSafeKeyEncoding
{
    /// <summary>The delimiter between the fields of a compound key.</summary>
    internal const char FieldSeparator = '|';

    /// <summary>The marker that introduces a four-hex-digit escaped code unit.</summary>
    internal const char EscapeChar = '%';

    /// <summary>
    /// Appends <paramref name="value"/> to <paramref name="builder"/>, copying
    /// safe characters verbatim and percent-encoding every other UTF-16 code unit
    /// as <see cref="EscapeChar"/> followed by four upper-case hex digits, so every
    /// escaped run has a fixed width and is unambiguous and reversible.
    /// </summary>
    /// <param name="builder">The key being built.</param>
    /// <param name="value">The field value to encode.</param>
    internal static void AppendEncoded(StringBuilder builder, string value)
    {
        foreach (var ch in value)
        {
            if (IsSafe(ch))
            {
                builder.Append(ch);
            }
            else
            {
                builder.Append(EscapeChar);
                builder.Append(((int)ch).ToString("X4", CultureInfo.InvariantCulture));
            }
        }
    }

    private static bool IsSafe(char ch) =>
        ch != FieldSeparator
        && ch != EscapeChar
        && ch is not ('/' or '\\' or '#' or '?')
        && ch is not (>= '\u0000' and <= '\u001f')
        && ch is not (>= '\u007f' and <= '\u009f');
}
