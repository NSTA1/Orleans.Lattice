using System.Globalization;
using System.IO.Hashing;
using System.Text;

namespace Orleans.Lattice.Auth;

/// <summary>
/// Bounds the caller-supplied text an admin audit record persists. The target id and
/// the detail values are built from the request's raw arguments before authorization
/// and validation, so an unauthorized caller could otherwise make every denial
/// durably write (and replicate) an arbitrarily large string. A value longer than
/// <see cref="MaxLength"/> is stored as a prefix plus a marker carrying its full length
/// and an xxHash64 of the whole value, so records stay comparable without storing
/// the caller's text.
/// </summary>
internal static class AdminAuditBounds
{
    /// <summary>The longest caller-derived string stored verbatim.</summary>
    internal const int MaxLength = 256;

    /// <summary>The most detail entries a record keeps.</summary>
    internal const int MaxDetailEntries = 8;

    /// <summary>How many leading characters of an oversized value are kept.</summary>
    internal const int KeptPrefix = 64;

    /// <summary>The marker an oversized value carries after its kept prefix.</summary>
    internal const string TruncationMarker = "...[truncated ";

    /// <summary>Returns <paramref name="value"/>, or its bounded form when it is longer than <see cref="MaxLength"/>.</summary>
    /// <param name="value">The value, or <see langword="null"/>.</param>
    internal static string? Bound(string? value)
    {
        if (value is null || value.Length <= MaxLength)
        {
            return value;
        }

        var hash = XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(value));
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{value.AsSpan(0, KeptPrefix)}{TruncationMarker}len={value.Length} xxh64={hash:x16}]");
    }

    /// <summary>Bounds every key and value of <paramref name="detail"/>, keeping at most <see cref="MaxDetailEntries"/> entries.</summary>
    /// <param name="detail">The detail, or <see langword="null"/>.</param>
    internal static IReadOnlyDictionary<string, string>? Bound(IReadOnlyDictionary<string, string>? detail)
    {
        if (detail is null)
        {
            return null;
        }

        var bounded = new Dictionary<string, string>(Math.Min(detail.Count, MaxDetailEntries), StringComparer.Ordinal);
        foreach (var (key, value) in detail)
        {
            if (bounded.Count == MaxDetailEntries)
            {
                break;
            }

            bounded[Bound(key)!] = Bound(value)!;
        }

        return bounded;
    }
}
