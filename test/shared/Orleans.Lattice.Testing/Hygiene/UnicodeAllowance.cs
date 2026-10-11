using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Testing.Hygiene;

/// <summary>
/// A reviewed exception for a bounded number of identical Unicode lines in one
/// tracked file. The digest covers the entire line, excluding its line ending.
/// </summary>
public sealed record UnicodeAllowance
{
    /// <summary>Creates a content-specific exception with a substantive justification.</summary>
    public UnicodeAllowance(string path, string digest, int count, string reason)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(digest);
        ArgumentNullException.ThrowIfNull(reason);
        if (path.Length == 0 || path.Any(c => c < 32 || c > 127) || path.StartsWith('/') || path.Contains('\\')
            || path.Contains(':') || path.Contains('*') || path.Contains('?')
            || path.Split('/').Any(p => p is "" or "." or ".."))
            throw new ArgumentException("Use an exact repository-relative path with '/' separators.", nameof(path));
        if (digest.Length != 64 || digest.Any(c => !char.IsAsciiHexDigit(c)))
            throw new ArgumentException("Use the SHA-256 digest of the complete UTF-8 line.", nameof(digest));
        if (count <= 0)
            throw new ArgumentOutOfRangeException(nameof(count), "An allowance must have a positive occurrence limit.");
        if (reason.Trim().Length < 40 || reason.Contains('\t') || reason.Contains('\n') || reason.Contains('\r')
            || reason.Any(c => c > 127))
            throw new ArgumentException("Supply a single-line ASCII justification of at least 40 characters.", nameof(reason));

        Path = path;
        Digest = digest.ToUpperInvariant();
        Count = count;
        Reason = reason;
    }

    /// <summary>The exact repository-relative path; no wildcards or directory exceptions.</summary>
    public string Path { get; }

    /// <summary>The SHA-256 digest of the complete line, encoded as UTF-8 without a BOM.</summary>
    public string Digest { get; }

    /// <summary>The maximum number of occurrences of that exact line in the file.</summary>
    public int Count { get; }

    /// <summary>The reason the literal Unicode must be preserved.</summary>
    public string Reason { get; }

    /// <summary>Computes an exception key without depending on CRLF versus LF line endings.</summary>
    public static string DigestLine(string line)
    {
        ArgumentNullException.ThrowIfNull(line);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(line)));
    }

    /// <summary>
    /// Reads the ASCII TSV manifest (path, line digest, occurrence count, reason).
    /// Blank lines and comments beginning with '#' are ignored; malformed rows fail.
    /// </summary>
    public static IReadOnlyList<UnicodeAllowance> Load(string path)
    {
        ArgumentNullException.ThrowIfNull(path);
        var entries = new List<UnicodeAllowance>();
        var keys = new HashSet<(string, string)>();
        var lineNumber = 0;
        foreach (var line in File.ReadLines(path))
        {
            lineNumber++;
            if (line.Length == 0 || line.StartsWith('#')) continue;
            var fields = line.Split('\t');
            if (fields.Length != 4 || !int.TryParse(fields[2], NumberStyles.None, CultureInfo.InvariantCulture, out var count))
                throw new FormatException($"{path}:{lineNumber}: expected path, SHA-256, positive count, and reason.");
            var entry = new UnicodeAllowance(fields[0], fields[1], count, fields[3]);
            if (!keys.Add((entry.Path, entry.Digest)))
                throw new FormatException($"{path}:{lineNumber}: duplicate Unicode allowance.");
            entries.Add(entry);
        }
        return entries;
    }
}
