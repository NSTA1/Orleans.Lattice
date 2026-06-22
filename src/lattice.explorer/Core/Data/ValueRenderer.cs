using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>How a value's bytes were interpreted for display.</summary>
public enum ValueFormat
{
    /// <summary>The value had no bytes.</summary>
    Empty,

    /// <summary>The value parsed as JSON and is shown pretty-printed.</summary>
    Json,

    /// <summary>The value decoded as printable UTF-8 text.</summary>
    Text,

    /// <summary>The value is opaque bytes, shown as a hex dump.</summary>
    Hex,
}

/// <summary>A display-ready rendering of a value's bytes.</summary>
public sealed record RenderedValue
{
    /// <summary>How the bytes were interpreted.</summary>
    public required ValueFormat Format { get; init; }

    /// <summary>The text to display (pretty JSON, decoded text, or a hex dump).</summary>
    public required string Content { get; init; }

    /// <summary>An optional note, e.g. that only a truncated preview was rendered.</summary>
    public string? Note { get; init; }
}

/// <summary>
/// Pure value-rendering logic for the Data tab: detects JSON and pretty-prints
/// it, otherwise falls back to printable UTF-8 text, otherwise a hex dump.
/// </summary>
public static class ValueRenderer
{
    /// <summary>The largest value, in bytes, that is auto-parsed as JSON.</summary>
    public const int MaxJsonAutoFormatBytes = 512 * 1024;

    private const string TruncatedNote = "Preview only - the full value is larger than the fetched bytes.";

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private static readonly Encoding StrictUtf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Renders a value's bytes for display.</summary>
    /// <param name="bytes">The value bytes (a preview when <paramref name="truncated"/> is set).</param>
    /// <param name="truncated">Whether <paramref name="bytes"/> is a truncated preview.</param>
    public static RenderedValue Render(byte[] bytes, bool truncated = false)
    {
        ArgumentNullException.ThrowIfNull(bytes);

        var note = truncated ? TruncatedNote : null;

        if (bytes.Length == 0)
        {
            return new RenderedValue { Format = ValueFormat.Empty, Content = string.Empty, Note = note };
        }

        if (!truncated && bytes.Length <= MaxJsonAutoFormatBytes && TryFormatJson(bytes, out var json))
        {
            return new RenderedValue { Format = ValueFormat.Json, Content = json, Note = note };
        }

        if (TryDecodeText(bytes, out var text))
        {
            return new RenderedValue { Format = ValueFormat.Text, Content = text, Note = note };
        }

        return new RenderedValue { Format = ValueFormat.Hex, Content = HexDump(bytes), Note = note };
    }

    private static bool TryFormatJson(byte[] bytes, out string formatted)
    {
        try
        {
            using var document = JsonDocument.Parse(bytes, new JsonDocumentOptions
            {
                CommentHandling = JsonCommentHandling.Skip,
                AllowTrailingCommas = true,
            });
            formatted = JsonSerializer.Serialize(document.RootElement, JsonOptions);
            return true;
        }
        catch (JsonException)
        {
            formatted = string.Empty;
            return false;
        }
    }

    private static bool TryDecodeText(byte[] bytes, out string text)
    {
        try
        {
            text = StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            text = string.Empty;
            return false;
        }

        foreach (var ch in text)
        {
            if (char.IsControl(ch) && ch is not '\t' and not '\n' and not '\r')
            {
                text = string.Empty;
                return false;
            }
        }

        return true;
    }

    /// <summary>Renders bytes as an offset/hex/ASCII dump, 16 bytes per row.</summary>
    public static string HexDump(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);

        var builder = new StringBuilder();
        for (var offset = 0; offset < bytes.Length; offset += 16)
        {
            builder.Append(offset.ToString("x8", CultureInfo.InvariantCulture)).Append("  ");

            var rowLength = Math.Min(16, bytes.Length - offset);
            for (var i = 0; i < 16; i++)
            {
                if (i < rowLength)
                {
                    builder.Append(bytes[offset + i].ToString("x2", CultureInfo.InvariantCulture)).Append(' ');
                }
                else
                {
                    builder.Append("   ");
                }

                if (i == 7)
                {
                    builder.Append(' ');
                }
            }

            builder.Append(' ');
            for (var i = 0; i < rowLength; i++)
            {
                var b = bytes[offset + i];
                builder.Append(b is >= 0x20 and < 0x7f ? (char)b : '.');
            }

            builder.Append('\n');
        }

        return builder.ToString();
    }
}
