using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The value editor's draft codec: how stored bytes become editable text and how a
/// draft becomes bytes again, in one of three formats. JSON is validated and can be
/// pretty-printed; text is stored as UTF-8 exactly as typed; a value that is not
/// valid UTF-8 is binary, shown as base64 and replaced whole.
/// </summary>
internal static class DataDraft
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static readonly JsonWriterOptions PrettyOptions = new()
    {
        Indented = true,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <summary>The format a stored value opens in: JSON when it parses as JSON, text when it is UTF-8, else binary.</summary>
    /// <param name="value">The stored bytes.</param>
    public static DataDraftFormat Detect(ReadOnlySpan<byte> value)
    {
        if (!TryDecode(value, out var text))
        {
            return DataDraftFormat.Base64;
        }

        return text.Length > 0 && IsJson(text, out _) ? DataDraftFormat.Json : DataDraftFormat.Text;
    }

    /// <summary>The editable text of <paramref name="value"/> in <paramref name="format"/>.</summary>
    /// <param name="value">The stored bytes.</param>
    /// <param name="format">The format to show it in.</param>
    /// <returns>The text, or <see langword="null"/> when the bytes cannot be shown in that format.</returns>
    public static string? ToText(ReadOnlySpan<byte> value, DataDraftFormat format)
    {
        if (format == DataDraftFormat.Base64)
        {
            return Convert.ToBase64String(value);
        }

        if (!TryDecode(value, out var text))
        {
            return null;
        }

        return format == DataDraftFormat.Json && IsJson(text, out _) ? Pretty(text) : text;
    }

    /// <summary>Encodes a draft, or explains why it cannot be stored as written.</summary>
    /// <param name="text">The draft text.</param>
    /// <param name="format">The draft's format.</param>
    /// <param name="value">The bytes to store, when the draft is valid.</param>
    /// <param name="error">Why the draft is invalid, or <see langword="null"/>.</param>
    /// <returns>Whether the draft is valid.</returns>
    public static bool TryEncode(string? text, DataDraftFormat format, out byte[] value, out string? error)
    {
        text ??= string.Empty;
        switch (format)
        {
            case DataDraftFormat.Json:
                if (!IsJson(text, out error))
                {
                    value = [];
                    return false;
                }

                value = Encoding.UTF8.GetBytes(text);
                return true;
            case DataDraftFormat.Base64:
                try
                {
                    value = Convert.FromBase64String(text.Trim());
                    error = null;
                    return true;
                }
                catch (FormatException)
                {
                    value = [];
                    error = "This is not valid base64. A binary value is replaced whole: paste its base64 encoding.";
                    return false;
                }

            default:
                value = Encoding.UTF8.GetBytes(text);
                error = null;
                return true;
        }
    }

    /// <summary>Whether <paramref name="text"/> is one JSON document, with the reason when it is not.</summary>
    /// <param name="text">The text.</param>
    /// <param name="error">Where and why it fails to parse, or <see langword="null"/>.</param>
    public static bool IsJson(string text, out string? error)
    {
        ArgumentNullException.ThrowIfNull(text);
        try
        {
            using var document = JsonDocument.Parse(text);
            error = null;
            return true;
        }
        catch (JsonException exception)
        {
            error = exception.LineNumber is { } line && exception.BytePositionInLine is { } column
                ? $"This is not valid JSON: line {line + 1}, position {column + 1}."
                : "This is not valid JSON.";
            return false;
        }
    }

    /// <summary>Re-indents a JSON document; returns the text unchanged when it is not JSON.</summary>
    /// <param name="text">The JSON text.</param>
    public static string Pretty(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        try
        {
            using var document = JsonDocument.Parse(text);
            using var buffer = new MemoryStream();
            using (var writer = new Utf8JsonWriter(buffer, PrettyOptions))
            {
                document.WriteTo(writer);
            }

            return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
        }
        catch (JsonException)
        {
            return text;
        }
    }

    /// <summary>The label a format reads as.</summary>
    /// <param name="format">The format.</param>
    public static string Label(DataDraftFormat format) => format switch
    {
        DataDraftFormat.Json => "JSON",
        DataDraftFormat.Text => "Text (UTF-8)",
        _ => "Binary (base64)",
    };

    private static bool TryDecode(ReadOnlySpan<byte> value, out string text)
    {
        try
        {
            text = StrictUtf8.GetString(value);
            return true;
        }
        catch (DecoderFallbackException)
        {
            text = string.Empty;
            return false;
        }
    }
}
