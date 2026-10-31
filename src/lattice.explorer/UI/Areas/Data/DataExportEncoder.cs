using System.Buffers;
using System.Buffers.Text;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Encodes a data export, one record at a time, in the <c>lattice-export/1</c> format
/// (issue #5366). It holds one reusable buffer sized to the largest record so far, so
/// encoding a file of any size holds one record, never the file.
/// </summary>
/// <remarks>
/// <para>
/// <b>Values.</b> A value is written as JSON, verbatim, when its bytes are exactly one
/// JSON value: valid UTF-8 under RFC 8259, with no byte-order mark, no leading or trailing
/// whitespace, and no carriage return or line feed anywhere. Every other value - binary,
/// text that is not JSON, JSON with surrounding whitespace or on several lines - is written
/// as standard, padded base64 and marked <c>base64</c>. Either way the original bytes are
/// recoverable exactly: the raw JSON text of the value, or the base64-decoded bytes.
/// </para>
/// <para>
/// <b>NDJSON</b> (<c>application/x-ndjson</c>): UTF-8, no byte-order mark, one object per
/// line, each line ending in a line feed: <c>{"key":"k","value":&lt;json&gt;}</c> or
/// <c>{"key":"k","value":"&lt;base64&gt;","encoding":"base64"}</c>. The last line is
/// <c>{"trailer":{...}}</c>, an object with no <c>key</c>.
/// </para>
/// <para>
/// <b>CSV</b> (<c>text/csv</c>, RFC 4180): UTF-8, no byte-order mark, CRLF line ends, the
/// header <c>key,value,encoding</c>, then one row per entry with the key always quoted, a
/// JSON value quoted with <c>json</c> in the encoding column, or a base64 value with
/// <c>base64</c>. The last row has an empty key, the trailer object as its quoted value,
/// and <c>trailer</c> as its encoding - a value no entry row ever carries.
/// </para>
/// </remarks>
internal sealed class DataExportEncoder : IDisposable
{
    /// <summary>The format id every export's trailer carries.</summary>
    public const string FormatId = "lattice-export/1";

    /// <summary>The NDJSON trailer line's one property.</summary>
    public const string TrailerProperty = "trailer";

    /// <summary>The key property of an NDJSON entry.</summary>
    public const string KeyProperty = "key";

    /// <summary>The value property of an NDJSON entry.</summary>
    public const string ValueProperty = "value";

    /// <summary>The encoding property of a base64 NDJSON entry, and the CSV encoding column's header.</summary>
    public const string EncodingProperty = "encoding";

    /// <summary>The encoding marker of a base64 value.</summary>
    public const string Base64Encoding = "base64";

    /// <summary>The CSV encoding marker of a JSON value.</summary>
    public const string JsonEncoding = "json";

    /// <summary>The CSV encoding marker of the trailer row.</summary>
    public const string TrailerEncoding = "trailer";

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        // The file is data for a person or a program, never HTML, so text is written
        // as itself rather than escaped for embedding in a page.
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static readonly JsonReaderOptions ReaderOptions = new()
    {
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
    };

    private static readonly byte[] CsvHeader = "key,value,encoding\r\n"u8.ToArray();

    private static readonly byte[] Utf8ByteOrderMark = [0xEF, 0xBB, 0xBF];

    private readonly DataExportFormat _format;
    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private readonly Utf8JsonWriter _json;

    /// <summary>Creates an encoder for <paramref name="format"/>.</summary>
    /// <param name="format">The file format.</param>
    public DataExportEncoder(DataExportFormat format)
    {
        _format = format;
        _json = new Utf8JsonWriter(_buffer, WriterOptions);
    }

    /// <summary>The bytes the file starts with: the CSV header row, or nothing for NDJSON.</summary>
    public ReadOnlyMemory<byte> Header() =>
        _format == DataExportFormat.Csv ? CsvHeader : ReadOnlyMemory<byte>.Empty;

    /// <summary>
    /// Encodes one entry. The returned bytes are valid until the next call on this encoder.
    /// </summary>
    /// <param name="key">The entry key.</param>
    /// <param name="value">The entry's stored value bytes.</param>
    /// <returns>The encoded record, line end included.</returns>
    /// <exception cref="DataExportKeyException">The key is not valid Unicode text, so no UTF-8 file can hold it.</exception>
    public ReadOnlyMemory<byte> Entry(string key, ReadOnlySpan<byte> value)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (HasUnpairedSurrogate(key))
        {
            throw new DataExportKeyException();
        }

        _buffer.ResetWrittenCount();
        var json = IsEmbeddableJson(value);
        if (_format == DataExportFormat.Csv)
        {
            WriteCsvKey(key);
            _buffer.Write(","u8);
            if (json)
            {
                WriteQuoted(value);
                _buffer.Write(",json\r\n"u8);
            }
            else
            {
                WriteBase64(value);
                _buffer.Write(",base64\r\n"u8);
            }
        }
        else
        {
            _json.Reset(_buffer);
            _json.WriteStartObject();
            _json.WriteString(KeyProperty, key);
            if (json)
            {
                _json.WritePropertyName(ValueProperty);
                _json.WriteRawValue(value, skipInputValidation: true);
            }
            else
            {
                _json.WriteBase64String(ValueProperty, value);
                _json.WriteString(EncodingProperty, Base64Encoding);
            }

            _json.WriteEndObject();
            _json.Flush();
            _buffer.Write("\n"u8);
        }

        return _buffer.WrittenMemory;
    }

    /// <summary>Encodes the trailer record. The returned bytes are valid until the next call on this encoder.</summary>
    /// <param name="trailer">The trailer.</param>
    /// <returns>The encoded trailer, line end included.</returns>
    public ReadOnlyMemory<byte> Trailer(ExplorerDownloadTrailer trailer)
    {
        ArgumentNullException.ThrowIfNull(trailer);
        _buffer.ResetWrittenCount();
        if (_format == DataExportFormat.Csv)
        {
            // The trailer object first, then quoted into the row.
            var trailerJson = new ArrayBufferWriter<byte>(256);
            using (var writer = new Utf8JsonWriter(trailerJson, WriterOptions))
            {
                trailer.WriteJson(writer);
            }

            _buffer.Write(","u8);
            WriteQuoted(trailerJson.WrittenSpan);
            _buffer.Write(",trailer\r\n"u8);
        }
        else
        {
            _json.Reset(_buffer);
            _json.WriteStartObject();
            _json.WritePropertyName(TrailerProperty);
            trailer.WriteJson(_json);
            _json.WriteEndObject();
            _json.Flush();
            _buffer.Write("\n"u8);
        }

        return _buffer.WrittenMemory;
    }

    /// <summary>
    /// Whether <paramref name="value"/> is written as JSON: exactly one RFC 8259 value in
    /// valid UTF-8, with no byte-order mark, no surrounding whitespace and no line break.
    /// </summary>
    /// <param name="value">The value bytes.</param>
    public static bool IsEmbeddableJson(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty
            || IsJsonWhitespace(value[0])
            || IsJsonWhitespace(value[^1])
            || value.StartsWith(Utf8ByteOrderMark)
            || value.IndexOfAny((byte)'\r', (byte)'\n') >= 0)
        {
            return false;
        }

        try
        {
            var reader = new Utf8JsonReader(value, ReaderOptions);
            if (!reader.Read())
            {
                return false;
            }

            reader.Skip();

            // A second value, or anything else after the first, is not one JSON value.
            return !reader.Read();
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <inheritdoc />
    public void Dispose() => _json.Dispose();

    private static bool IsJsonWhitespace(byte b) => b is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n';

    private static bool HasUnpairedSurrogate(string text)
    {
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 >= text.Length || !char.IsLowSurrogate(text[i + 1]))
                {
                    return true;
                }

                i++;
            }
            else if (char.IsLowSurrogate(c))
            {
                return true;
            }
        }

        return false;
    }

    private void WriteCsvKey(string key)
    {
        _buffer.Write("\""u8);
        var start = 0;
        while (start <= key.Length)
        {
            var quote = key.IndexOf('"', start);
            var end = quote < 0 ? key.Length : quote;
            var segment = key.AsSpan(start, end - start);
            var span = _buffer.GetSpan(Encoding.UTF8.GetMaxByteCount(segment.Length));
            _buffer.Advance(Encoding.UTF8.GetBytes(segment, span));
            if (quote < 0)
            {
                break;
            }

            _buffer.Write("\"\""u8);
            start = quote + 1;
        }

        _buffer.Write("\""u8);
    }

    private void WriteQuoted(ReadOnlySpan<byte> text)
    {
        _buffer.Write("\""u8);
        while (true)
        {
            var quote = text.IndexOf((byte)'"');
            if (quote < 0)
            {
                _buffer.Write(text);
                break;
            }

            _buffer.Write(text[..quote]);
            _buffer.Write("\"\""u8);
            text = text[(quote + 1)..];
        }

        _buffer.Write("\""u8);
    }

    private void WriteBase64(ReadOnlySpan<byte> value)
    {
        var span = _buffer.GetSpan(Base64.GetMaxEncodedToUtf8Length(value.Length));
        Base64.EncodeToUtf8(value, span, out _, out var written);
        _buffer.Advance(written);
    }
}
