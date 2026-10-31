using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Turns an import's file into a stream of records, one at a time, in any of the import
/// formats (issue #5365). It pulls the file through a <see cref="DataImportLineReader"/>,
/// parses each line (NDJSON with one <see cref="JsonDocument"/> per line, CSV with
/// <see cref="DataImportCsvReader"/>) and yields an entry or a refusal before reading the
/// next, so it holds one line whatever the size of the file.
/// </summary>
/// <remarks>
/// A record's <see cref="DataImportRecord.Error"/> is a sentence with no line number; the
/// caller shows it with <see cref="DataImportRecord.Line"/>. A file that cannot be read any
/// further raises <see cref="DataImportFileException"/>.
/// </remarks>
internal sealed class DataImportReader
{
    /// <summary>The longest line, or CSV record, an import reads: 4 MB.</summary>
    public const int DefaultMaxLineBytes = 4 * 1024 * 1024;

    private static readonly JsonDocumentOptions DocumentOptions = new()
    {
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
        MaxDepth = 64,
    };

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly DataImportSettings _settings;
    private readonly int _maxLineBytes;
    private readonly DataImportKeyTemplate? _template;
    private readonly DataImportJsonPointer[] _keyPointers = [];
    private readonly DataImportJsonPointer? _valuePointer;
    private readonly Func<int, (string?, string?)> _lookup;
    private JsonElement _root;
    private IReadOnlyList<string> _row = [];
    private int[] _keyColumns = [];
    private ArrayBufferWriter<byte>? _rowJson;
    private Utf8JsonWriter? _rowWriter;
    private long _linesRead;

    /// <summary>Creates a reader for <paramref name="settings"/>, which must be valid.</summary>
    /// <param name="settings">What to read.</param>
    /// <param name="maxLineBytes">The longest line or CSV record to read.</param>
    /// <exception cref="ArgumentException">The settings are not valid.</exception>
    public DataImportReader(DataImportSettings settings, int maxLineBytes = DefaultMaxLineBytes)
    {
        ArgumentNullException.ThrowIfNull(settings);
        if (settings.Validate() is { } problem)
        {
            throw new ArgumentException(problem, nameof(settings));
        }

        _settings = settings;
        _maxLineBytes = maxLineBytes;
        if (settings.UsesKeyTemplate)
        {
            var pointers = settings.Format == DataImportFormat.Ndjson;
            DataImportKeyTemplate.TryParse(settings.KeyTemplate, pointers, out _template, out _);
            if (pointers)
            {
                _keyPointers = _template!.References.Select(Pointer).ToArray();
                if (settings.ValueSource == DataImportValueSource.Field)
                {
                    _valuePointer = Pointer(settings.ValueField!);
                }
            }
        }

        _lookup = Lookup;
    }

    /// <summary>How many lines have been read. Safe to read from any thread.</summary>
    public long LinesRead => Volatile.Read(ref _linesRead);

    /// <summary>For a <c>lattice-export/1</c> file, whether its trailer line has been read.</summary>
    public bool TrailerSeen { get; private set; }

    /// <summary>For a <c>lattice-export/1</c> file, whether its trailer said the export was complete.</summary>
    public bool TrailerComplete { get; private set; }

    /// <summary>Reads <paramref name="stream"/> to its end, yielding each record.</summary>
    /// <param name="stream">The file.</param>
    /// <param name="cancellationToken">Stops the read.</param>
    /// <returns>The records, in file order.</returns>
    /// <exception cref="DataImportFileException">The file cannot be read any further.</exception>
    public async IAsyncEnumerable<DataImportRecord> ReadAsync(Stream stream, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        TrailerSeen = false;
        TrailerComplete = false;
        using var lines = new DataImportLineReader(stream, _maxLineBytes);
        if (_settings.Format == DataImportFormat.Csv)
        {
            await foreach (var record in ReadCsvAsync(lines, cancellationToken).ConfigureAwait(false))
            {
                yield return record;
            }

            yield break;
        }

        while (await lines.ReadLineAsync(cancellationToken).ConfigureAwait(false))
        {
            Volatile.Write(ref _linesRead, lines.LineNumber);
            var line = lines.Current;
            if (IsBlank(line.Span))
            {
                continue;
            }

            if (Read(line, lines.LineNumber) is { } record)
            {
                yield return record;
            }
        }
    }

    /// <summary>Whether <paramref name="value"/> is a JSON number literal, as CSV type inference writes one.</summary>
    /// <param name="value">The field's text.</param>
    internal static bool IsJsonNumber(ReadOnlySpan<char> value)
    {
        var i = 0;
        if (i < value.Length && value[i] == '-')
        {
            i++;
        }

        if (i >= value.Length)
        {
            return false;
        }

        if (value[i] == '0')
        {
            i++;
        }
        else if (value[i] is >= '1' and <= '9')
        {
            while (i < value.Length && char.IsAsciiDigit(value[i]))
            {
                i++;
            }
        }
        else
        {
            return false;
        }

        if (i < value.Length && value[i] == '.')
        {
            i++;
            var start = i;
            while (i < value.Length && char.IsAsciiDigit(value[i]))
            {
                i++;
            }

            if (i == start)
            {
                return false;
            }
        }

        if (i < value.Length && value[i] is 'e' or 'E')
        {
            i++;
            if (i < value.Length && value[i] is '+' or '-')
            {
                i++;
            }

            var start = i;
            while (i < value.Length && char.IsAsciiDigit(value[i]))
            {
                i++;
            }

            if (i == start)
            {
                return false;
            }
        }

        return i == value.Length;
    }

    private static DataImportJsonPointer Pointer(string text)
    {
        DataImportJsonPointer.TryParse(text, out var pointer, out _);
        return pointer!;
    }

    private static bool IsBlank(ReadOnlySpan<byte> line) => line.IndexOfAnyExcept(" \t\r"u8) < 0;

    private DataImportRecord? Read(ReadOnlyMemory<byte> line, long number) => _settings.Format switch
    {
        DataImportFormat.KeyValue => ReadKeyValue(line.Span, number),
        DataImportFormat.LatticeExport => ReadExport(line, number),
        _ => ReadNdjson(line, number),
    };

    private static DataImportRecord ReadKeyValue(ReadOnlySpan<byte> line, long number)
    {
        var split = line.IndexOf((byte)'=');
        if (split <= 0)
        {
            return new(number, null, "The line is not key=value.");
        }

        string key;
        try
        {
            key = StrictUtf8.GetString(line[..split]);
        }
        catch (DecoderFallbackException)
        {
            return new(number, null, "The key is not valid UTF-8 text.");
        }

        return new(number, new DataEntry { Key = key, Value = line[(split + 1)..].ToArray() }, null);
    }

    private DataImportRecord ReadNdjson(ReadOnlyMemory<byte> line, long number)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(line, DocumentOptions);
        }
        catch (JsonException exception)
        {
            return new(number, null, NotJson(exception));
        }

        using (document)
        {
            _root = document.RootElement;
            if (!_template!.TryBuild(_lookup, out var key, out var keyError))
            {
                return new(number, null, keyError);
            }

            if (string.IsNullOrEmpty(key))
            {
                return new(number, null, "The key is empty.");
            }

            byte[] value;
            if (_valuePointer is null)
            {
                value = line.ToArray();
            }
            else if (_valuePointer.TryResolve(_root, out var element))
            {
                value = JsonMarshal.GetRawUtf8Value(element).ToArray();
            }
            else
            {
                return new(number, null, $"There is no value at {_valuePointer.Text}.");
            }

            return new(number, new DataEntry { Key = key, Value = value }, null);
        }
    }

    private DataImportRecord? ReadExport(ReadOnlyMemory<byte> line, long number)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(line, DocumentOptions);
        }
        catch (JsonException exception)
        {
            return new(number, null, NotJson(exception));
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return new(number, null, "The line is not a lattice-export/1 entry: it is not a JSON object.");
            }

            if (TrailerSeen)
            {
                return new(number, null, "The line follows the export's trailer, which ends the file.");
            }

            if (root.TryGetProperty(DataExportEncoder.TrailerProperty, out var trailer))
            {
                TrailerSeen = true;
                TrailerComplete = trailer.ValueKind == JsonValueKind.Object
                    && trailer.TryGetProperty("complete", out var complete)
                    && complete.ValueKind == JsonValueKind.True;
                return null;
            }

            if (!root.TryGetProperty(DataExportEncoder.KeyProperty, out var keyElement) || keyElement.ValueKind != JsonValueKind.String)
            {
                return new(number, null, "The line has no \"key\" string.");
            }

            var key = keyElement.GetString();
            if (string.IsNullOrEmpty(key))
            {
                return new(number, null, "The key is empty.");
            }

            if (!root.TryGetProperty(DataExportEncoder.ValueProperty, out var valueElement))
            {
                return new(number, null, "The line has no \"value\".");
            }

            byte[] value;
            if (root.TryGetProperty(DataExportEncoder.EncodingProperty, out var encoding))
            {
                if (encoding.ValueKind != JsonValueKind.String || encoding.GetString() != DataExportEncoder.Base64Encoding)
                {
                    return new(number, null, "The line's \"encoding\" is not \"base64\", the only encoding lattice-export/1 writes.");
                }

                if (valueElement.ValueKind != JsonValueKind.String || !valueElement.TryGetBytesFromBase64(out var decoded))
                {
                    return new(number, null, "The line's base64 value is not valid base64.");
                }

                value = decoded;
            }
            else
            {
                value = JsonMarshal.GetRawUtf8Value(valueElement).ToArray();
            }

            return new(number, new DataEntry { Key = key, Value = value }, null);
        }
    }

    private async IAsyncEnumerable<DataImportRecord> ReadCsvAsync(DataImportLineReader lines, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var csv = new DataImportCsvReader(lines, _settings.Delimiter);
        if (!await csv.ReadRecordAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new DataImportFileException(1, "The file is empty: a CSV import needs a header row naming its columns.");
        }

        Volatile.Write(ref _linesRead, lines.LineNumber);
        var header = ReadHeader(csv);
        var columns = new Dictionary<string, int>(header.Count, StringComparer.Ordinal);
        for (var i = 0; i < header.Count; i++)
        {
            columns[header[i]] = i;
        }

        _keyColumns = _template!.References.Select(name => Column(columns, name, "key template")).ToArray();
        var valueColumn = _settings.ValueSource == DataImportValueSource.Field
            ? Column(columns, _settings.ValueField!, "value")
            : -1;

        while (await csv.ReadRecordAsync(cancellationToken).ConfigureAwait(false))
        {
            Volatile.Write(ref _linesRead, lines.LineNumber);
            var number = csv.RecordLine;
            if (csv.Error is { } error)
            {
                yield return new(number, null, error);
                continue;
            }

            var fields = csv.Fields;
            if (fields.Count != header.Count)
            {
                yield return new(number, null, FormattableString.Invariant(
                    $"The row has {fields.Count:N0} {(fields.Count == 1 ? "field" : "fields")}; the header has {header.Count:N0}."));
                continue;
            }

            _row = fields;
            if (!_template.TryBuild(_lookup, out var key, out var keyError))
            {
                yield return new(number, null, keyError);
                continue;
            }

            if (string.IsNullOrEmpty(key))
            {
                yield return new(number, null, "The key is empty.");
                continue;
            }

            var value = valueColumn >= 0
                ? Encoding.UTF8.GetBytes(fields[valueColumn])
                : RowObject(header, fields);
            yield return new(number, new DataEntry { Key = key, Value = value }, null);
        }
    }

    private static IReadOnlyList<string> ReadHeader(DataImportCsvReader csv)
    {
        if (csv.Error is { } error)
        {
            throw new DataImportFileException(csv.RecordLine, "The header row cannot be read: " + error);
        }

        var header = csv.Fields.ToArray();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in header)
        {
            if (name.Length == 0)
            {
                throw new DataImportFileException(csv.RecordLine, "The header row has an empty column name: name every column.");
            }

            if (!seen.Add(name))
            {
                throw new DataImportFileException(csv.RecordLine, $"The header row names the column \"{name}\" twice.");
            }
        }

        return header;
    }

    private static int Column(Dictionary<string, int> columns, string name, string use) =>
        columns.TryGetValue(name, out var index)
            ? index
            : throw new DataImportFileException(1, $"The {use} names the column \"{name}\", which the header row does not have.");

    private byte[] RowObject(IReadOnlyList<string> header, IReadOnlyList<string> fields)
    {
        _rowJson ??= new ArrayBufferWriter<byte>(1024);
        _rowJson.ResetWrittenCount();
        if (_rowWriter is null)
        {
            _rowWriter = new Utf8JsonWriter(_rowJson, WriterOptions);
        }
        else
        {
            _rowWriter.Reset(_rowJson);
        }

        _rowWriter.WriteStartObject();
        for (var i = 0; i < header.Count; i++)
        {
            var field = fields[i];
            _rowWriter.WritePropertyName(header[i]);
            if (_settings.InferTypes && field is "true" or "false")
            {
                _rowWriter.WriteBooleanValue(field == "true");
            }
            else if (_settings.InferTypes && IsJsonNumber(field))
            {
                _rowWriter.WriteRawValue(field, skipInputValidation: true);
            }
            else
            {
                _rowWriter.WriteStringValue(field);
            }
        }

        _rowWriter.WriteEndObject();
        _rowWriter.Flush();
        return _rowJson.WrittenSpan.ToArray();
    }

    private (string?, string?) Lookup(int reference)
    {
        if (_settings.Format == DataImportFormat.Csv)
        {
            return (_row[_keyColumns[reference]], null);
        }

        var pointer = _keyPointers[reference];
        if (!pointer.TryResolve(_root, out var element))
        {
            return (null, $"There is no value at {pointer.Text} for the key.");
        }

        return DataImportJsonPointer.ScalarText(element) is { } text
            ? (text, null)
            : (null, $"The value at {pointer.Text} is {Kind(element.ValueKind)}, which cannot be a key: a key comes from text, a number or a boolean.");
    }

    private static string Kind(JsonValueKind kind) => kind switch
    {
        JsonValueKind.Object => "an object",
        JsonValueKind.Array => "an array",
        _ => "null",
    };

    private static string NotJson(JsonException exception) =>
        exception.BytePositionInLine is { } position
            ? FormattableString.Invariant($"The line is not one JSON value: the problem is at byte {position + 1:N0}.")
            : "The line is not one JSON value.";
}
