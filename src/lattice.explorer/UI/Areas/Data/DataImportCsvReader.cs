using System.Buffers;
using System.Text;
using System.Text.Unicode;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Reads RFC 4180 CSV records one at a time over a <see cref="DataImportLineReader"/>
/// (issue #5365): fields separated by a delimiter, a field in double quotes that may hold
/// the delimiter, a line break, or a doubled quote, and blank lines skipped. A record is
/// bounded by the line reader's bound, so the reader holds one record whatever the size of
/// the file.
/// </summary>
internal sealed class DataImportCsvReader
{
    /// <summary>The most fields a record may have.</summary>
    internal const int MaxFields = 10_000;

    private readonly DataImportLineReader _lines;
    private readonly char _delimiter;
    private readonly List<string> _fields = [];
    private readonly StringBuilder _field = new();
    private char[] _chars = [];

    /// <summary>Creates a reader.</summary>
    /// <param name="lines">The file's lines.</param>
    /// <param name="delimiter">The field delimiter.</param>
    public DataImportCsvReader(DataImportLineReader lines, char delimiter)
    {
        ArgumentNullException.ThrowIfNull(lines);
        _lines = lines;
        _delimiter = delimiter;
    }

    /// <summary>The 1-based line the current record starts on.</summary>
    public long RecordLine { get; private set; }

    /// <summary>The current record's fields.</summary>
    public IReadOnlyList<string> Fields => _fields;

    /// <summary>Why the current record is malformed, or <see langword="null"/>.</summary>
    public string? Error { get; private set; }

    /// <summary>Reads the next record, skipping blank lines.</summary>
    /// <param name="cancellationToken">Stops the read.</param>
    /// <returns><see langword="false"/> at the end of the file.</returns>
    /// <exception cref="DataImportFileException">A record is longer than the line reader's bound.</exception>
    public async ValueTask<bool> ReadRecordAsync(CancellationToken cancellationToken)
    {
        _fields.Clear();
        _field.Clear();
        Error = null;
        var started = false;
        var inQuotes = false;
        var afterQuote = false;
        long length = 0;

        while (true)
        {
            if (!await _lines.ReadLineAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!started)
                {
                    return false;
                }

                Error ??= "A quoted field is never closed.";
                EndField(ref afterQuote);
                return true;
            }

            var bytes = _lines.Current.Span;
            if (!started)
            {
                if (bytes.IsEmpty)
                {
                    continue;
                }

                started = true;
                RecordLine = _lines.LineNumber;
            }

            length += bytes.Length;
            if (length > _lines.MaxLineBytes)
            {
                throw new DataImportFileException(RecordLine, Line(
                    $"The record on line {RecordLine:N0} is longer than {DataExportDialog.Size(_lines.MaxLineBytes)}, the longest record an import reads. Nothing after it was read."));
            }

            if (!Utf8.IsValid(bytes))
            {
                Error ??= "The text is not valid UTF-8.";
            }

            if (_chars.Length < bytes.Length)
            {
                if (_chars.Length > 0)
                {
                    ArrayPool<char>.Shared.Return(_chars);
                }

                _chars = ArrayPool<char>.Shared.Rent(Math.Max(bytes.Length, 256));
            }

            var count = Encoding.UTF8.GetChars(bytes, _chars);
            var chars = _chars.AsSpan(0, count);
            for (var i = 0; i < chars.Length; i++)
            {
                var c = chars[i];
                if (inQuotes)
                {
                    if (c == '"')
                    {
                        if (i + 1 < chars.Length && chars[i + 1] == '"')
                        {
                            _field.Append('"');
                            i++;
                        }
                        else
                        {
                            inQuotes = false;
                            afterQuote = true;
                        }
                    }
                    else
                    {
                        _field.Append(c);
                    }
                }
                else if (c == _delimiter)
                {
                    EndField(ref afterQuote);
                }
                else if (c == '"' && _field.Length == 0 && !afterQuote)
                {
                    inQuotes = true;
                }
                else
                {
                    if (afterQuote)
                    {
                        Error ??= Line($"There is text after the closing quote of field {_fields.Count + 1:N0}.");
                    }

                    _field.Append(c);
                }
            }

            if (inQuotes)
            {
                // A quoted field runs on: the line break is part of its text.
                if (_lines.EndedWithCarriageReturn)
                {
                    _field.Append('\r');
                }

                if (_lines.EndedWithLineFeed)
                {
                    _field.Append('\n');
                }

                continue;
            }

            EndField(ref afterQuote);
            return true;
        }
    }

    private void EndField(ref bool afterQuote)
    {
        afterQuote = false;
        if (_fields.Count >= MaxFields)
        {
            Error ??= Line($"The record has more than {MaxFields:N0} fields.");
            _field.Clear();
            return;
        }

        _fields.Add(_field.ToString());
        _field.Clear();
    }

    private static string Line(FormattableString text) => FormattableString.Invariant(text);
}
