using System.Buffers;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Reads a stream one line at a time through a fixed read buffer (issue #5365). A line that
/// fits the buffer is handed out as a slice of it, with no copy; one that spans reads is
/// assembled in a second pooled buffer bounded by <see cref="MaxLineBytes"/>, so the reader
/// holds at most one read buffer and one line whatever the size of the file.
/// </summary>
/// <remarks>
/// A line ends at a line feed, which with a preceding carriage return is not part of the
/// line. <see cref="Current"/> is valid only until the next <see cref="ReadLineAsync"/>.
/// </remarks>
internal sealed class DataImportLineReader : IDisposable
{
    /// <summary>The size of each read from the stream.</summary>
    internal const int ReadSize = 64 * 1024;

    private static readonly byte[] Utf8ByteOrderMark = [0xEF, 0xBB, 0xBF];

    private readonly Stream _stream;
    private byte[] _buffer;
    private byte[]? _assembly;
    private int _start;
    private int _end;
    private int _assembled;
    private bool _endOfStream;
    private ReadOnlyMemory<byte> _current;

    /// <summary>Creates a reader over <paramref name="stream"/>.</summary>
    /// <param name="stream">The file.</param>
    /// <param name="maxLineBytes">The longest line the reader accepts, in bytes.</param>
    public DataImportLineReader(Stream stream, int maxLineBytes)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxLineBytes, 1);
        _stream = stream;
        MaxLineBytes = maxLineBytes;
        _buffer = ArrayPool<byte>.Shared.Rent(ReadSize);
    }

    /// <summary>The longest line the reader accepts, in bytes.</summary>
    public int MaxLineBytes { get; }

    /// <summary>The 1-based number of the line in <see cref="Current"/>; zero before the first.</summary>
    public long LineNumber { get; private set; }

    /// <summary>Whether the line in <see cref="Current"/> ended with a carriage return before its line feed.</summary>
    public bool EndedWithCarriageReturn { get; private set; }

    /// <summary>Whether the line in <see cref="Current"/> ended with a line feed (the last line of a file may not).</summary>
    public bool EndedWithLineFeed { get; private set; }

    /// <summary>The current line's bytes, without its line end, and without a UTF-8 byte-order mark on the first line.</summary>
    public ReadOnlyMemory<byte> Current => _current;

    /// <summary>Reads the next line.</summary>
    /// <param name="cancellationToken">Stops the read.</param>
    /// <returns><see langword="false"/> at the end of the stream.</returns>
    /// <exception cref="DataImportFileException">A line is longer than <see cref="MaxLineBytes"/>.</exception>
    public async ValueTask<bool> ReadLineAsync(CancellationToken cancellationToken)
    {
        _assembled = 0;
        while (true)
        {
            var pending = _buffer.AsMemory(_start, _end - _start);
            var feed = pending.Span.IndexOf((byte)'\n');
            if (feed >= 0)
            {
                ReadOnlyMemory<byte> line;
                if (_assembled == 0)
                {
                    line = pending[..feed];
                }
                else
                {
                    Assemble(pending.Span[..feed]);
                    line = _assembly.AsMemory(0, _assembled);
                }

                _start += feed + 1;
                return Complete(line, lineFeed: true);
            }

            if (_endOfStream)
            {
                if (_assembled == 0 && pending.IsEmpty)
                {
                    _current = ReadOnlyMemory<byte>.Empty;
                    return false;
                }

                ReadOnlyMemory<byte> line;
                if (_assembled == 0)
                {
                    line = pending;
                }
                else
                {
                    Assemble(pending.Span);
                    line = _assembly.AsMemory(0, _assembled);
                }

                _start = _end;
                return Complete(line, lineFeed: false);
            }

            // The line continues past the bytes read so far: keep them, then read more.
            if (!pending.IsEmpty)
            {
                Assemble(pending.Span);
            }

            _start = 0;
            _end = 0;
            var read = await _stream.ReadAsync(_buffer.AsMemory(0, ReadSize), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                _endOfStream = true;
            }
            else
            {
                _end = read;
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = [];
        if (_assembly is not null)
        {
            ArrayPool<byte>.Shared.Return(_assembly);
            _assembly = null;
        }
    }

    private bool Complete(ReadOnlyMemory<byte> line, bool lineFeed)
    {
        LineNumber++;
        EndedWithLineFeed = lineFeed;
        EndedWithCarriageReturn = lineFeed && !line.IsEmpty && line.Span[^1] == (byte)'\r';
        if (EndedWithCarriageReturn)
        {
            line = line[..^1];
        }

        if (LineNumber == 1 && line.Span.StartsWith(Utf8ByteOrderMark))
        {
            line = line[Utf8ByteOrderMark.Length..];
        }

        if (line.Length > MaxLineBytes)
        {
            throw TooLong();
        }

        _current = line;
        return true;
    }

    private void Assemble(ReadOnlySpan<byte> bytes)
    {
        // One byte of slack for the carriage return a line may end with.
        if (_assembled + bytes.Length > MaxLineBytes + 1)
        {
            throw TooLong();
        }

        if (_assembly is null || _assembly.Length < _assembled + bytes.Length)
        {
            var grown = ArrayPool<byte>.Shared.Rent(Math.Max(_assembled + bytes.Length, Math.Min(MaxLineBytes + 1, (_assembly?.Length ?? ReadSize) * 2)));
            if (_assembly is not null)
            {
                _assembly.AsSpan(0, _assembled).CopyTo(grown);
                ArrayPool<byte>.Shared.Return(_assembly);
            }

            _assembly = grown;
        }

        bytes.CopyTo(_assembly.AsSpan(_assembled));
        _assembled += bytes.Length;
    }

    private DataImportFileException TooLong() =>
        new(LineNumber + 1, string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"Line {LineNumber + 1:N0} is longer than {DataExportDialog.Size(MaxLineBytes)}, the longest line an import reads. Nothing after it was read."));
}
