using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Every line an import refused (issue #5365). The first <see cref="ShownLimit"/> are kept
/// in memory for the dialog to list; every one is also appended, as one NDJSON line, to a
/// temporary file the user can download in full. So the list is complete however many
/// lines were refused, and memory holds only the first page of it.
/// </summary>
/// <remarks>
/// The file holds keys and reasons, never values. It is created on the first refusal in the
/// process's temporary directory under a random name, readable by the downloads that open
/// it, and deleted when the log is disposed: when the import's dialog closes or its circuit
/// ends.
/// </remarks>
internal sealed class DataImportRejectionLog : IDisposable
{
    /// <summary>How many refusals the dialog lists.</summary>
    public const int ShownLimit = 100;

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private readonly List<DataImportRejection> _shown = [];
    private readonly Lock _gate = new();
    private readonly ArrayBufferWriter<byte> _line = new(512);
    private readonly Utf8JsonWriter _writer;
    private readonly string _directory;
    private FileStream? _file;
    private string? _path;
    private long _count;
    private bool _disposed;

    /// <summary>Creates an empty log.</summary>
    /// <param name="directory">Where the full list is written; the process's temporary directory when <see langword="null"/>.</param>
    public DataImportRejectionLog(string? directory = null)
    {
        _directory = directory ?? Path.GetTempPath();
        _writer = new Utf8JsonWriter(_line, WriterOptions);
    }

    /// <summary>How many lines were refused.</summary>
    public long Count => Volatile.Read(ref _count);

    /// <summary>The first <see cref="ShownLimit"/> refusals, in the order they were reported.</summary>
    public IReadOnlyList<DataImportRejection> Shown
    {
        get
        {
            lock (_gate)
            {
                return [.. _shown];
            }
        }
    }

    /// <summary>The path of the full list, or <see langword="null"/> when nothing has been refused.</summary>
    internal string? FilePath => _path;

    /// <summary>Records a refusal.</summary>
    /// <param name="rejection">The refusal.</param>
    public void Add(DataImportRejection rejection)
    {
        ArgumentNullException.ThrowIfNull(rejection);
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            if (_shown.Count < ShownLimit)
            {
                _shown.Add(rejection);
            }

            _file ??= Create();
            _line.ResetWrittenCount();
            _writer.Reset(_line);
            _writer.WriteStartObject();
            if (rejection.Line is { } line)
            {
                _writer.WriteNumber("line", line);
            }
            else
            {
                _writer.WriteNull("line");
            }

            _writer.WriteString("key", rejection.Key);
            _writer.WriteString("code", rejection.Code);
            _writer.WriteString("reason", rejection.Reason);
            _writer.WriteEndObject();
            _writer.Flush();
            _file.Write(_line.WrittenSpan);
            _file.WriteByte((byte)'\n');
            Volatile.Write(ref _count, _count + 1);
        }
    }

    /// <summary>
    /// Opens the full list for reading, as it stands: one NDJSON line per refusal. Empty when
    /// nothing has been refused.
    /// </summary>
    /// <returns>A stream the caller disposes.</returns>
    public Stream OpenRead()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_file is null || _path is null)
            {
                return new MemoryStream([], writable: false);
            }

            _file.Flush();
            return new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _writer.Dispose();
            _file?.Dispose();
            _file = null;
            if (_path is not null)
            {
                try
                {
                    File.Delete(_path);
                }
                catch (IOException)
                {
                    // A download still reading it holds it open; the temporary directory reclaims it.
                }
                catch (UnauthorizedAccessException)
                {
                    // As above.
                }
            }
        }
    }

    private FileStream Create()
    {
        _path = Path.Combine(_directory, "lattice-import-rejections-" + Guid.NewGuid().ToString("N") + ".ndjson");
        return new FileStream(_path, FileMode.CreateNew, FileAccess.Write, FileShare.Read | FileShare.Delete, 16 * 1024);
    }
}
