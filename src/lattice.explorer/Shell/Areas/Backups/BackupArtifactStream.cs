namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// A read-only, forward-only <see cref="Stream"/> over an exported artifact's
/// chunk stream, so the artifact reaches the browser a chunk at a time and the
/// server never holds the whole of it.
/// </summary>
internal sealed class BackupArtifactStream : Stream
{
    private readonly IAsyncEnumerator<ReadOnlyMemory<byte>> _chunks;
    private ReadOnlyMemory<byte> _current;
    private bool _finished;
    private bool _disposed;
    private long _position;

    /// <summary>Wraps <paramref name="chunks"/>.</summary>
    /// <param name="chunks">The artifact's ordered chunks.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    public BackupArtifactStream(IAsyncEnumerable<ReadOnlyMemory<byte>> chunks, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(chunks);
        _chunks = chunks.GetAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    /// Opens the stream and reads its first chunk, so a denial or a missing
    /// artifact surfaces here, before any byte is handed to the browser.
    /// </summary>
    /// <param name="chunks">The artifact's ordered chunks.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    public static async Task<BackupArtifactStream> OpenAsync(IAsyncEnumerable<ReadOnlyMemory<byte>> chunks, CancellationToken cancellationToken = default)
    {
        var stream = new BackupArtifactStream(chunks, cancellationToken);
        try
        {
            if (await stream._chunks.MoveNextAsync().ConfigureAwait(false))
            {
                stream._current = stream._chunks.Current;
            }
            else
            {
                stream._finished = true;
            }

            return stream;
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public override bool CanRead => true;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => false;

    /// <inheritdoc />
    public override long Length => throw new NotSupportedException();

    /// <inheritdoc />
    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.IsEmpty)
        {
            return 0;
        }

        while (_current.IsEmpty)
        {
            if (_finished)
            {
                return 0;
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (!await _chunks.MoveNextAsync().ConfigureAwait(false))
            {
                _finished = true;
                return 0;
            }

            _current = _chunks.Current;
        }

        var count = Math.Min(buffer.Length, _current.Length);
        _current[..count].CopyTo(buffer);
        _current = _current[count..];
        _position += count;
        return count;
    }

    /// <inheritdoc />
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count) =>
        ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc />
    public override void Flush()
    {
    }

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void SetLength(long value) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _chunks.DisposeAsync().ConfigureAwait(false);
        }

        await base.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing && !_disposed)
        {
            _disposed = true;
            _chunks.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        base.Dispose(disposing);
    }
}
