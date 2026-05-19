using System.Buffers;

namespace Orleans.Lattice;

/// <summary>
/// <see cref="IBufferWriter{T}"/> backed by a rented <see cref="ArrayPool{T}.Shared"/>
/// buffer. Used by the WAL grain to encode each captured
/// <see cref="LatticeMutation"/> exactly once at append time; the
/// rented buffer is then detached as an
/// <see cref="ArraySegment{T}"/> and handed to the storage provider
/// via <see cref="IWalStorageProvider.AppendEncodedBatchAsync"/>. The
/// grain returns the underlying array to the pool once the flush
/// settles, so the per-append allocation is bounded by the encoder's
/// resize amplification rather than by the raw payload size.
/// <para>
/// Internal: the contract is WAL-local but the type is not itself a
/// grain - it is a plain <see cref="IBufferWriter{T}"/> adapter over
/// <see cref="ArrayPool{T}.Shared"/>. Reusable by any future WAL
/// codepath in <c>Orleans.Lattice</c> that wants to produce
/// <see cref="ArraySegment{T}"/>-shaped payloads without allocating
/// a fresh <see cref="byte"/>[] per encode.
/// </para>
/// </summary>
internal sealed class PooledByteBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[]? _buffer;
    private int _written;

    /// <summary>
    /// Initialises a new writer with no rented buffer; the first
    /// <see cref="GetSpan"/> / <see cref="GetMemory"/> call rents one
    /// from the shared pool sized to the caller's hint (or 256 bytes
    /// if the hint is zero).
    /// </summary>
    public PooledByteBufferWriter()
    {
    }

    /// <summary>
    /// Number of bytes written so far to the rented buffer.
    /// </summary>
    public int WrittenCount => _written;

    /// <inheritdoc />
    public void Advance(int count)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }
        if (_buffer is null || _written + count > _buffer.Length)
        {
            throw new InvalidOperationException("Advanced past the end of the rented buffer.");
        }
        _written += count;
    }

    /// <inheritdoc />
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer!.AsMemory(_written);
    }

    /// <inheritdoc />
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer!.AsSpan(_written);
    }

    /// <summary>
    /// Returns an <see cref="ArraySegment{T}"/> covering the written
    /// bytes and transfers ownership of the rented buffer to the
    /// caller. After this call the writer is detached: subsequent
    /// <see cref="GetSpan"/> / <see cref="GetMemory"/> calls will
    /// rent a fresh buffer. The caller is responsible for returning
    /// the segment's <see cref="ArraySegment{T}.Array"/> to
    /// <see cref="ArrayPool{T}.Shared"/> once the bytes are no longer
    /// needed.
    /// </summary>
    public ArraySegment<byte> DetachWrittenSegment()
    {
        if (_buffer is null)
        {
            // Caller produced zero bytes (e.g. an encoder that
            // serialises a default value into nothing). Return an
            // empty segment with a non-null backing array so the
            // grain's "return to pool" path is unconditional.
            return new ArraySegment<byte>(Array.Empty<byte>(), 0, 0);
        }
        var segment = new ArraySegment<byte>(_buffer, 0, _written);
        _buffer = null;
        _written = 0;
        return segment;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null;
            _written = 0;
        }
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(sizeHint));
        }
        if (sizeHint == 0)
        {
            sizeHint = 1;
        }

        if (_buffer is null)
        {
            var initial = Math.Max(sizeHint, 256);
            _buffer = ArrayPool<byte>.Shared.Rent(initial);
            return;
        }

        var available = _buffer.Length - _written;
        if (available >= sizeHint)
        {
            return;
        }

        var newSize = Math.Max(_buffer.Length * 2, _written + sizeHint);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _written);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }
}
