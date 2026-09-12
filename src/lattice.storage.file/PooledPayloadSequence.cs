using System.Buffers;

namespace Orleans.Lattice.Storage.File;

/// <summary>
/// Reads a WAL payload into a chain of pooled fixed-size chunks and exposes
/// it as a <see cref="ReadOnlySequence{T}"/>, so decoding an entry never
/// requires a contiguous buffer the size of the entry.
/// <para>
/// <b>Why this matters more than the byte count.</b> A per-page byte ceiling
/// bounds how much a read totals; it does not bound the largest single block
/// the read must find. The previous decode path needed one contiguous
/// <c>byte[PayloadLength]</c> per entry and then let the deserializer
/// allocate again from it, so a large record demanded two to three times its
/// own size in <i>contiguous</i> memory. On a heap that is nearly full - and
/// therefore nearly always fragmented - a contiguous request is the first
/// thing to fail, and it fails while plenty of total memory remains. Chunked
/// segments turn that into many small requests that a fragmented heap can
/// still satisfy, and renting them means a steady-state replay allocates
/// essentially nothing per page.
/// </para>
/// <para>
/// Chunks are <see cref="ChunkBytes"/> so they stay below the 85,000-byte
/// large-object-heap threshold: a chunk that landed on the LOH would
/// reintroduce the fragmentation-sensitive allocation this type exists to
/// avoid, and would not be compacted between collections.
/// </para>
/// <para>
/// Not thread-safe; instances are owned by the single read in progress and
/// <see cref="Reset"/> is called between entries so one instance serves a
/// whole page.
/// </para>
/// </summary>
internal sealed class PooledPayloadSequence : IDisposable
{
    /// <summary>Size of each pooled chunk, below the LOH threshold.</summary>
    internal const int ChunkBytes = 64 * 1024;

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        internal Segment(ReadOnlyMemory<byte> memory, long runningIndex)
        {
            Memory = memory;
            RunningIndex = runningIndex;
        }

        internal void Append(Segment next) => Next = next;
    }

    private readonly List<byte[]> _rented = new();
    private Segment? _first;
    private Segment? _last;
    private int _lastLength;
    private bool _disposed;

    /// <summary>
    /// The payload most recently read by <see cref="Fill"/>, as a sequence
    /// over the pooled chunks. Valid until the next <see cref="Reset"/>.
    /// </summary>
    internal ReadOnlySequence<byte> Sequence =>
        _first is null || _last is null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(_first, 0, _last, _lastLength);

    /// <summary>
    /// Reads exactly <paramref name="byteCount"/> bytes from
    /// <paramref name="stream"/> at <paramref name="position"/> into pooled
    /// chunks, replacing any previously read payload.
    /// </summary>
    internal void Fill(Stream stream, long position, int byteCount)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        Reset();
        if (byteCount <= 0)
        {
            return;
        }

        stream.Seek(position, SeekOrigin.Begin);
        var remaining = byteCount;
        var runningIndex = 0L;
        while (remaining > 0)
        {
            var wanted = remaining < ChunkBytes ? remaining : ChunkBytes;
            var chunk = ArrayPool<byte>.Shared.Rent(wanted);
            _rented.Add(chunk);
            stream.ReadExactly(chunk, 0, wanted);

            var segment = new Segment(new ReadOnlyMemory<byte>(chunk, 0, wanted), runningIndex);
            if (_first is null)
            {
                _first = segment;
            }
            else
            {
                _last!.Append(segment);
            }

            _last = segment;
            _lastLength = wanted;
            runningIndex += wanted;
            remaining -= wanted;
        }
    }

    /// <summary>
    /// Returns every rented chunk to the pool and empties the sequence,
    /// leaving the instance reusable for the next entry.
    /// </summary>
    internal void Reset()
    {
        foreach (var chunk in _rented)
        {
            ArrayPool<byte>.Shared.Return(chunk);
        }

        _rented.Clear();
        _first = null;
        _last = null;
        _lastLength = 0;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        Reset();
        _disposed = true;
    }
}
