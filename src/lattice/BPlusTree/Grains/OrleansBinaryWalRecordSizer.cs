using System.Buffers;
using Orleans.Serialization;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="IWalRecordSizer"/> implementation that returns
/// the exact serialised byte length under the canonical Orleans
/// binary wire format. Uses a counting <see cref="IBufferWriter{T}"/>
/// (<see cref="CountingBufferWriter"/>) so the serialiser walks every
/// field exactly once without producing a materialised buffer; the
/// counting writer's only state is a single rented scratch span
/// (returned via <see cref="ArrayPool{T}"/>) and a running total.
/// <para>
/// Constructing one instance per grain (singleton DI registration)
/// keeps the underlying <c>Serializer&lt;WalRecord&gt;</c> codec hot;
/// individual calls are fully thread-safe because each call rents its
/// own scratch span.
/// </para>
/// </summary>
internal sealed class OrleansBinaryWalRecordSizer(Serializer<WalRecord> serializer) : IWalRecordSizer
{
    private readonly Serializer<WalRecord> _serializer = serializer
        ?? throw new ArgumentNullException(nameof(serializer));

    /// <summary>
    /// Single-slot cached writer pool. The first thread to enter
    /// <see cref="Measure"/> after construction grabs the slot via
    /// <see cref="Interlocked.Exchange{T}(ref T, T)"/>; subsequent
    /// concurrent calls allocate a fresh writer and discard it on
    /// completion. The grain-turn scheduler serialises every call
    /// from a given grain so the steady-state path is single-threaded
    /// and reuses the slot every iteration, eliminating per-call
    /// writer heap allocations.
    /// </summary>
    private CountingBufferWriter? _cached;

    /// <inheritdoc />
    public int Measure(WalRecord entry)
    {
        var writer = Interlocked.Exchange(ref _cached, null) ?? new CountingBufferWriter();
        try
        {
            writer.Reset();
            _serializer.Serialize(entry, writer);
            return writer.WrittenCount;
        }
        finally
        {
            // Return the writer to the slot. If a concurrent caller
            // already returned theirs, ours is dropped on the floor
            // and its scratch buffer goes back to ArrayPool via the
            // writer's finaliser-safe Dispose - which the disposal
            // we trigger immediately below makes explicit.
            if (Interlocked.CompareExchange(ref _cached, writer, null) is not null)
            {
                writer.Dispose();
            }
        }
    }

    /// <summary>
    /// <see cref="IBufferWriter{T}"/> that counts bytes written
    /// without retaining them. Reuses a single rented buffer from the
    /// shared <see cref="ArrayPool{T}"/> across every span request -
    /// the serialiser's <c>GetSpan</c> / <c>Advance</c> pattern hands
    /// back the same scratch region every iteration, so the total
    /// allocation budget per <see cref="Measure"/> call is one writer
    /// instance plus one pool rent (the rent is typically reused
    /// across calls because the same pool bucket is hit).
    /// <para>
    /// Implemented as a <c>class</c> rather than a <c>struct</c>
    /// because the serialiser dispatches against the
    /// <see cref="IBufferWriter{T}"/> interface; a struct receiver
    /// would be boxed on the call and the running
    /// <see cref="WrittenCount"/> would be lost when control returns
    /// to <see cref="Measure"/>.
    /// </para>
    /// </summary>
    private sealed class CountingBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private const int DefaultScratchSize = 4096;
        private byte[]? _scratch;
        public int WrittenCount;

        /// <summary>
        /// Zeroes the running byte count so the writer can be reused
        /// for a subsequent <see cref="Measure"/> call without
        /// dropping its rented scratch buffer.
        /// </summary>
        public void Reset()
        {
            WrittenCount = 0;
        }

        public void Advance(int count)
        {
            if (count < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }
            WrittenCount += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0) => EnsureScratch(sizeHint);

        public Span<byte> GetSpan(int sizeHint = 0) => EnsureScratch(sizeHint).AsSpan();

        private byte[] EnsureScratch(int sizeHint)
        {
            var requested = sizeHint <= 0 ? DefaultScratchSize : sizeHint;
            if (_scratch is null || _scratch.Length < requested)
            {
                if (_scratch is not null)
                {
                    ArrayPool<byte>.Shared.Return(_scratch);
                }
                _scratch = ArrayPool<byte>.Shared.Rent(requested);
            }
            return _scratch;
        }

        public void Dispose()
        {
            if (_scratch is not null)
            {
                ArrayPool<byte>.Shared.Return(_scratch);
                _scratch = null;
            }
        }
    }
}
