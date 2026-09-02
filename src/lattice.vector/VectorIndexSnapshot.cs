using System.Buffers.Binary;

namespace Orleans.Lattice.Vector;

/// <summary>
/// A bounded, chunked plan for persisting a <see cref="VectorIndex"/>, captured
/// at one index version.
/// <para>
/// The snapshot never materialises the index as a single unbounded record: it
/// describes a fixed list of chunks, each of which the caller sizes with
/// <see cref="MeasureChunk"/> and renders into its own buffer with
/// <see cref="WriteChunk"/>. Centroid chunks come first, so a reader that has
/// applied only those can already rank partitions with
/// <see cref="VectorIndex.SelectPartitions"/> and fetch nothing but the vector
/// chunks it actually needs. Because a cell already stores its members
/// contiguously, a vector chunk is a slice of that cell rather than a gather
/// across the corpus.
/// </para>
/// <para>
/// The plan is only valid while the index it was taken from is unchanged. Every
/// render re-checks <see cref="VectorIndex.Version"/> and throws
/// <see cref="InvalidOperationException"/> if the index moved, so a concurrent
/// mutation can never produce a silently torn snapshot.
/// </para>
/// </summary>
public sealed class VectorIndexSnapshot
{
    private readonly VectorIndex _index;
    private readonly long _capturedVersion;
    private readonly VectorIndexChunkKind[] _kinds;
    private readonly int[] _partitionIds;
    private readonly int[] _sequences;
    private readonly int[] _itemCounts;
    private readonly int[] _offsets;

    internal VectorIndexSnapshot(VectorIndex index, int maxItemsPerChunk)
    {
        _index = index;
        _capturedVersion = index.Version;

        var dimensions = index.Dimensions;
        var partitionCount = index.PartitionCount;
        var centroidChunkCount = partitionCount == 0 ? 0 : CeilingDivide(partitionCount, maxItemsPerChunk);

        var chunkCount = centroidChunkCount + CountVectorChunks(index, maxItemsPerChunk);

        _kinds = new VectorIndexChunkKind[chunkCount];
        _partitionIds = new int[chunkCount];
        _sequences = new int[chunkCount];
        _itemCounts = new int[chunkCount];
        _offsets = new int[chunkCount];

        var next = 0;
        for (var sequence = 0; sequence < centroidChunkCount; sequence++)
        {
            var start = sequence * maxItemsPerChunk;
            var items = Math.Min(maxItemsPerChunk, partitionCount - start);
            RequireChunkFits((long)items * dimensions * sizeof(float));
            _kinds[next] = VectorIndexChunkKind.Centroids;
            _partitionIds[next] = start;
            _sequences[next] = sequence;
            _itemCounts[next] = items;
            _offsets[next] = start;
            next++;
        }

        var total = 0;
        if (partitionCount == 0)
        {
            var size = index.SegmentSize(0);
            next = PlanVectorChunks(next, partitionId: -1, size, maxItemsPerChunk, dimensions);
            total = size;
        }
        else
        {
            for (var partition = 0; partition < partitionCount; partition++)
            {
                var size = index.PartitionSize(partition);
                next = PlanVectorChunks(next, partition, size, maxItemsPerChunk, dimensions);
                total += size;
            }
        }

        Header = new VectorIndexHeader(
            VectorIndexFormat.Version,
            dimensions,
            index.Metric,
            partitionCount,
            index.Probes,
            index.Seed,
            total,
            next,
            centroidChunkCount,
            _capturedVersion);
    }

    /// <summary>
    /// The snapshot's fixed-size preamble. A durable consumer persists this
    /// first: it declares the shape, the format version, and how many chunks and
    /// centroid chunks follow.
    /// </summary>
    public VectorIndexHeader Header { get; }

    /// <summary>The number of chunks the snapshot is made of.</summary>
    public int ChunkCount => _kinds.Length;

    /// <summary>
    /// Describes one chunk without rendering it, so a caller can size a buffer,
    /// derive a storage key, and skip a partition whose
    /// <see cref="VectorIndex.PartitionVersion"/> has not moved.
    /// </summary>
    /// <param name="chunkIndex">The zero-based chunk index, below <see cref="ChunkCount"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkIndex"/> is out of range.</exception>
    public VectorIndexChunkDescriptor Describe(int chunkIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(chunkIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(chunkIndex, _kinds.Length);

        var kind = _kinds[chunkIndex];
        var items = _itemCounts[chunkIndex];
        var bytes = VectorIndexFormat.ChunkHeaderSize + (kind == VectorIndexChunkKind.Centroids
            ? items * Header.Dimensions * sizeof(float)
            : items * ((Header.Dimensions * sizeof(float)) + sizeof(long)));

        return new VectorIndexChunkDescriptor(
            kind, _partitionIds[chunkIndex], _sequences[chunkIndex], items, bytes);
    }

    /// <summary>
    /// The exact number of bytes <see cref="WriteChunk"/> writes for one chunk.
    /// </summary>
    /// <param name="chunkIndex">The zero-based chunk index, below <see cref="ChunkCount"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkIndex"/> is out of range.</exception>
    public int MeasureChunk(int chunkIndex) => Describe(chunkIndex).ByteCount;

    /// <summary>
    /// Renders one chunk into a caller-owned buffer and returns the number of
    /// bytes written.
    /// </summary>
    /// <param name="chunkIndex">The zero-based chunk index, below <see cref="ChunkCount"/>.</param>
    /// <param name="destination">A span of at least <see cref="MeasureChunk"/> bytes.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkIndex"/> is out of range.</exception>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too short.</exception>
    /// <exception cref="InvalidOperationException">The index was mutated after the snapshot was captured.</exception>
    public int WriteChunk(int chunkIndex, Span<byte> destination)
    {
        var descriptor = Describe(chunkIndex);
        if (_index.Version != _capturedVersion)
        {
            throw new InvalidOperationException(
                $"The index moved from version {_capturedVersion} to {_index.Version} after this snapshot was captured. "
                + "Capture a fresh snapshot rather than writing a torn one.");
        }

        if (destination.Length < descriptor.ByteCount)
        {
            throw new ArgumentException(
                $"Chunk {chunkIndex} needs {descriptor.ByteCount} bytes but only {destination.Length} were supplied.",
                nameof(destination));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[..4], VectorIndexFormat.ChunkMagic);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(4, 4), VectorIndexFormat.Version);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8, 4), (int)descriptor.Kind);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(12, 4), descriptor.PartitionId);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), descriptor.Sequence);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), descriptor.ItemCount);

        var payload = destination.Slice(
            VectorIndexFormat.ChunkHeaderSize, descriptor.ByteCount - VectorIndexFormat.ChunkHeaderSize);

        if (descriptor.Kind == VectorIndexChunkKind.Centroids)
        {
            _index.WriteCentroidPayload(_offsets[chunkIndex], descriptor.ItemCount, payload);
        }
        else
        {
            var segment = descriptor.PartitionId < 0 ? 0 : descriptor.PartitionId;
            _index.WriteVectorPayload(segment, _offsets[chunkIndex], descriptor.ItemCount, payload);
        }

        return descriptor.ByteCount;
    }

    private int PlanVectorChunks(int next, int partitionId, int count, int maxItemsPerChunk, int dimensions)
    {
        var chunks = CeilingDivide(count, maxItemsPerChunk);
        for (var sequence = 0; sequence < chunks; sequence++)
        {
            var start = sequence * maxItemsPerChunk;
            var items = Math.Min(maxItemsPerChunk, count - start);
            RequireChunkFits((long)items * ((dimensions * sizeof(float)) + sizeof(long)));
            _kinds[next] = VectorIndexChunkKind.Vectors;
            _partitionIds[next] = partitionId;
            _sequences[next] = sequence;
            _itemCounts[next] = items;
            _offsets[next] = start;
            next++;
        }

        return next;
    }

    private static int CountVectorChunks(VectorIndex index, int maxItemsPerChunk)
    {
        if (index.PartitionCount == 0)
        {
            return CeilingDivide(index.SegmentSize(0), maxItemsPerChunk);
        }

        var chunks = 0;
        for (var partition = 0; partition < index.PartitionCount; partition++)
        {
            chunks += CeilingDivide(index.PartitionSize(partition), maxItemsPerChunk);
        }

        return chunks;
    }

    // In long, because maxItemsPerChunk is caller-supplied: the usual int
    // ceiling-divide idiom overflows to a negative numerator for a very large
    // divisor and silently yields zero chunks for a non-empty index.
    private static int CeilingDivide(int count, int divisor) =>
        (int)(((long)count + divisor - 1) / divisor);

    private static void RequireChunkFits(long payloadBytes)
    {
        if (payloadBytes + VectorIndexFormat.ChunkHeaderSize > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(payloadBytes),
                "The requested chunk size produces a chunk larger than the largest span that can address it. Lower maxItemsPerChunk.");
        }
    }
}
