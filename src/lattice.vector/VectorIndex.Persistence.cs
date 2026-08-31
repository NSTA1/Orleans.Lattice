using System.Buffers;
using System.Buffers.Binary;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;

namespace Orleans.Lattice.Vector;

public sealed partial class VectorIndex
{
    /// <summary>The seed this index trains from, restated in every snapshot header.</summary>
    public ulong Seed => _options.Seed;

    /// <summary>
    /// Whether every centroid chunk has been applied. A restored index only
    /// becomes <see cref="VectorIndexState.Ready"/> once this is
    /// <see langword="true"/>; until then it answers exhaustively over whatever
    /// vectors have arrived, and reports <see cref="VectorIndexState.Building"/>.
    /// </summary>
    public bool CentroidsComplete => _missingCentroids == 0;

    /// <summary>
    /// Captures a chunked persistence plan for the index's current contents.
    /// Nothing is copied until <see cref="VectorIndexSnapshot.WriteChunk"/> is
    /// called, and every chunk holds at most <paramref name="maxItemsPerChunk"/>
    /// centroids or vectors, so no single record is unbounded.
    /// </summary>
    /// <param name="maxItemsPerChunk">The largest number of centroids or vectors one chunk may carry.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxItemsPerChunk"/> is not positive, or produces a chunk too large to address.</exception>
    public VectorIndexSnapshot CreateSnapshot(int maxItemsPerChunk)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxItemsPerChunk);
        return new VectorIndexSnapshot(this, maxItemsPerChunk);
    }

    /// <summary>
    /// Creates an empty index shaped by a persisted header, ready to have its
    /// chunks applied with <see cref="ApplyChunk"/>. The header supplies the
    /// dimensionality, metric, partitioning, and probe count; the options supply
    /// the knobs a snapshot does not carry, such as the training sample size a
    /// later retrain would use.
    /// </summary>
    /// <param name="header">A header previously read with <see cref="VectorIndexHeader.Read"/>.</param>
    /// <param name="options">The configuration for the restored index. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="VectorIndexFormatException">
    /// The header is not one this build supports, or contradicts
    /// <paramref name="options"/> on dimensionality or metric, or declares a
    /// partitioning it supplies no centroid chunks for.
    /// </exception>
    public static VectorIndex Restore(VectorIndexHeader header, VectorIndexOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (!VectorIndexFormat.IsSupported(header.FormatVersion))
        {
            throw new VectorIndexFormatException(
                $"Vector index snapshot format version {header.FormatVersion} is not supported by this build, which reads version {VectorIndexFormat.Version}.");
        }

        if (header.Dimensions != options.Dimensions)
        {
            throw new VectorIndexFormatException(
                $"The snapshot stores {header.Dimensions}-dimensional vectors but the supplied options declare {options.Dimensions}.");
        }

        if (header.Metric != options.Metric)
        {
            throw new VectorIndexFormatException(
                $"The snapshot was built for metric {header.Metric} but the supplied options declare {options.Metric}.");
        }

        if (header.PartitionCount == 0 && header.CentroidChunkCount != 0)
        {
            throw new VectorIndexFormatException(
                "The snapshot declares centroid chunks but no partitions, so it could never be applied.");
        }

        if (header.PartitionCount > 0 && header.CentroidChunkCount == 0)
        {
            throw new VectorIndexFormatException(
                $"The snapshot declares {header.PartitionCount} partitions but no centroid chunks, so the partitioning could never be restored.");
        }

        var index = new VectorIndex(options);
        if (header.PartitionCount > 0)
        {
            index.PrepareForRestore(header);
        }
        else
        {
            index.EnsureSegmentsExist();
        }

        if (header.Count > 0)
        {
            index.EnsureCapacity(header.Count);
        }

        return index;
    }

    /// <summary>
    /// Applies one persisted chunk, returning what it contained. Chunks may
    /// arrive in any order and may be re-applied safely: a vector chunk replaces
    /// any key it carries that is already present, so a resumed or retried
    /// restore converges on the same index.
    /// </summary>
    /// <param name="chunk">The bytes previously produced by <see cref="VectorIndexSnapshot.WriteChunk"/>.</param>
    /// <exception cref="VectorIndexFormatException">
    /// The chunk marker or format version is wrong, the chunk is truncated, or it
    /// names a partition this index does not have.
    /// </exception>
    public VectorIndexChunkDescriptor ApplyChunk(ReadOnlySpan<byte> chunk)
    {
        if (chunk.Length < VectorIndexFormat.ChunkHeaderSize)
        {
            throw new VectorIndexFormatException(
                $"A vector index chunk is at least {VectorIndexFormat.ChunkHeaderSize} bytes but only {chunk.Length} were supplied.");
        }

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(chunk[..4]);
        if (magic != VectorIndexFormat.ChunkMagic)
        {
            throw new VectorIndexFormatException(
                $"These bytes do not open with the vector index chunk marker (read 0x{magic:X8}).");
        }

        var formatVersion = BinaryPrimitives.ReadInt32LittleEndian(chunk.Slice(4, 4));
        if (!VectorIndexFormat.IsSupported(formatVersion))
        {
            throw new VectorIndexFormatException(
                $"Vector index chunk format version {formatVersion} is not supported by this build, which reads version {VectorIndexFormat.Version}.");
        }

        var kind = (VectorIndexChunkKind)BinaryPrimitives.ReadInt32LittleEndian(chunk.Slice(8, 4));
        var partitionId = BinaryPrimitives.ReadInt32LittleEndian(chunk.Slice(12, 4));
        var sequence = BinaryPrimitives.ReadInt32LittleEndian(chunk.Slice(16, 4));
        var itemCount = BinaryPrimitives.ReadInt32LittleEndian(chunk.Slice(20, 4));

        if (itemCount < 0 || sequence < 0)
        {
            throw new VectorIndexFormatException(
                "A vector index chunk declared a negative item count or sequence.");
        }

        var payload = chunk[VectorIndexFormat.ChunkHeaderSize..];
        return kind switch
        {
            VectorIndexChunkKind.Centroids => ApplyCentroidChunk(partitionId, sequence, itemCount, payload),
            VectorIndexChunkKind.Vectors => ApplyVectorChunk(partitionId, sequence, itemCount, payload),
            _ => throw new VectorIndexFormatException(
                $"A vector index chunk declared kind {(int)kind}, which is not a defined VectorIndexChunkKind member."),
        };
    }

    private void PrepareForRestore(VectorIndexHeader header)
    {
        var centroidLength = (long)header.PartitionCount * _dimensions;
        if (centroidLength > Array.MaxLength)
        {
            throw new VectorIndexFormatException(
                $"A centroid block of {header.PartitionCount} partitions by {_dimensions} dimensions exceeds the largest array the runtime can allocate.");
        }

        _partitionCount = header.PartitionCount;
        _probes = header.Probes > 0
            ? Math.Min(header.Probes, header.PartitionCount)
            : VectorIndexOptions.AutoProbes(header.PartitionCount);
        _centroids = new float[(int)centroidLength];
        _centroidSquaredNorms = new float[header.PartitionCount];

        // Readiness is tracked per partition, not per chunk sequence. A chunk
        // that arrives carrying no partitions - a truncated writer, or chunks
        // mixed from two snapshot generations - can then never promote the index
        // to Ready with a centroid block it has not actually filled.
        _centroidsPresent = new bool[header.PartitionCount];
        _missingCentroids = header.PartitionCount;
        _centroidChunkCount = header.CentroidChunkCount;
        AllocateSegments(header.PartitionCount);
        _version++;
    }

    private VectorIndexChunkDescriptor ApplyCentroidChunk(
        int firstPartition, int sequence, int itemCount, ReadOnlySpan<byte> payload)
    {
        if (_partitionCount == 0)
        {
            throw new VectorIndexFormatException(
                "A centroid chunk was applied to an index that was not restored from a partitioned header.");
        }

        if (itemCount <= 0)
        {
            throw new VectorIndexFormatException(
                "A centroid chunk carried no centroids, so it cannot be part of a snapshot this build wrote.");
        }

        if (firstPartition < 0 || (long)firstPartition + itemCount > _partitionCount)
        {
            throw new VectorIndexFormatException(
                $"A centroid chunk covers partitions [{firstPartition}, {firstPartition + itemCount}) but the index has {_partitionCount}.");
        }

        if (sequence >= _centroidChunkCount)
        {
            throw new VectorIndexFormatException(
                $"A centroid chunk declared sequence {sequence} but the header declared only {_centroidChunkCount} centroid chunks.");
        }

        var expected = (long)itemCount * _dimensions * sizeof(float);
        RequirePayload(payload.Length, expected, "centroid");

        ReadFloats(payload[..(int)expected], _centroids.AsSpan(firstPartition * _dimensions, itemCount * _dimensions));

        for (var partition = firstPartition; partition < firstPartition + itemCount; partition++)
        {
            if (!_centroidsPresent[partition])
            {
                _centroidsPresent[partition] = true;
                _missingCentroids--;
            }
        }

        _version++;
        if (_missingCentroids == 0)
        {
            RecomputeSquaredNorms(_centroids, _centroidSquaredNorms, _partitionCount);
            ReplaceProvisionalPlacements();
        }

        return new VectorIndexChunkDescriptor(
            VectorIndexChunkKind.Centroids,
            firstPartition,
            sequence,
            itemCount,
            VectorIndexFormat.ChunkHeaderSize + (int)expected);
    }

    private VectorIndexChunkDescriptor ApplyVectorChunk(
        int partitionId, int sequence, int itemCount, ReadOnlySpan<byte> payload)
    {
        if (partitionId < -1 || partitionId >= _partitionCount)
        {
            throw new VectorIndexFormatException(
                $"A vector chunk names partition {partitionId} but the index has {_partitionCount} partitions.");
        }

        if (partitionId == -1 && _partitionCount > 0)
        {
            throw new VectorIndexFormatException(
                "A vector chunk carried unassigned vectors but the index was restored with a partitioning.");
        }

        EnsureSegmentsExist();
        var segment = partitionId < 0 ? 0 : partitionId;
        var stride = (_dimensions * sizeof(float)) + sizeof(long);
        var expected = (long)itemCount * stride;
        RequirePayload(payload.Length, expected, "vector");

        for (var i = 0; i < itemCount; i++)
        {
            var record = payload.Slice(i * stride, stride);
            var key = BinaryPrimitives.ReadInt64LittleEndian(record[..sizeof(long)]);
            Remove(key);

            var position = _segmentCounts[segment];
            ReserveSegment(segment, position + 1);
            var destination = _segmentVectors[segment].AsSpan(position * _dimensions, _dimensions);
            ReadFloats(record[sizeof(long)..], destination);
            _segmentNorms[segment][position] = TensorPrimitives.Norm(destination);
            _segmentKeys[segment][position] = key;
            _segmentCounts[segment] = position + 1;
            _location[key] = Pack(segment, position);
            _provisional?.Remove(key);
            _count++;
            _version++;
            _segmentVersions[segment] = _version;
        }

        return new VectorIndexChunkDescriptor(
            VectorIndexChunkKind.Vectors,
            partitionId,
            sequence,
            itemCount,
            VectorIndexFormat.ChunkHeaderSize + (int)expected);
    }

    /// <summary>
    /// Re-places every vector that was inserted while a restore was still
    /// streaming its centroids. Such a vector was parked in cell 0 because there
    /// was no honest nearest cell at the time; now that the partitioning is
    /// complete it is moved to the cell its own query will actually probe.
    /// </summary>
    private void ReplaceProvisionalPlacements()
    {
        if (_provisional is not { Count: > 0 })
        {
            _provisional = null;
            return;
        }

        var pending = _provisional.Count;
        var keys = ArrayPool<long>.Shared.Rent(pending);
        var scratch = ArrayPool<float>.Shared.Rent(_dimensions);
        try
        {
            var written = 0;
            foreach (var key in _provisional)
            {
                keys[written++] = key;
            }

            // Cleared before re-inserting so the re-placed vectors are not parked
            // a second time by the very Insert that is fixing them.
            _provisional = null;

            var vector = scratch.AsSpan(0, _dimensions);
            for (var i = 0; i < written; i++)
            {
                var key = keys[i];
                if (!_location.TryGetValue(key, out var location))
                {
                    continue;
                }

                VectorAt(SegmentOf(location), PositionOf(location)).CopyTo(vector);
                Remove(key);
                Insert(key, vector);
            }
        }
        finally
        {
            ArrayPool<long>.Shared.Return(keys);
            ArrayPool<float>.Shared.Return(scratch);
        }
    }

    internal int SegmentSize(int segment) => _segmentCount == 0 ? 0 : _segmentCounts[segment];

    internal void WriteCentroidPayload(int firstPartition, int itemCount, Span<byte> destination) =>
        WriteFloats(_centroids.AsSpan(firstPartition * _dimensions, itemCount * _dimensions), destination);

    /// <summary>
    /// Renders a contiguous run of one cell as key / vector records. Because a
    /// cell already stores its members contiguously, this is a bounded walk over
    /// one slice rather than a gather across the corpus.
    /// </summary>
    internal void WriteVectorPayload(int segment, int offset, int itemCount, Span<byte> destination)
    {
        var stride = (_dimensions * sizeof(float)) + sizeof(long);
        var vectors = _segmentVectors[segment];
        var keys = _segmentKeys[segment];
        for (var i = 0; i < itemCount; i++)
        {
            var position = offset + i;
            var record = destination.Slice(i * stride, stride);
            BinaryPrimitives.WriteInt64LittleEndian(record[..sizeof(long)], keys[position]);
            WriteFloats(new ReadOnlySpan<float>(vectors, position * _dimensions, _dimensions), record[sizeof(long)..]);
        }
    }

    private static void RequirePayload(int actual, long expected, string what)
    {
        if (actual < expected)
        {
            throw new VectorIndexFormatException(
                $"A {what} chunk declares a payload of {expected} bytes but only {actual} were supplied.");
        }
    }

    private static void WriteFloats(ReadOnlySpan<float> source, Span<byte> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.AsBytes(source).CopyTo(destination);
            return;
        }

        for (var i = 0; i < source.Length; i++)
        {
            BinaryPrimitives.WriteSingleLittleEndian(destination.Slice(i * sizeof(float), sizeof(float)), source[i]);
        }
    }

    private static void ReadFloats(ReadOnlySpan<byte> source, Span<float> destination)
    {
        if (BitConverter.IsLittleEndian)
        {
            source[..(destination.Length * sizeof(float))].CopyTo(MemoryMarshal.AsBytes(destination));
            return;
        }

        for (var i = 0; i < destination.Length; i++)
        {
            destination[i] = BinaryPrimitives.ReadSingleLittleEndian(source.Slice(i * sizeof(float), sizeof(float)));
        }
    }
}
