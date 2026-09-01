using System.Buffers.Binary;

namespace Orleans.Lattice.Vector;

/// <summary>
/// The fixed-size preamble of a chunked <see cref="VectorIndex"/> snapshot: the
/// index's shape, the seed it was trained from, and the number of chunks that
/// follow. It is small enough to persist as a single record, and a durable
/// consumer reads it first so it knows how to interpret - or refuse - the chunks.
/// </summary>
/// <param name="FormatVersion">The snapshot format version. See <see cref="VectorIndexFormat.Version"/>.</param>
/// <param name="Dimensions">The dimensionality of every vector in the snapshot.</param>
/// <param name="Metric">The similarity kernel the index ranks by.</param>
/// <param name="PartitionCount">The trained partition count, or <c>0</c> when the index was untrained.</param>
/// <param name="Probes">The probe count the index searched with, or <c>0</c> when it was untrained.</param>
/// <param name="Seed">The seed the index was trained from.</param>
/// <param name="Count">The number of live vectors the snapshot carries.</param>
/// <param name="ChunkCount">The total number of chunks that make up the snapshot.</param>
/// <param name="CentroidChunkCount">
/// How many of those chunks are <see cref="VectorIndexChunkKind.Centroids"/>. A
/// reader has the complete centroid set - and can therefore rank partitions -
/// once it has applied this many of them.
/// </param>
/// <param name="IndexVersion">The index's mutation counter at the moment the snapshot was captured.</param>
public readonly record struct VectorIndexHeader(
    int FormatVersion,
    int Dimensions,
    VectorDistanceMetric Metric,
    int PartitionCount,
    int Probes,
    ulong Seed,
    int Count,
    int ChunkCount,
    int CentroidChunkCount,
    long IndexVersion)
{
    /// <summary>The exact number of bytes <see cref="Write"/> produces.</summary>
    public static int Size => VectorIndexFormat.HeaderSize;

    /// <summary>
    /// Writes the header into <paramref name="destination"/> and returns the
    /// number of bytes written, which is always <see cref="Size"/>.
    /// </summary>
    /// <param name="destination">A span of at least <see cref="Size"/> bytes.</param>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too short.</exception>
    public int Write(Span<byte> destination)
    {
        if (destination.Length < VectorIndexFormat.HeaderSize)
        {
            throw new ArgumentException(
                $"A vector index header needs {VectorIndexFormat.HeaderSize} bytes but only {destination.Length} were supplied.",
                nameof(destination));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[..4], VectorIndexFormat.HeaderMagic);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(4, 4), FormatVersion);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8, 4), Dimensions);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(12, 4), (int)Metric);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), PartitionCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), Probes);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(24, 8), Seed);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(32, 4), Count);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(36, 4), ChunkCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(40, 4), CentroidChunkCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(44, 8), IndexVersion);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(52, 4), 0);
        return VectorIndexFormat.HeaderSize;
    }

    /// <summary>
    /// Reads a header, throwing when the bytes are not a header this build
    /// understands.
    /// </summary>
    /// <param name="source">The persisted header bytes.</param>
    /// <exception cref="VectorIndexFormatException">
    /// The span is too short, the marker is wrong, the format version is
    /// unsupported, or a field is out of range.
    /// </exception>
    public static VectorIndexHeader Read(ReadOnlySpan<byte> source)
    {
        if (source.Length < VectorIndexFormat.HeaderSize)
        {
            throw new VectorIndexFormatException(
                $"A vector index header is {VectorIndexFormat.HeaderSize} bytes but only {source.Length} were supplied.");
        }

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(source[..4]);
        if (magic != VectorIndexFormat.HeaderMagic)
        {
            throw new VectorIndexFormatException(
                $"These bytes do not open with the vector index header marker (read 0x{magic:X8}).");
        }

        var formatVersion = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(4, 4));
        if (!VectorIndexFormat.IsSupported(formatVersion))
        {
            throw new VectorIndexFormatException(
                $"Vector index snapshot format version {formatVersion} is not supported by this build, which reads version {VectorIndexFormat.Version}.");
        }

        var dimensions = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(8, 4));
        var metric = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(12, 4));
        var partitionCount = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(16, 4));
        var probes = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(20, 4));
        var seed = BinaryPrimitives.ReadUInt64LittleEndian(source.Slice(24, 8));
        var count = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(32, 4));
        var chunkCount = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(36, 4));
        var centroidChunkCount = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(40, 4));
        var indexVersion = BinaryPrimitives.ReadInt64LittleEndian(source.Slice(44, 8));

        if (dimensions <= 0)
        {
            throw new VectorIndexFormatException(
                $"A vector index header must declare a positive dimensionality but declared {dimensions}.");
        }

        if (metric is not ((int)VectorDistanceMetric.Cosine or (int)VectorDistanceMetric.DotProduct))
        {
            throw new VectorIndexFormatException(
                $"A vector index header declared metric {metric}, which is not a defined VectorDistanceMetric member.");
        }

        if (partitionCount < 0 || probes < 0 || count < 0 || chunkCount < 0 || centroidChunkCount < 0)
        {
            throw new VectorIndexFormatException(
                "A vector index header declared a negative count, which no snapshot this build writes can contain.");
        }

        return new VectorIndexHeader(
            formatVersion,
            dimensions,
            (VectorDistanceMetric)metric,
            partitionCount,
            probes,
            seed,
            count,
            chunkCount,
            centroidChunkCount,
            indexVersion);
    }

    /// <summary>
    /// Reads a header without throwing, so a consumer can treat an unreadable or
    /// version-incompatible persisted form as "rebuild from source" rather than
    /// as a fault.
    /// </summary>
    /// <param name="source">The persisted header bytes.</param>
    /// <param name="header">The decoded header when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the bytes are a header this build understands.</returns>
    public static bool TryRead(ReadOnlySpan<byte> source, out VectorIndexHeader header)
    {
        try
        {
            header = Read(source);
            return true;
        }
        catch (VectorIndexFormatException)
        {
            header = default;
            return false;
        }
    }
}
