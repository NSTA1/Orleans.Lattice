using System.Buffers.Binary;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The commit record of a persisted index: the snapshot header the chunks must
/// be interpreted against, plus the generation and centroid epoch that say
/// <i>which</i> chunks are live.
/// <para>
/// It is written last by every flush, so its presence is what promotes a set of
/// written chunks to a loadable index. A reader that cannot decode it - because
/// it is absent, truncated, corrupt, or from a layout or snapshot version this
/// build does not read - has a clean "rebuild from source" branch rather than a
/// fault, which is the whole reason the index is allowed to be a derived
/// projection.
/// </para>
/// </summary>
/// <param name="Generation">The generation whose records this manifest commits.</param>
/// <param name="CentroidEpoch">The epoch the live centroid chunks were written under.</param>
/// <param name="IndexedCount">The number of live vectors the committed records carry.</param>
/// <param name="Header">The snapshot header the chunks were produced from.</param>
public readonly record struct VectorIndexManifest(
    long Generation,
    long CentroidEpoch,
    int IndexedCount,
    VectorIndexHeader Header)
{
    /// <summary>The exact number of bytes <see cref="Write"/> produces.</summary>
    public static int Size => VectorIndexPersistenceFormat.ManifestPayloadSize;

    /// <summary>
    /// Writes the manifest payload into <paramref name="destination"/> and
    /// returns the number of bytes written, which is always <see cref="Size"/>.
    /// </summary>
    /// <param name="destination">A span of at least <see cref="Size"/> bytes.</param>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too short.</exception>
    public int Write(Span<byte> destination)
    {
        if (destination.Length < Size)
        {
            throw new ArgumentException(
                $"A vector index manifest needs {Size} bytes but only {destination.Length} were supplied.",
                nameof(destination));
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[..8], Generation);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(8, 8), CentroidEpoch);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(16, 4), IndexedCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(20, 4), Header.PartitionCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(24, 8), 0);
        Header.Write(destination.Slice(32, VectorIndexFormat.HeaderSize));
        return Size;
    }

    /// <summary>
    /// Renders the manifest as a complete, checksummed durable record.
    /// </summary>
    public byte[] ToRecord()
    {
        Span<byte> payload = stackalloc byte[Size];
        Write(payload);
        return VectorIndexRecord.Wrap(payload);
    }

    /// <summary>
    /// Decodes a manifest from a complete durable record without throwing, so an
    /// unreadable, corrupt, or version-incompatible manifest is a clean branch
    /// rather than a fault.
    /// </summary>
    /// <param name="record">The persisted record bytes, or <see langword="null"/> when the key was absent.</param>
    /// <param name="manifest">The decoded manifest when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the record is a manifest this build can act on.</returns>
    public static bool TryReadRecord(ReadOnlySpan<byte> record, out VectorIndexManifest manifest)
    {
        manifest = default;
        if (!VectorIndexRecord.TryUnwrap(record, out var payload) || payload.Length != Size)
        {
            return false;
        }

        var generation = BinaryPrimitives.ReadInt64LittleEndian(payload[..8]);
        var centroidEpoch = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(8, 8));
        var indexedCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(16, 4));
        var declaredPartitions = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(20, 4));

        if (generation < 0 || centroidEpoch < 0 || indexedCount < 0)
        {
            return false;
        }

        if (!VectorIndexHeader.TryRead(payload.Slice(32, VectorIndexFormat.HeaderSize), out var header))
        {
            return false;
        }

        // The partition count is carried twice on purpose: the manifest's own
        // copy and the snapshot header's. They are written together and can only
        // disagree if the record was assembled from two generations, which is
        // exactly the silent corruption a checksum over one contiguous payload
        // cannot see.
        if (declaredPartitions != header.PartitionCount || indexedCount != header.Count)
        {
            return false;
        }

        manifest = new VectorIndexManifest(generation, centroidEpoch, indexedCount, header);
        return true;
    }
}
