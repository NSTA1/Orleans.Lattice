using System.Buffers.Binary;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// One partition's commit record: which epoch its live chunks were written
/// under, how many of them there are, and how many vectors they carry.
/// <para>
/// A partition is rewritten by writing its chunks under a fresh epoch and then
/// replacing this record. Because the record names the epoch, an interrupted
/// rewrite leaves the previous epoch's chunks still committed and the new
/// epoch's chunks orphaned, so a loader either sees the whole old partition or
/// the whole new one and never a mixture of the two. A mixture is what would let
/// a deleted vector reappear, so this is the record that makes incremental
/// persistence safe rather than merely cheap.
/// </para>
/// </summary>
/// <param name="Epoch">The epoch the partition's live chunks were written under.</param>
/// <param name="ChunkCount">How many chunks make up the partition at that epoch.</param>
/// <param name="VectorCount">How many vectors those chunks carry in total.</param>
/// <param name="IndexVersion">The index version stamp the partition was captured at.</param>
public readonly record struct VectorIndexPartitionState(
    long Epoch,
    int ChunkCount,
    int VectorCount,
    long IndexVersion)
{
    /// <summary>The exact number of bytes <see cref="Write"/> produces.</summary>
    public static int Size => VectorIndexPersistenceFormat.PartitionStatePayloadSize;

    /// <summary>
    /// Writes the payload into <paramref name="destination"/> and returns the
    /// number of bytes written, which is always <see cref="Size"/>.
    /// </summary>
    /// <param name="destination">A span of at least <see cref="Size"/> bytes.</param>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too short.</exception>
    public int Write(Span<byte> destination)
    {
        if (destination.Length < Size)
        {
            throw new ArgumentException(
                $"A vector index partition state needs {Size} bytes but only {destination.Length} were supplied.",
                nameof(destination));
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[..8], Epoch);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8, 4), ChunkCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(12, 4), VectorCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(16, 8), IndexVersion);
        return Size;
    }

    /// <summary>Renders the partition state as a complete, checksummed durable record.</summary>
    public byte[] ToRecord()
    {
        Span<byte> payload = stackalloc byte[Size];
        Write(payload);
        return VectorIndexRecord.Wrap(payload);
    }

    /// <summary>
    /// Decodes a partition state from a complete durable record without throwing.
    /// </summary>
    /// <param name="record">The persisted record bytes.</param>
    /// <param name="state">The decoded state when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the record is a partition state this build can act on.</returns>
    public static bool TryReadRecord(ReadOnlySpan<byte> record, out VectorIndexPartitionState state)
    {
        state = default;
        if (!VectorIndexRecord.TryUnwrap(record, out var payload) || payload.Length != Size)
        {
            return false;
        }

        var epoch = BinaryPrimitives.ReadInt64LittleEndian(payload[..8]);
        var chunkCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(8, 4));
        var vectorCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(12, 4));
        var indexVersion = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(16, 8));

        if (epoch < 0 || chunkCount < 0 || vectorCount < 0 || indexVersion < 0)
        {
            return false;
        }

        state = new VectorIndexPartitionState(epoch, chunkCount, vectorCount, indexVersion);
        return true;
    }
}
