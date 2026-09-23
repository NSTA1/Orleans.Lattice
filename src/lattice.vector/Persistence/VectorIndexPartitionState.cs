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
/// <para>
/// A flush rewrites only the chunks whose content changed, so the chunks of one
/// partition may have been written under different epochs. <see cref="Epoch"/> is
/// then the newest of them, and the record carries one epoch per chunk after the
/// fixed fields. The swap is still made by replacing this one record, so a loader
/// still sees either the whole previous partition or the whole new one.
/// </para>
/// </summary>
/// <param name="Epoch">The newest epoch any of the partition's live chunks was written under.</param>
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
    /// <para>
    /// Only the compact form, in which every chunk lives under
    /// <see cref="Epoch"/>, is accepted here. A partition that a flush rewrote in
    /// part keeps its unchanged chunks under their earlier epochs and is stored in
    /// an extended form naming each chunk's epoch, which the durable index decodes
    /// itself.
    /// </para>
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

    /// <summary>
    /// Renders the state as a durable record that also names the epoch each chunk
    /// was written under.
    /// <para>
    /// A flush that rewrites only the chunks whose content changed leaves the
    /// others under the epochs they were first written at, so the partition's
    /// chunks no longer share one epoch and the record has to say where each one
    /// lives. When they do all share <see cref="Epoch"/>, the compact form is
    /// written instead, which is byte-identical to what a build without per-chunk
    /// epochs writes and reads.
    /// </para>
    /// </summary>
    /// <param name="chunkEpochs">One epoch per chunk, <see cref="ChunkCount"/> of them.</param>
    internal byte[] ToRecord(ReadOnlySpan<long> chunkEpochs)
    {
        if (chunkEpochs.Length != ChunkCount)
        {
            throw new ArgumentException(
                $"A partition of {ChunkCount} chunks needs {ChunkCount} chunk epochs, not {chunkEpochs.Length}.",
                nameof(chunkEpochs));
        }

        var uniform = true;
        foreach (var chunkEpoch in chunkEpochs)
        {
            if (chunkEpoch != Epoch)
            {
                uniform = false;
                break;
            }
        }

        if (uniform)
        {
            return ToRecord();
        }

        var payload = new byte[Size + (chunkEpochs.Length * sizeof(long))];
        Write(payload);
        for (var i = 0; i < chunkEpochs.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(Size + (i * sizeof(long)), sizeof(long)), chunkEpochs[i]);
        }

        return VectorIndexRecord.Wrap(payload);
    }

    /// <summary>
    /// Decodes a partition state in either its compact form, where every chunk
    /// lives under <see cref="Epoch"/>, or its extended form, which names each
    /// chunk's epoch.
    /// </summary>
    /// <param name="record">The persisted record bytes.</param>
    /// <param name="state">The decoded state when this returns <see langword="true"/>.</param>
    /// <param name="chunkEpochs">One epoch per chunk when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the record is a partition state this build can act on.</returns>
    internal static bool TryReadRecord(
        ReadOnlySpan<byte> record, out VectorIndexPartitionState state, out long[] chunkEpochs)
    {
        state = default;
        chunkEpochs = [];
        if (!VectorIndexRecord.TryUnwrap(record, out var payload) || payload.Length < Size)
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

        var epochs = new long[chunkCount];
        if (payload.Length == Size)
        {
            Array.Fill(epochs, epoch);
        }
        else if (payload.Length == Size + ((long)chunkCount * sizeof(long)))
        {
            for (var i = 0; i < chunkCount; i++)
            {
                var chunkEpoch = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(Size + (i * sizeof(long)), sizeof(long)));

                // The record's own epoch is the newest any of its chunks carries,
                // so a chunk claiming a later one is a record this build did not write.
                if (chunkEpoch < 0 || chunkEpoch > epoch)
                {
                    return false;
                }

                epochs[i] = chunkEpoch;
            }
        }
        else
        {
            return false;
        }

        state = new VectorIndexPartitionState(epoch, chunkCount, vectorCount, indexVersion);
        chunkEpochs = epochs;
        return true;
    }
}
