using System.Buffers.Binary;
using System.IO.Hashing;

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// The self-verifying envelope every durable index record is wrapped in: a
/// marker, the layout version, the payload length, and a checksum over the
/// payload.
/// <para>
/// The envelope exists so corruption is <i>detected</i> rather than served. A
/// truncated record fails the length check, a torn or bit-flipped one fails the
/// checksum, a record from a future build fails the version check, and a record
/// read from the wrong key fails the marker check. All four collapse to the same
/// answer - <see cref="TryUnwrap"/> returns <see langword="false"/> - because the
/// index is a derived projection and the correct response to any of them is to
/// discard it and recompute, never to repair it or to trust it partially.
/// </para>
/// </summary>
public static class VectorIndexRecord
{
    /// <summary>
    /// The total number of bytes a record occupies for a payload of
    /// <paramref name="payloadLength"/> bytes.
    /// </summary>
    /// <param name="payloadLength">The payload length in bytes.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="payloadLength"/> is negative.</exception>
    public static int Measure(int payloadLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(payloadLength);
        return VectorIndexPersistenceFormat.RecordHeaderSize + payloadLength;
    }

    /// <summary>
    /// Writes the envelope and its payload into a caller-owned buffer and returns
    /// the number of bytes written.
    /// </summary>
    /// <param name="payload">The record body.</param>
    /// <param name="destination">A span of at least <see cref="Measure"/> bytes.</param>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too short.</exception>
    public static int Wrap(ReadOnlySpan<byte> payload, Span<byte> destination)
    {
        var total = Measure(payload.Length);
        if (destination.Length < total)
        {
            throw new ArgumentException(
                $"A vector index record of {payload.Length} payload bytes needs {total} bytes but only {destination.Length} were supplied.",
                nameof(destination));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[..4], VectorIndexPersistenceFormat.RecordMagic);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(4, 4), VectorIndexPersistenceFormat.Version);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8, 4), payload.Length);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(12, 4), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(destination.Slice(16, 8), XxHash64.HashToUInt64(payload));
        payload.CopyTo(destination[VectorIndexPersistenceFormat.RecordHeaderSize..]);
        return total;
    }

    /// <summary>
    /// Allocates and returns a complete record for a payload. Used on the flush
    /// path, where the store's write surface takes an owned array anyway.
    /// </summary>
    /// <param name="payload">The record body.</param>
    public static byte[] Wrap(ReadOnlySpan<byte> payload)
    {
        var record = new byte[Measure(payload.Length)];
        Wrap(payload, record);
        return record;
    }

    /// <summary>
    /// Stamps the envelope over a payload that has already been written into the
    /// record's payload region, so a chunk is rendered straight into its final
    /// buffer instead of being built and then copied.
    /// </summary>
    /// <param name="record">A buffer of exactly <see cref="Measure"/> bytes for the payload length.</param>
    /// <param name="payloadLength">The number of payload bytes already written.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="payloadLength"/> is negative.</exception>
    /// <exception cref="ArgumentException"><paramref name="record"/> is not exactly the right length.</exception>
    public static void Seal(Span<byte> record, int payloadLength)
    {
        var total = Measure(payloadLength);
        if (record.Length != total)
        {
            throw new ArgumentException(
                $"Sealing a record of {payloadLength} payload bytes needs a buffer of exactly {total} bytes, but {record.Length} were supplied.",
                nameof(record));
        }

        var body = record.Slice(VectorIndexPersistenceFormat.RecordHeaderSize, payloadLength);
        BinaryPrimitives.WriteUInt32LittleEndian(record[..4], VectorIndexPersistenceFormat.RecordMagic);
        BinaryPrimitives.WriteInt32LittleEndian(record.Slice(4, 4), VectorIndexPersistenceFormat.Version);
        BinaryPrimitives.WriteInt32LittleEndian(record.Slice(8, 4), payloadLength);
        BinaryPrimitives.WriteInt32LittleEndian(record.Slice(12, 4), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(record.Slice(16, 8), XxHash64.HashToUInt64(body));
    }

    /// <summary>
    /// Validates a persisted record and yields its payload without copying it.
    /// </summary>
    /// <param name="record">The persisted bytes.</param>
    /// <param name="payload">The verified payload when this returns <see langword="true"/>.</param>
    /// <returns>
    /// <see langword="false"/> when the record is truncated, does not carry the
    /// marker, declares an unsupported layout version, declares a length that
    /// does not match the bytes present, or fails its checksum.
    /// </returns>
    public static bool TryUnwrap(ReadOnlySpan<byte> record, out ReadOnlySpan<byte> payload)
    {
        payload = default;
        if (record.Length < VectorIndexPersistenceFormat.RecordHeaderSize)
        {
            return false;
        }

        if (BinaryPrimitives.ReadUInt32LittleEndian(record[..4]) != VectorIndexPersistenceFormat.RecordMagic)
        {
            return false;
        }

        if (!VectorIndexPersistenceFormat.IsSupported(BinaryPrimitives.ReadInt32LittleEndian(record.Slice(4, 4))))
        {
            return false;
        }

        var length = BinaryPrimitives.ReadInt32LittleEndian(record.Slice(8, 4));
        if (length < 0 || Measure(length) != record.Length)
        {
            return false;
        }

        var body = record.Slice(VectorIndexPersistenceFormat.RecordHeaderSize, length);
        if (XxHash64.HashToUInt64(body) != BinaryPrimitives.ReadUInt64LittleEndian(record.Slice(16, 8)))
        {
            return false;
        }

        payload = body;
        return true;
    }
}
