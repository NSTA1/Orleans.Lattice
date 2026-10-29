using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Storage.File;

/// <summary>
/// Binary framing for the segmented append-only WAL log file. Every
/// record is self-describing and CRC-protected so a crash that leaves a
/// torn trailing record is detected and discarded on recovery.
/// <para>
/// Wire layout of a single record:
/// <c>[type:1][bodyLen:4 LE][body:bodyLen][crc32:4 LE]</c> where the
/// CRC-32 covers the <c>type</c>, <c>bodyLen</c>, and <c>body</c> bytes.
/// Three record types participate:
/// </para>
/// <list type="bullet">
/// <item><description><b>Data</b> - one appended WAL entry. Body is
/// <c>[offset:8 LE][payload]</c>. Data records are durable-but-not-yet-
/// observable until a following <b>Commit</b> record covers them, which
/// is what gives a batch its all-or-nothing property: a crash between
/// the data records and the commit trailer leaves orphan data records
/// that recovery rolls back.</description></item>
/// <item><description><b>Commit</b> - seals the immediately-preceding
/// run of uncommitted Data records into one atomic batch. Body is
/// <c>[count:4 LE]</c>.</description></item>
/// <item><description><b>Trim</b> - a self-committing durability marker
/// recording that every entry with offset &lt;= <c>throughOffset</c> has
/// been logically trimmed. Body is <c>[throughOffset:8 LE]</c>. Losing a
/// trim marker to a crash only over-retains, so trim records need no
/// separate commit.</description></item>
/// </list>
/// </summary>
internal static class FileWalRecordFormat
{
    /// <summary>Record-type tag for an appended WAL entry.</summary>
    internal const byte RecordTypeData = 1;

    /// <summary>Record-type tag for a batch commit trailer.</summary>
    internal const byte RecordTypeCommit = 2;

    /// <summary>Record-type tag for a trim watermark marker.</summary>
    internal const byte RecordTypeTrim = 3;

    /// <summary>Byte length of the type tag and body-length prefix.</summary>
    internal const int HeaderLength = sizeof(byte) + sizeof(int);

    /// <summary>Byte length of the trailing CRC-32.</summary>
    internal const int ChecksumLength = sizeof(uint);

    /// <summary>
    /// Fixed per-record framing overhead in bytes: the 1-byte type tag,
    /// the 4-byte body length, and the trailing 4-byte CRC-32.
    /// </summary>
    internal const int FramingOverhead = HeaderLength + ChecksumLength;

    /// <summary>Byte length of a Data record body's fixed offset prefix.</summary>
    internal const int DataBodyPrefix = sizeof(long);

    /// <summary>Byte position of the payload within a Data record.</summary>
    internal const int DataPayloadOffset = HeaderLength + DataBodyPrefix;

    /// <summary>Total encoded byte length of a Commit record.</summary>
    internal const int CommitRecordLength = FramingOverhead + sizeof(int);

    /// <summary>Total encoded byte length of a Trim record.</summary>
    internal const int TrimRecordLength = FramingOverhead + sizeof(long);

    /// <summary>
    /// Returns the total encoded byte length of a Data record whose
    /// payload is <paramref name="payloadLength"/> bytes long.
    /// </summary>
    internal static int DataRecordLength(int payloadLength) =>
        FramingOverhead + DataBodyPrefix + payloadLength;

    /// <summary>
    /// Writes a Data record for <paramref name="offset"/> /
    /// <paramref name="payload"/> into <paramref name="destination"/> and
    /// returns the number of bytes written. The destination must have at
    /// least <see cref="DataRecordLength(int)"/> bytes of room.
    /// </summary>
    internal static int WriteDataRecord(Span<byte> destination, long offset, ReadOnlySpan<byte> payload)
    {
        var bodyLength = DataBodyPrefix + payload.Length;
        WriteHeader(destination, RecordTypeData, bodyLength);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(HeaderLength, sizeof(long)), offset);
        payload.CopyTo(destination.Slice(DataPayloadOffset, payload.Length));
        WriteChecksum(destination, bodyLength);
        return FramingOverhead + bodyLength;
    }

    /// <summary>
    /// Writes a Commit record covering <paramref name="count"/> preceding
    /// Data records into <paramref name="destination"/> and returns the
    /// number of bytes written.
    /// </summary>
    internal static int WriteCommitRecord(Span<byte> destination, int count)
    {
        const int bodyLength = sizeof(int);
        WriteHeader(destination, RecordTypeCommit, bodyLength);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(HeaderLength, sizeof(int)), count);
        WriteChecksum(destination, bodyLength);
        return CommitRecordLength;
    }

    /// <summary>
    /// Writes a Trim record for <paramref name="throughOffsetInclusive"/>
    /// into <paramref name="destination"/> and returns the number of
    /// bytes written.
    /// </summary>
    internal static int WriteTrimRecord(Span<byte> destination, long throughOffsetInclusive)
    {
        const int bodyLength = sizeof(long);
        WriteHeader(destination, RecordTypeTrim, bodyLength);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(HeaderLength, sizeof(long)), throughOffsetInclusive);
        WriteChecksum(destination, bodyLength);
        return TrimRecordLength;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteHeader(Span<byte> destination, byte recordType, int bodyLength)
    {
        destination[0] = recordType;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(sizeof(byte), sizeof(int)), bodyLength);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteChecksum(Span<byte> destination, int bodyLength)
    {
        var checksumOffset = HeaderLength + bodyLength;
        var checksum = Crc32.HashToUInt32(destination.Slice(0, checksumOffset));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(checksumOffset, ChecksumLength), checksum);
    }
}
