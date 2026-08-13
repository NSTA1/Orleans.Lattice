using System.Buffers.Binary;
using System.IO.Hashing;

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

    /// <summary>
    /// Fixed per-record framing overhead in bytes: the 1-byte type tag,
    /// the 4-byte body length, and the trailing 4-byte CRC-32.
    /// </summary>
    internal const int FramingOverhead = 1 + 4 + 4;

    /// <summary>Byte length of a Data record body's fixed offset prefix.</summary>
    internal const int DataBodyPrefix = sizeof(long);

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
        var bodyLen = DataBodyPrefix + payload.Length;
        destination[0] = RecordTypeData;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(1, 4), bodyLen);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(5, 8), offset);
        payload.CopyTo(destination.Slice(13, payload.Length));
        var crcOver = destination.Slice(0, 5 + bodyLen);
        var crc = Crc32.HashToUInt32(crcOver);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(5 + bodyLen, 4), crc);
        return FramingOverhead + bodyLen;
    }

    /// <summary>
    /// Writes a Commit record covering <paramref name="count"/> preceding
    /// Data records into <paramref name="destination"/> and returns the
    /// number of bytes written.
    /// </summary>
    internal static int WriteCommitRecord(Span<byte> destination, int count)
    {
        const int bodyLen = sizeof(int);
        destination[0] = RecordTypeCommit;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(1, 4), bodyLen);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(5, 4), count);
        var crc = Crc32.HashToUInt32(destination.Slice(0, 5 + bodyLen));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(5 + bodyLen, 4), crc);
        return CommitRecordLength;
    }

    /// <summary>
    /// Writes a Trim record for <paramref name="throughOffsetInclusive"/>
    /// into <paramref name="destination"/> and returns the number of
    /// bytes written.
    /// </summary>
    internal static int WriteTrimRecord(Span<byte> destination, long throughOffsetInclusive)
    {
        const int bodyLen = sizeof(long);
        destination[0] = RecordTypeTrim;
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(1, 4), bodyLen);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(5, 8), throughOffsetInclusive);
        var crc = Crc32.HashToUInt32(destination.Slice(0, 5 + bodyLen));
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(5 + bodyLen, 4), crc);
        return TrimRecordLength;
    }
}
