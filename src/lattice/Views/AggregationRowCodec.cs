using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Reserved-key layout and binary (de)serialisation for an aggregation view's
/// internal rows, which live in the <c>view-{name}</c> tree under a reserved NUL
/// (<c>\u0000</c>) prefix that can never collide with a materialised group key
/// (group keys are forbidden from beginning with NUL). Three row families share
/// the tree alongside the bare-keyed materialised group values:
/// <list type="bullet">
/// <item><b>Membership</b> (<c>\u0000m{sourceKey}</c>) - the group and value a source key last contributed; the "read before write" retraction pointer.</item>
/// <item><b>Accumulator</b> (<c>\u0000a{groupKey}\u0000{slot}</c>) - the running count and sum of a group shard (count / sum kinds).</item>
/// <item><b>Inverse</b> (<c>\u0000i{groupKey}\u0000{slot}</c>) - the per-source-key contributions of a group shard (min / max / set-union kinds).</item>
/// </list>
/// The payloads never travel the wire (they are opaque bytes in the view tree),
/// so they use a compact manual encoding rather than an Orleans serializer.
/// </summary>
internal static class AggregationRowCodec
{
    /// <summary>The reserved NUL prefix every internal row key begins with.</summary>
    internal const string ReservedPrefix = "\u0000";

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="groupKey"/> falls in the
    /// reserved region and must not be materialised: an empty key, or one beginning
    /// with the reserved NUL (<c>\u0000</c>) prefix. A materialised value under such
    /// a key would sort below <see cref="FirstNonReservedKey"/> - invisible to every
    /// view read - and could collide with an internal accumulator / inverse /
    /// membership row. The applier rejects a contribution whose group key is
    /// reserved rather than corrupt the view silently.
    /// </summary>
    internal static bool IsReservedGroupKey(string groupKey) =>
        groupKey.Length == 0 || groupKey[0] == '\u0000';

    /// <summary>
    /// The "logically empty" sentinel an internal row carries when it has been
    /// retracted to nothing (an accumulator slot whose count reached 0, or a
    /// retracted membership row). Because the all-or-nothing atomic flip
    /// (<see cref="IAggregationViewStore.SetManyAtomicAsync"/>) can only
    /// <c>Set</c> - it cannot delete - a row that needs to vanish atomically with
    /// its siblings is instead flipped to this sentinel, and the read path
    /// (<see cref="IsEmpty"/>) treats it as absent. A single byte (length 1) can
    /// never collide with a real row: accumulator rows are exactly 16 bytes,
    /// membership rows at least 10, and inverse rows at least 4. The applier
    /// opportunistically deletes the sentinel after materialising, so it never
    /// leaks past one drain pass. This value is append-only and wire-compatible
    /// with the existing Phase 3 row formats (it is a new value family, not a
    /// change to any existing layout).
    /// </summary>
    private static readonly byte[] EmptySentinel = [0x00];

    /// <summary>Returns the "logically empty" sentinel value (see remarks on the codec's empty-row handling).</summary>
    internal static byte[] EmptyRow() => [0x00];

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="bytes"/> is the
    /// "logically empty" sentinel - a retracted accumulator slot or membership
    /// row that the atomic flip flipped to empty rather than deleting. Callers
    /// treat such a row as absent.
    /// </summary>
    internal static bool IsEmpty(byte[] bytes) =>
        bytes.Length == EmptySentinel.Length && bytes[0] == EmptySentinel[0];

    /// <summary>
    /// The lowest key a materialised group value can take: reads of the
    /// view-facing surface start here to skip the reserved-prefixed internal rows
    /// (all of which sort below this because NUL is the lowest character).
    /// </summary>
    internal const string FirstNonReservedKey = "\u0001";

    /// <summary>Returns the membership row key for <paramref name="sourceKey"/>.</summary>
    internal static string MembershipKey(string sourceKey) => "\u0000m" + sourceKey;

    /// <summary>Returns the accumulator row key for a group shard.</summary>
    internal static string AccumulatorKey(string groupKey, int slot) => "\u0000a" + groupKey + "\u0000" + slot.ToString();

    /// <summary>Returns the inverse-contribution row key for a group shard.</summary>
    internal static string InverseKey(string groupKey, int slot) => "\u0000i" + groupKey + "\u0000" + slot.ToString();

    /// <summary>Returns the fold-contribution row key for a group shard (custom fold views).</summary>
    internal static string FoldInverseKey(string groupKey, int slot) => "\u0000f" + groupKey + "\u0000" + slot.ToString();

    /// <summary>
    /// Maps a source key to its accumulator shard in <c>[0, fanout)</c> using a
    /// process-independent hash so every cluster shards identically.
    /// </summary>
    internal static int Slot(string sourceKey, int fanout)
    {
        if (fanout <= 1)
        {
            return 0;
        }

        // Hash the key from a stack (or pooled, for long keys) UTF-8 buffer
        // instead of allocating a fresh byte[] per call. This is the same
        // idiom LatticeSharding / ShardMap.GetVirtualSlot use, and it runs on
        // the view-write hot path: every source mutation feeding a count / sum
        // aggregation view routes through here (once or twice per contribution)
        // to pick its accumulator shard. The XxHash32 input bytes are identical,
        // so the resulting slot is unchanged for every key.
        var maxByteCount = Encoding.UTF8.GetMaxByteCount(sourceKey.Length);
        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = Encoding.UTF8.GetBytes(sourceKey, buffer);
            var hash = XxHash32.HashToUInt32(buffer[..written]);
            return (int)(hash % (uint)fanout);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>Encodes a membership row.</summary>
    internal static byte[] EncodeMembership(in MembershipRow row)
    {
        // Emit the row directly into an exact-size array instead of a
        // MemoryStream + BinaryWriter (which allocate a growable backing
        // buffer, a writer, and an encoder per call, then a final ToArray
        // copy). Every source mutation feeding a group-by view writes exactly
        // one membership row through here, so this runs on the view-write hot
        // path. The output is byte-for-byte identical to the BinaryWriter
        // encoding it replaces (7-bit length-prefixed UTF-8 strings, a single
        // bool byte, little-endian double), so persisted rows stay readable.
        var hasMember = row.Member is not null;
        var size = Utf8Size(row.GroupKey) + sizeof(bool) + sizeof(double)
            + (hasMember ? Utf8Size(row.Member!) : 0);
        var buffer = new byte[size];
        var writer = new RowWriter(buffer);
        writer.WriteString(row.GroupKey);
        writer.WriteBool(hasMember);
        writer.WriteDouble(row.Numeric);
        if (hasMember)
        {
            writer.WriteString(row.Member!);
        }

        return buffer;
    }

    /// <summary>Decodes a membership row produced by <see cref="EncodeMembership"/>.</summary>
    internal static MembershipRow DecodeMembership(byte[] bytes)
    {
        // Read directly from the row span via RowReader instead of a per-call
        // MemoryStream + BinaryReader (which allocate a stream, a reader, and a
        // decode char buffer on every read). This is the exact inverse of the
        // RowWriter encode path and runs on the view read/drain hot path: every
        // group-by contribution reads its source key's prior membership row here
        // to retract it. The byte layout parsed is identical to the BinaryReader
        // encoding (7-bit length-prefixed UTF-8 strings, a single bool byte,
        // little-endian double), so persisted rows read back unchanged.
        var reader = new RowReader(bytes);
        var groupKey = reader.ReadString();
        var hasMember = reader.ReadBool();
        var numeric = reader.ReadDouble();
        string? member = hasMember ? reader.ReadString() : null;
        return new MembershipRow(groupKey, numeric, member);
    }

    /// <summary>Encodes an accumulator row.</summary>
    internal static byte[] EncodeAccumulator(in AccumulatorRow row)
    {
        var buffer = new byte[sizeof(long) + sizeof(double)];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, row.Count);
        System.Buffers.Binary.BinaryPrimitives.WriteDoubleBigEndian(buffer.AsSpan(sizeof(long)), row.Sum);
        return buffer;
    }

    /// <summary>Decodes an accumulator row produced by <see cref="EncodeAccumulator"/>.</summary>
    internal static AccumulatorRow DecodeAccumulator(byte[] bytes)
    {
        var count = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(bytes);
        var sum = System.Buffers.Binary.BinaryPrimitives.ReadDoubleBigEndian(bytes.AsSpan(sizeof(long)));
        return new AccumulatorRow(count, sum);
    }

    /// <summary>Encodes an inverse-contribution row (a source-key to contribution map).</summary>
    internal static byte[] EncodeInverse(IReadOnlyDictionary<string, MemberEntry> entries)
    {
        // See EncodeMembership: this replaces a per-call MemoryStream +
        // BinaryWriter with a single sizing pass over the entries followed by
        // a direct write into an exact-size array. The dictionary is not
        // mutated between the two passes, so both enumerate in the same order
        // and the bytes are identical to the BinaryWriter encoding.
        var size = sizeof(int);
        foreach (var (sourceKey, entry) in entries)
        {
            size += Utf8Size(sourceKey) + sizeof(bool) + sizeof(double)
                + (entry.Member is not null ? Utf8Size(entry.Member) : 0);
        }

        var buffer = new byte[size];
        var writer = new RowWriter(buffer);
        writer.WriteInt32(entries.Count);
        foreach (var (sourceKey, entry) in entries)
        {
            writer.WriteString(sourceKey);
            var hasMember = entry.Member is not null;
            writer.WriteBool(hasMember);
            writer.WriteDouble(entry.Numeric);
            if (hasMember)
            {
                writer.WriteString(entry.Member!);
            }
        }

        return buffer;
    }

    /// <summary>Decodes an inverse-contribution row produced by <see cref="EncodeInverse"/>.</summary>
    internal static Dictionary<string, MemberEntry> DecodeInverse(byte[] bytes)
    {
        // See DecodeMembership: a RowReader span walk replaces the per-call
        // MemoryStream + BinaryReader. This is the hottest decode of the three -
        // every min / max / set-union group-shard update reads its inverse row
        // here, folds the contribution, and re-encodes it - so it removes a
        // stream + reader + decode buffer on each such view mutation. The parsed
        // layout is byte-for-byte the BinaryReader encoding.
        var reader = new RowReader(bytes);
        var count = reader.ReadInt32();
        var map = new Dictionary<string, MemberEntry>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var sourceKey = reader.ReadString();
            var hasMember = reader.ReadBool();
            var numeric = reader.ReadDouble();
            string? member = hasMember ? reader.ReadString() : null;
            map[sourceKey] = new MemberEntry(numeric, member);
        }

        return map;
    }

    /// <summary>The group and value a source key last contributed.</summary>
    /// <param name="GroupKey">The group the source key last belonged to.</param>
    /// <param name="Numeric">The numeric the source key last contributed (sum / min / max).</param>
    /// <param name="Member">The member the source key last contributed (set-union), or <see langword="null"/>.</param>
    internal readonly record struct MembershipRow(string GroupKey, double Numeric, string? Member);

    /// <summary>A group shard's running count and sum.</summary>
    /// <param name="Count">The number of live source keys in the shard.</param>
    /// <param name="Sum">The running sum of the shard's numeric contributions.</param>
    internal readonly record struct AccumulatorRow(long Count, double Sum);

    /// <summary>A single source key's contribution inside an inverse row.</summary>
    /// <param name="Numeric">The numeric contributed (min / max).</param>
    /// <param name="Member">The member contributed (set-union), or <see langword="null"/>.</param>
    internal readonly record struct MemberEntry(double Numeric, string? Member);

    /// <summary>Encodes a fold-contribution row (a source-key to member-value map for a custom fold group shard).</summary>
    internal static byte[] EncodeFoldInverse(IReadOnlyDictionary<string, FoldMember> entries)
    {
        // See EncodeMembership: a single sizing pass then a direct write into
        // an exact-size array, replacing the per-call MemoryStream +
        // BinaryWriter. The value bytes are written raw (no length prefix of
        // their own beyond the explicit int32 length), matching the prior
        // BinaryWriter.Write(byte[]) call byte-for-byte.
        var size = sizeof(int);
        foreach (var (sourceKey, entry) in entries)
        {
            size += Utf8Size(sourceKey) + sizeof(long) + sizeof(int)
                + sizeof(int) + entry.Value.Length;
        }

        var buffer = new byte[size];
        var writer = new RowWriter(buffer);
        writer.WriteInt32(entries.Count);
        foreach (var (sourceKey, entry) in entries)
        {
            writer.WriteString(sourceKey);
            writer.WriteInt64(entry.Timestamp.WallClockTicks);
            writer.WriteInt32(entry.Timestamp.Counter);
            writer.WriteInt32(entry.Value.Length);
            writer.WriteRaw(entry.Value);
        }

        return buffer;
    }

    /// <summary>Decodes a fold-contribution row produced by <see cref="EncodeFoldInverse"/>.</summary>
    internal static Dictionary<string, FoldMember> DecodeFoldInverse(byte[] bytes)
    {
        // See DecodeMembership: a RowReader span walk replaces the per-call
        // MemoryStream + BinaryReader on the custom-fold group-shard read path.
        // The raw value bytes are read as an exact-length slice copy, matching
        // BinaryReader.ReadBytes(length) byte-for-byte.
        var reader = new RowReader(bytes);
        var count = reader.ReadInt32();
        var map = new Dictionary<string, FoldMember>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var sourceKey = reader.ReadString();
            var ticks = reader.ReadInt64();
            var counter = reader.ReadInt32();
            var length = reader.ReadInt32();
            var value = reader.ReadBytes(length);
            map[sourceKey] = new FoldMember(value, new HybridLogicalClock { WallClockTicks = ticks, Counter = counter });
        }

        return map;
    }

    /// <summary>A single source key's contribution inside a fold-inverse row.</summary>
    /// <param name="Value">The source value bytes the source key last contributed.</param>
    /// <param name="Timestamp">The source entry HLC, used to order the re-fold.</param>
    internal readonly record struct FoldMember(byte[] Value, HybridLogicalClock Timestamp);

    /// <summary>
    /// Returns the number of bytes <see cref="RowWriter.WriteString"/> emits for
    /// <paramref name="value"/>: a 7-bit-encoded UTF-8 byte-count prefix followed
    /// by the UTF-8 bytes, exactly as <see cref="BinaryWriter.Write(string)"/> does.
    /// </summary>
    private static int Utf8Size(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        return SevenBitSize(byteCount) + byteCount;
    }

    /// <summary>Returns the number of bytes a 7-bit-encoded <paramref name="value"/> occupies.</summary>
    private static int SevenBitSize(int value)
    {
        var v = (uint)value;
        var size = 1;
        while (v >= 0x80)
        {
            size++;
            v >>= 7;
        }

        return size;
    }

    /// <summary>
    /// A forward-only cursor that writes the same byte layout as
    /// <see cref="BinaryWriter"/> with <see cref="Encoding.UTF8"/> (7-bit
    /// length-prefixed UTF-8 strings, single-byte bools, little-endian numerics,
    /// raw byte spans) directly into a caller-owned span, so a row can be
    /// encoded into an exact-size array with no intermediate stream or writer.
    /// </summary>
    private ref struct RowWriter(Span<byte> buffer)
    {
        private readonly Span<byte> _buffer = buffer;
        private int _pos;

        public void WriteBool(bool value) => _buffer[_pos++] = value ? (byte)1 : (byte)0;

        public void WriteInt32(int value)
        {
            BinaryPrimitives.WriteInt32LittleEndian(_buffer[_pos..], value);
            _pos += sizeof(int);
        }

        public void WriteInt64(long value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(_buffer[_pos..], value);
            _pos += sizeof(long);
        }

        public void WriteDouble(double value)
        {
            BinaryPrimitives.WriteDoubleLittleEndian(_buffer[_pos..], value);
            _pos += sizeof(double);
        }

        public void WriteRaw(ReadOnlySpan<byte> value)
        {
            value.CopyTo(_buffer[_pos..]);
            _pos += value.Length;
        }

        public void WriteString(string value)
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            Write7BitEncodedInt(byteCount);
            Encoding.UTF8.GetBytes(value, _buffer[_pos..]);
            _pos += byteCount;
        }

        private void Write7BitEncodedInt(int value)
        {
            var v = (uint)value;
            while (v >= 0x80)
            {
                _buffer[_pos++] = (byte)(v | 0x80);
                v >>= 7;
            }

            _buffer[_pos++] = (byte)v;
        }
    }

    /// <summary>
    /// A forward-only cursor that reads the same byte layout
    /// <see cref="RowWriter"/> emits (7-bit length-prefixed UTF-8 strings,
    /// single-byte bools, little-endian numerics, raw byte slices) directly from
    /// a caller-owned span, so a row can be decoded with no intermediate
    /// <see cref="MemoryStream"/> or <see cref="BinaryReader"/> (and no reader
    /// decode buffer) per call. It is the exact inverse of <see cref="RowWriter"/>
    /// and parses the identical format <see cref="BinaryReader"/> with
    /// <see cref="Encoding.UTF8"/> produced, so previously persisted rows read
    /// back unchanged.
    /// </summary>
    private ref struct RowReader(ReadOnlySpan<byte> buffer)
    {
        private readonly ReadOnlySpan<byte> _buffer = buffer;
        private int _pos;

        public bool ReadBool() => _buffer[_pos++] != 0;

        public int ReadInt32()
        {
            var value = BinaryPrimitives.ReadInt32LittleEndian(_buffer[_pos..]);
            _pos += sizeof(int);
            return value;
        }

        public long ReadInt64()
        {
            var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer[_pos..]);
            _pos += sizeof(long);
            return value;
        }

        public double ReadDouble()
        {
            var value = BinaryPrimitives.ReadDoubleLittleEndian(_buffer[_pos..]);
            _pos += sizeof(double);
            return value;
        }

        public byte[] ReadBytes(int count)
        {
            var value = _buffer.Slice(_pos, count).ToArray();
            _pos += count;
            return value;
        }

        public string ReadString()
        {
            var byteCount = Read7BitEncodedInt();
            var value = Encoding.UTF8.GetString(_buffer.Slice(_pos, byteCount));
            _pos += byteCount;
            return value;
        }

        private int Read7BitEncodedInt()
        {
            // Mirrors BinaryReader.Read7BitEncodedInt: low-order 7 bits per byte,
            // continuation flag in the high bit, at most five bytes for a 32-bit
            // value. A malformed prefix is rejected exactly as BinaryReader does.
            var result = 0;
            var shift = 0;
            while (shift < 5 * 7)
            {
                var b = _buffer[_pos++];
                result |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0)
                {
                    return result;
                }

                shift += 7;
            }

            throw new FormatException("The 7-bit encoded length prefix is malformed.");
        }
    }
}
