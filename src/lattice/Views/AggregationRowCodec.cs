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
/// The payloads are opaque bytes in the view tree and use a compact manual
/// encoding rather than an Orleans serializer. They are <b>not</b> purely local:
/// under <see cref="LatticeViewReplicationMode.ShipView"/> the view tree itself is
/// replicated, so a row can arrive from a remote peer and reach these decoders.
/// Every decode is therefore defensive - lengths and entry counts are bounded
/// against the bytes actually present before anything is sized from them, and a
/// malformed row raises <see cref="InvalidDataException"/> rather than an
/// index-out-of-range fault or an unbounded allocation.
/// </summary>
internal static class AggregationRowCodec
{
    /// <summary>
    /// The smallest number of bytes an inverse entry can occupy: an empty
    /// length-prefixed source key (1), the has-member flag (1), and the numeric (8).
    /// Used to bound a wire-supplied entry count before pre-sizing.
    /// </summary>
    private const int MinimumInverseEntrySize = 1 + sizeof(bool) + sizeof(double);

    /// <summary>
    /// The smallest number of bytes a fold-inverse entry can occupy: an empty
    /// length-prefixed source key (1), the HLC wall clock (8) and counter (4), and
    /// a zero-length value's length prefix (4).
    /// Used to bound a wire-supplied entry count before pre-sizing.
    /// </summary>
    private const int MinimumFoldInverseEntrySize = 1 + sizeof(long) + sizeof(int) + sizeof(int);

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

    /// <summary>
    /// Decodes only the fields of a membership row that the retraction path
    /// consumes - the group the source key last belonged to and the numeric it
    /// last contributed - stepping over the member string rather than
    /// transcoding it.
    /// <para>
    /// Every contribute and every retract reads the source key's prior
    /// membership row here to retract it, and each of those readers uses the row
    /// for exactly two things: to find the group shard to decrement
    /// (<c>GroupKey</c>) and to subtract the value it contributed
    /// (<c>Numeric</c>). No caller reads the member. On a set-union view - the
    /// one kind whose membership rows carry a member at all - decoding it
    /// allocated a fresh string per membership read that was dropped
    /// unexamined.
    /// </para>
    /// <para>
    /// The bytes walked, and the order and strictness of the validation, are
    /// identical to <see cref="DecodeMembership"/>: the member's length prefix
    /// is still read and still bounded against the row, so a truncated or
    /// corrupt row raises <see cref="InvalidDataException"/> at the same byte
    /// whether or not the caller wanted the string.
    /// </para>
    /// </summary>
    internal static MembershipHead DecodeMembershipHead(byte[] bytes)
    {
        var reader = new RowReader(bytes);
        var groupKey = reader.ReadString();
        var hasMember = reader.ReadBool();
        var numeric = reader.ReadDouble();
        if (hasMember)
        {
            reader.SkipString();
        }

        return new MembershipHead(groupKey, numeric);
    }

    /// <summary>
    /// Decodes a membership row produced by <see cref="EncodeMembership"/>.
    /// <para>
    /// The applier reads membership rows through
    /// <see cref="DecodeMembershipHead"/>, which skips the member no caller
    /// reads. This full decode is retained as the round-trip inverse of
    /// <see cref="EncodeMembership"/> and as the oracle the head read's parity
    /// tests compare against; do not delete it as unused.
    /// </para>
    /// </summary>
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

    /// <summary>
    /// A zero-entry inverse / fold-inverse row: the four-byte little-endian
    /// entry count and nothing else. A splice against an absent or
    /// empty-sentinel shard starts here, so the "first entry in a new shard"
    /// case goes through exactly the same encode as every other mutation
    /// instead of needing a second, differently-tested path. Compiles to a
    /// span over static data, so it costs no allocation.
    /// </summary>
    internal static ReadOnlySpan<byte> EmptyEntryRow => [0, 0, 0, 0];

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
        var count = reader.ReadBoundedCount(MinimumInverseEntrySize);
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

    /// <summary>
    /// The subset of a membership row the retraction path actually consumes.
    /// See <see cref="DecodeMembershipHead"/> for why the member is absent.
    /// </summary>
    /// <param name="GroupKey">The group the source key last belonged to.</param>
    /// <param name="Numeric">The numeric the source key last contributed (sum / min / max).</param>
    internal readonly record struct MembershipHead(string GroupKey, double Numeric);

    /// <summary>A group shard's running count and sum.</summary>
    /// <param name="Count">The number of live source keys in the shard.</param>
    /// <param name="Sum">The running sum of the shard's numeric contributions.</param>
    internal readonly record struct AccumulatorRow(long Count, double Sum);

    /// <summary>A single source key's contribution inside an inverse row.</summary>
    /// <param name="Numeric">The numeric contributed (min / max).</param>
    /// <param name="Member">The member contributed (set-union), or <see langword="null"/>.</param>
    internal readonly record struct MemberEntry(double Numeric, string? Member);

    /// <summary>
    /// Rewrites an encoded inverse row so that <paramref name="sourceKey"/>'s
    /// entry becomes <paramref name="add"/> - or disappears when
    /// <paramref name="add"/> is <see langword="null"/> - copying every other
    /// entry through as raw bytes. Returns <see langword="null"/> when no entry
    /// would survive, which the caller turns into a delete.
    /// <para>
    /// This is the read-modify-write counterpart of <see cref="InverseRowScan"/>.
    /// A min / max / set-union contribution mutates exactly one entry of one
    /// shard row, but it did so by decoding the whole row into a
    /// <see cref="Dictionary{TKey,TValue}"/> - a fresh source-key string and a
    /// hash insert for every entry in the shard - assigning or removing one key,
    /// and then re-encoding every entry, transcoding each of those same source
    /// keys back to UTF-8. Both halves are proportional to the shard's size for a
    /// change that is constant. The splice walks the encoded bytes twice (once to
    /// size the result and once to fill it), transcodes only the key being
    /// spliced, and copies the untouched entries verbatim, so nothing but the
    /// changed entry is ever materialised.
    /// </para>
    /// <para>
    /// <b>Equivalence.</b> For a row this codec produced - which cannot carry a
    /// duplicate key, having been encoded from a dictionary - the output is
    /// byte-for-byte what decode, mutate, re-encode produces, including entry
    /// order: <see cref="Dictionary{TKey,TValue}"/> enumerates in entry-index
    /// order, so assigning an existing key keeps its slot (the splice replaces in
    /// place), assigning an absent key appends (the splice appends), and removing
    /// leaves the rest in order (the splice elides the run). A hostile row
    /// carrying the spliced key twice is handled the way the dictionary handles
    /// it - the first occurrence is replaced and later ones elided, and a removal
    /// elides them all - so the two agree there too. A hostile row carrying some
    /// OTHER key twice is the one residual: the splice copies both occurrences
    /// through where a re-encode would have collapsed them. That is inert,
    /// because every reader of the row either decodes it (collapsing the pair
    /// again, to the same last-wins value) or folds it into an extremum or a set,
    /// which a repeat cannot change.
    /// </para>
    /// <para>
    /// <b>Validation.</b> The sizing walk performs exactly the reads
    /// <see cref="DecodeInverse"/> performs, in the same order, so a truncated or
    /// corrupt row raises <see cref="InvalidDataException"/> at the same byte.
    /// </para>
    /// </summary>
    /// <param name="row">A row produced by <see cref="EncodeInverse"/>.</param>
    /// <param name="sourceKey">The source key whose entry is being spliced.</param>
    /// <param name="add">The replacement entry, or <see langword="null"/> to remove.</param>
    /// <param name="moveToEnd">
    /// When <see langword="true"/>, an existing entry for <paramref name="sourceKey"/>
    /// is elided rather than replaced where it sits, and
    /// <paramref name="add"/> is appended after the surviving entries. This is
    /// the byte-for-byte result of splicing the key out and then splicing it back
    /// in - the removal elides it, and the re-add finds the key absent and
    /// appends - which is exactly what
    /// <c>AggregationApplier.ContributeInverseAsync</c> used to produce with two
    /// store round trips when a source key was re-contributed to the group it
    /// already belonged to. Fusing the pair keeps the row identical and halves
    /// the read-modify-write. Ignored when <paramref name="add"/> is
    /// <see langword="null"/>, a removal having no entry to position.
    /// </param>
    internal static byte[]? SpliceInverse(ReadOnlySpan<byte> row, string sourceKey, MemberEntry? add, bool moveToEnd = false)
    {
        byte[]? rented = null;
        var maxKeyBytes = Encoding.UTF8.GetMaxByteCount(sourceKey.Length);
        Span<byte> keyBuffer = maxKeyBytes <= 256
            ? stackalloc byte[256]
            : (rented = ArrayPool<byte>.Shared.Rent(maxKeyBytes));
        try
        {
            var key = keyBuffer[..Encoding.UTF8.GetBytes(sourceKey, keyBuffer)];

            // Pass one: validate the row exactly as DecodeInverse would, and
            // measure the spliced result without writing anything.
            var reader = new RowReader(row);
            var count = reader.ReadBoundedCount(MinimumInverseEntrySize);
            var entriesStart = reader.Position;
            var matchCount = 0;
            var matchBytes = 0;
            var firstMatchStart = 0;
            var firstMatchEnd = 0;
            for (var i = 0; i < count; i++)
            {
                var start = reader.Position;
                var matched = reader.ReadStringBytes().SequenceEqual(key);
                var hasMember = reader.ReadBool();
                reader.ReadDouble();
                if (hasMember)
                {
                    reader.SkipString();
                }

                if (matched)
                {
                    matchCount++;
                    matchBytes += reader.Position - start;
                    if (matchCount == 1)
                    {
                        firstMatchStart = start;
                        firstMatchEnd = reader.Position;
                    }
                }
            }

            var entriesEnd = reader.Position;
            var hasMember2 = add is { Member: not null };
            var addedSize = add is { } entry
                ? Utf8Size(sourceKey) + sizeof(bool) + sizeof(double) + (hasMember2 ? Utf8Size(entry.Member!) : 0)
                : 0;
            var newCount = count - matchCount + (add is null ? 0 : 1);
            if (newCount == 0)
            {
                return null;
            }

            var buffer = new byte[sizeof(int) + (entriesEnd - entriesStart) - matchBytes + addedSize];
            var writer = new RowWriter(buffer);
            writer.WriteInt32(newCount);

            // Pass two, block form. A row this codec produced carries a key at
            // most once, so the surviving entries are at most two contiguous
            // runs - those before the match and those after it - and pass one
            // already delimited both. Copying the runs as blocks emits exactly
            // the bytes the entry-by-entry walk below emits, without re-reading
            // a length prefix, re-comparing a key, or re-parsing a field. The
            // general walk is kept for the hostile row that repeats the key.
            if (matchCount <= 1)
            {
                if (matchCount == 0)
                {
                    writer.WriteRaw(row[entriesStart..entriesEnd]);
                }
                else
                {
                    writer.WriteRaw(row[entriesStart..firstMatchStart]);
                    if (add is { } inPlace && !moveToEnd)
                    {
                        WriteInverseEntry(ref writer, sourceKey, inPlace);
                    }

                    writer.WriteRaw(row[firstMatchEnd..entriesEnd]);
                }

                if (add is { } tail && (matchCount == 0 || moveToEnd))
                {
                    WriteInverseEntry(ref writer, sourceKey, tail);
                }

                return buffer;
            }

            // Pass two: copy every surviving entry through as raw bytes, writing
            // the replacement in the first matched entry's place.
            reader = new RowReader(row);
            reader.ReadBoundedCount(MinimumInverseEntrySize);
            var written = false;
            for (var i = 0; i < count; i++)
            {
                var start = reader.Position;
                var matched = reader.ReadStringBytes().SequenceEqual(key);
                var hasMember = reader.ReadBool();
                reader.ReadDouble();
                if (hasMember)
                {
                    reader.SkipString();
                }

                if (!matched)
                {
                    writer.WriteRaw(row[start..reader.Position]);
                    continue;
                }

                if (add is { } replacement && !written && !moveToEnd)
                {
                    WriteInverseEntry(ref writer, sourceKey, replacement);
                    written = true;
                }
            }

            if (add is { } appended && !written)
            {
                WriteInverseEntry(ref writer, sourceKey, appended);
            }

            return buffer;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static void WriteInverseEntry(ref RowWriter writer, string sourceKey, in MemberEntry entry)
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

    /// <summary>
    /// A forward-only, allocation-free cursor over an inverse row for a reader
    /// that folds the row's contributions and discards the source keys.
    /// <para>
    /// <see cref="DecodeInverse"/> exists for the read-modify-write path, which
    /// genuinely needs a keyed map. The group re-materialise path does not: it
    /// reduces every shard's entries to one extremum or one member set and never
    /// looks a source key up, yet it paid for a whole
    /// <see cref="Dictionary{TKey,TValue}"/> per shard plus a freshly-decoded
    /// string for every source key in it, then dropped the lot. This cursor walks
    /// the identical byte layout and materialises nothing: a source key is
    /// stepped over with <see cref="RowReader.SkipString"/> rather than
    /// transcoded.
    /// </para>
    /// <para>
    /// The member string is the caller's choice rather than a flag on the cursor,
    /// so neither walk carries the other's branch: <see cref="MoveNextNumeric"/>
    /// skips the member (min / max, which read only the numeric) and
    /// <see cref="MoveNext"/> materialises it (set-union, which unions the
    /// members). Both validate exactly what <see cref="DecodeInverse"/> validates,
    /// so a truncated or corrupt row still raises
    /// <see cref="InvalidDataException"/> at the same byte.
    /// </para>
    /// </summary>
    internal ref struct InverseRowScan
    {
        private RowReader _reader;
        private int _remaining;

        /// <summary>Opens a cursor over an encoded inverse row.</summary>
        /// <param name="bytes">A row produced by <see cref="EncodeInverse"/>.</param>
        internal InverseRowScan(ReadOnlySpan<byte> bytes)
        {
            _reader = new RowReader(bytes);
            _remaining = _reader.ReadBoundedCount(MinimumInverseEntrySize);
        }

        /// <summary>The number of entries not yet walked.</summary>
        public readonly int Remaining => _remaining;

        /// <summary>The numeric the current entry contributed.</summary>
        public double Numeric { get; private set; }

        /// <summary>
        /// The member the current entry contributed, or <see langword="null"/>
        /// when it contributed none or the walk skipped it.
        /// </summary>
        public string? Member { get; private set; }

        /// <summary>
        /// Advances to the next entry, reading its numeric and stepping over both
        /// its source key and its member. For min / max, which consume neither.
        /// </summary>
        /// <returns><see langword="true"/> when an entry was read.</returns>
        public bool MoveNextNumeric()
        {
            if (_remaining == 0)
            {
                return false;
            }

            _remaining--;
            _reader.SkipString();
            var hasMember = _reader.ReadBool();
            Numeric = _reader.ReadDouble();
            if (hasMember)
            {
                _reader.SkipString();
            }

            Member = null;
            return true;
        }

        /// <summary>
        /// Advances to the next entry, reading its numeric and its member and
        /// stepping over its source key. For set-union, which consumes members.
        /// </summary>
        /// <returns><see langword="true"/> when an entry was read.</returns>
        public bool MoveNext()
        {
            if (_remaining == 0)
            {
                return false;
            }

            _remaining--;
            _reader.SkipString();
            var hasMember = _reader.ReadBool();
            Numeric = _reader.ReadDouble();
            Member = hasMember ? _reader.ReadString() : null;
            return true;
        }
    }

    /// <summary>
    /// Encodes a fold-contribution row (a source-key to member-value map for a
    /// custom fold group shard).
    /// <para>
    /// The applier mutates these rows through <see cref="SpliceFoldInverse"/>
    /// rather than re-encoding a whole map. This encoder, and the
    /// <see cref="DecodeFoldInverse"/> below it, are retained as the pair the
    /// splice's byte-for-byte parity tests and benchmark baselines are defined
    /// against; do not delete them as unused.
    /// </para>
    /// </summary>
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
        var count = reader.ReadBoundedCount(MinimumFoldInverseEntrySize);
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
    /// The fold-inverse counterpart of <see cref="SpliceInverse"/>: rewrites an
    /// encoded fold-inverse row so that <paramref name="sourceKey"/>'s entry
    /// becomes <paramref name="add"/>, or disappears when <paramref name="add"/>
    /// is <see langword="null"/>. Returns <see langword="null"/> when no entry
    /// would survive.
    /// <para>
    /// The waste removed here is strictly larger than on the inverse row,
    /// because a fold entry carries an opaque value payload:
    /// <see cref="DecodeFoldInverse"/> allocates a source-key string <i>and</i> a
    /// fresh <see cref="byte"/> array copy of every member's value just to hand
    /// the whole shard back for one key to be assigned or removed, after which
    /// the re-encode copies each of those arrays back out again. The splice
    /// copies the untouched entries' bytes straight from the old row to the new
    /// one, so no member value is ever duplicated onto the heap.
    /// </para>
    /// <para>
    /// The equivalence and validation guarantees are exactly those documented on
    /// <see cref="SpliceInverse"/>.
    /// </para>
    /// </summary>
    /// <param name="row">A row produced by <see cref="EncodeFoldInverse"/>.</param>
    /// <param name="sourceKey">The source key whose entry is being spliced.</param>
    /// <param name="add">The replacement entry, or <see langword="null"/> to remove.</param>
    /// <param name="moveToEnd">
    /// When <see langword="true"/>, an existing entry for <paramref name="sourceKey"/>
    /// is elided rather than replaced in place and <paramref name="add"/> is
    /// appended after the survivors. See the corresponding parameter on
    /// <see cref="SpliceInverse"/>: it makes one splice produce exactly the row
    /// a remove-then-add pair produced, so a same-group re-contribution costs one
    /// store round trip instead of two. Ignored when <paramref name="add"/> is
    /// <see langword="null"/>.
    /// </param>
    internal static byte[]? SpliceFoldInverse(ReadOnlySpan<byte> row, string sourceKey, FoldMember? add, bool moveToEnd = false)
    {
        byte[]? rented = null;
        var maxKeyBytes = Encoding.UTF8.GetMaxByteCount(sourceKey.Length);
        Span<byte> keyBuffer = maxKeyBytes <= 256
            ? stackalloc byte[256]
            : (rented = ArrayPool<byte>.Shared.Rent(maxKeyBytes));
        try
        {
            var key = keyBuffer[..Encoding.UTF8.GetBytes(sourceKey, keyBuffer)];

            var reader = new RowReader(row);
            var count = reader.ReadBoundedCount(MinimumFoldInverseEntrySize);
            var entriesStart = reader.Position;
            var matchCount = 0;
            var matchBytes = 0;
            var firstMatchStart = 0;
            var firstMatchEnd = 0;
            for (var i = 0; i < count; i++)
            {
                var start = reader.Position;
                var matched = reader.ReadStringBytes().SequenceEqual(key);
                reader.ReadInt64();
                reader.ReadInt32();
                reader.SkipBytes(reader.ReadInt32());
                if (matched)
                {
                    matchCount++;
                    matchBytes += reader.Position - start;
                    if (matchCount == 1)
                    {
                        firstMatchStart = start;
                        firstMatchEnd = reader.Position;
                    }
                }
            }

            var entriesEnd = reader.Position;
            var addedSize = add is { } entry
                ? Utf8Size(sourceKey) + sizeof(long) + sizeof(int) + sizeof(int) + entry.Value.Length
                : 0;
            var newCount = count - matchCount + (add is null ? 0 : 1);
            if (newCount == 0)
            {
                return null;
            }

            var buffer = new byte[sizeof(int) + (entriesEnd - entriesStart) - matchBytes + addedSize];
            var writer = new RowWriter(buffer);
            writer.WriteInt32(newCount);

            // Pass two, block form - see SpliceInverse. The fold row's saving is
            // strictly larger: every skipped entry also carries an opaque value
            // payload whose length prefix the walk would read only to step over
            // it, and whose bytes it would copy one entry at a time.
            if (matchCount <= 1)
            {
                if (matchCount == 0)
                {
                    writer.WriteRaw(row[entriesStart..entriesEnd]);
                }
                else
                {
                    writer.WriteRaw(row[entriesStart..firstMatchStart]);
                    if (add is { } inPlace && !moveToEnd)
                    {
                        WriteFoldInverseEntry(ref writer, sourceKey, inPlace);
                    }

                    writer.WriteRaw(row[firstMatchEnd..entriesEnd]);
                }

                if (add is { } tail && (matchCount == 0 || moveToEnd))
                {
                    WriteFoldInverseEntry(ref writer, sourceKey, tail);
                }

                return buffer;
            }

            reader = new RowReader(row);
            reader.ReadBoundedCount(MinimumFoldInverseEntrySize);
            var written = false;
            for (var i = 0; i < count; i++)
            {
                var start = reader.Position;
                var matched = reader.ReadStringBytes().SequenceEqual(key);
                reader.ReadInt64();
                reader.ReadInt32();
                reader.SkipBytes(reader.ReadInt32());
                if (!matched)
                {
                    writer.WriteRaw(row[start..reader.Position]);
                    continue;
                }

                if (add is { } replacement && !written && !moveToEnd)
                {
                    WriteFoldInverseEntry(ref writer, sourceKey, replacement);
                    written = true;
                }
            }

            if (add is { } appended && !written)
            {
                WriteFoldInverseEntry(ref writer, sourceKey, appended);
            }

            return buffer;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static void WriteFoldInverseEntry(ref RowWriter writer, string sourceKey, in FoldMember entry)
    {
        writer.WriteString(sourceKey);
        writer.WriteInt64(entry.Timestamp.WallClockTicks);
        writer.WriteInt32(entry.Timestamp.Counter);
        writer.WriteInt32(entry.Value.Length);
        writer.WriteRaw(entry.Value);
    }

    /// <summary>
    /// A forward-only, allocation-free cursor over a fold-inverse row for a
    /// reader that flattens the row rather than looking keys up in it.
    /// <para>
    /// The counterpart of <see cref="InverseRowScan"/> for the custom-fold path,
    /// and it removes a strictly larger waste: the re-fold decoded each shard
    /// into a <see cref="Dictionary{TKey,TValue}"/> and then immediately walked
    /// that dictionary back out into a flat list, so every entry paid a hash, a
    /// bucket insert and a second copy for a map that was never probed. This
    /// cursor yields the same source key, value bytes and timestamp straight from
    /// the row, and <see cref="Remaining"/> lets the caller grow its list to the
    /// exact incoming count instead of doubling into it.
    /// </para>
    /// <para>
    /// The source key IS materialised here, unlike the inverse cursor: the re-fold
    /// orders its members by (HLC, source key), so the key is consumed rather
    /// than discarded. Validation matches <see cref="DecodeFoldInverse"/> byte for
    /// byte.
    /// </para>
    /// </summary>
    internal ref struct FoldInverseRowScan
    {
        private RowReader _reader;
        private int _remaining;

        /// <summary>Opens a cursor over an encoded fold-inverse row.</summary>
        /// <param name="bytes">A row produced by <see cref="EncodeFoldInverse"/>.</param>
        internal FoldInverseRowScan(ReadOnlySpan<byte> bytes)
        {
            _reader = new RowReader(bytes);
            _remaining = _reader.ReadBoundedCount(MinimumFoldInverseEntrySize);
        }

        /// <summary>The number of entries not yet walked.</summary>
        public readonly int Remaining => _remaining;

        /// <summary>The source key that contributed the current entry.</summary>
        public string SourceKey { get; private set; } = string.Empty;

        /// <summary>The current entry's contributed value and source HLC.</summary>
        public FoldMember Member { get; private set; }

        /// <summary>Advances to the next entry.</summary>
        /// <returns><see langword="true"/> when an entry was read.</returns>
        public bool MoveNext()
        {
            if (_remaining == 0)
            {
                return false;
            }

            _remaining--;
            SourceKey = _reader.ReadString();
            var ticks = _reader.ReadInt64();
            var counter = _reader.ReadInt32();
            var length = _reader.ReadInt32();
            var value = _reader.ReadBytes(length);
            Member = new FoldMember(value, new HybridLogicalClock { WallClockTicks = ticks, Counter = counter });
            return true;
        }
    }

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
        /// <summary>
        /// Longest UTF-16 length whose UTF-8 encoding is provably under the
        /// one-byte 7-bit prefix bound. A UTF-16 code unit encodes to at most
        /// three UTF-8 bytes, and a surrogate pair is two code units for four
        /// bytes, so three times the length is an upper bound on the encoded
        /// size for every string. Written as a constant rather than asked of
        /// <see cref="Encoding.GetMaxByteCount(int)"/>, which is a virtual call
        /// on <see cref="Encoding"/> and was being paid once per string purely
        /// to recompute this same product.
        /// </summary>
        private const int MaxSingleBytePrefixChars = 0x7F / 3;

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

        /// <summary>
        /// Writes a 7-bit-encoded UTF-8 byte count followed by the UTF-8 bytes,
        /// exactly as <see cref="BinaryWriter.Write(string)"/> does.
        /// <para>
        /// A string whose <i>worst case</i> UTF-8 length is already below
        /// <c>0x80</c> must encode to fewer than <c>0x80</c> bytes, so its
        /// 7-bit prefix is provably exactly one byte wide. That is the only
        /// thing the count pass was needed for, so the body is encoded straight
        /// past the reserved prefix slot and the prefix is back-filled from the
        /// encoder's own written count - which is by definition the number the
        /// count pass would have returned. That removes a full UTF-8 scan of
        /// every string on the row-encode path, which runs once per source key
        /// on every aggregation fold and re-encode.
        /// </para>
        /// <para>
        /// A longer string keeps the two-pass shape, because its prefix width is
        /// not known before the count and the body cannot be placed without it.
        /// The sizing pass that allocated this buffer measured the same string
        /// with <c>Utf8Size</c>, so the fast path's one-byte prefix and the
        /// space reserved for it agree by construction.
        /// </para>
        /// </summary>
        public void WriteString(string value)
        {
            if (value.Length <= MaxSingleBytePrefixChars)
            {
                var written = Encoding.UTF8.GetBytes(value, _buffer[(_pos + 1)..]);
                _buffer[_pos] = (byte)written;
                _pos += written + 1;
                return;
            }

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

        /// <summary>The number of bytes left to read, never negative.</summary>
        public readonly int Remaining => _buffer.Length - _pos;

        /// <summary>
        /// The cursor's current byte offset into the row. A splice takes it
        /// either side of an entry to delimit that entry's raw bytes, so the
        /// entry can be copied through without being materialised.
        /// </summary>
        public readonly int Position => _pos;

        public bool ReadBool()
        {
            Demand(sizeof(byte));
            return _buffer[_pos++] != 0;
        }

        public int ReadInt32()
        {
            Demand(sizeof(int));
            var value = BinaryPrimitives.ReadInt32LittleEndian(_buffer[_pos..]);
            _pos += sizeof(int);
            return value;
        }

        public long ReadInt64()
        {
            Demand(sizeof(long));
            var value = BinaryPrimitives.ReadInt64LittleEndian(_buffer[_pos..]);
            _pos += sizeof(long);
            return value;
        }

        public double ReadDouble()
        {
            Demand(sizeof(double));
            var value = BinaryPrimitives.ReadDoubleLittleEndian(_buffer[_pos..]);
            _pos += sizeof(double);
            return value;
        }

        public byte[] ReadBytes(int count)
        {
            // The length prefix is attacker-controlled on a ShipView row, so it is
            // validated against what the row can actually hold before the copy is
            // sized from it. Without this a negative or oversized length reaches
            // Slice and raises ArgumentOutOfRangeException, which the drain loop
            // does not recognise as a framing fault.
            if (count < 0 || count > Remaining)
            {
                throw new InvalidDataException(
                    $"An aggregation row declares a {count}-byte value but only {Remaining} byte(s) remain; the row is truncated or corrupt.");
            }

            var value = _buffer.Slice(_pos, count).ToArray();
            _pos += count;
            return value;
        }

        public string ReadString()
        {
            var byteCount = ReadStringLength();
            var value = Encoding.UTF8.GetString(_buffer.Slice(_pos, byteCount));
            _pos += byteCount;
            return value;
        }

        /// <summary>
        /// Steps over a length-prefixed UTF-8 string without transcoding it.
        /// Validates the prefix exactly as <see cref="ReadString"/> does, so a
        /// corrupt row is rejected at the same byte whether the caller wanted the
        /// string or not - a skip must not be a weaker gate than a read.
        /// </summary>
        public void SkipString()
        {
            // The byte count must land in a local first. Written as
            // `_pos += ReadStringLength()`, C# loads `_pos` BEFORE the call, so
            // the cursor advance that call makes over the length prefix is
            // overwritten by the store - leaving the reader one prefix short and
            // parsing the following fields from inside the previous string.
            var byteCount = ReadStringLength();
            _pos += byteCount;
        }

        /// <summary>
        /// Returns a length-prefixed UTF-8 string's raw bytes without
        /// transcoding them, so a caller comparing against a known key can do so
        /// on bytes rather than by decoding every candidate. Validates the prefix
        /// exactly as <see cref="ReadString"/> does.
        /// </summary>
        public ReadOnlySpan<byte> ReadStringBytes()
        {
            var byteCount = ReadStringLength();
            var value = _buffer.Slice(_pos, byteCount);

            // See SkipString: the count lands in a local before the store.
            _pos += byteCount;
            return value;
        }

        /// <summary>
        /// Steps over a raw byte run, bounding its declared length against the
        /// row exactly as <see cref="ReadBytes"/> does, so a hostile length is
        /// rejected identically whether the caller wanted the bytes or not.
        /// </summary>
        public void SkipBytes(int count)
        {
            if (count < 0 || count > Remaining)
            {
                throw new InvalidDataException(
                    $"An aggregation row declares a {count}-byte value but only {Remaining} byte(s) remain; the row is truncated or corrupt.");
            }

            _pos += count;
        }

        /// <summary>
        /// Reads and bounds a string's 7-bit-encoded UTF-8 byte-count prefix,
        /// leaving the cursor on the first content byte.
        /// </summary>
        private int ReadStringLength()
        {
            var byteCount = Read7BitEncodedInt();
            if (byteCount < 0 || byteCount > Remaining)
            {
                throw new InvalidDataException(
                    $"An aggregation row declares a {byteCount}-byte string but only {Remaining} byte(s) remain; the row is truncated or corrupt.");
            }

            return byteCount;
        }

        /// <summary>
        /// Reads a leading entry count and bounds it against the bytes that remain,
        /// so a caller can pre-size a collection from it without a hostile or
        /// corrupt row turning four bytes into a multi-gigabyte allocation. Each
        /// entry costs at least <paramref name="minimumEntrySize"/> bytes, so a
        /// count above that ceiling is necessarily a lie.
        /// </summary>
        public int ReadBoundedCount(int minimumEntrySize)
        {
            var count = ReadInt32();
            var maxPossible = Remaining / minimumEntrySize;
            if (count < 0 || count > maxPossible)
            {
                throw new InvalidDataException(
                    $"An aggregation row reports {count} entries but its {Remaining} remaining byte(s) can hold at most {maxPossible}; the row is truncated or corrupt.");
            }

            return count;
        }

        private void Demand(int bytes)
        {
            if (Remaining < bytes)
            {
                throw new InvalidDataException(
                    $"An aggregation row needs {bytes} more byte(s) but only {Remaining} remain; the row is truncated or corrupt.");
            }
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
                Demand(sizeof(byte));
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
