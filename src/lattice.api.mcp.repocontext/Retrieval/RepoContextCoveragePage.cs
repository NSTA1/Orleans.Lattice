using System.Buffers.Binary;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The paging arithmetic and on-disk encoding of the per-page vector-coverage
/// digest: a compacted mirror of the membership tree that makes gap detection cost
/// a function of the <b>page</b> count rather than the source count (issue #2486).
/// <para>
/// A source identifier is the first sixteen lowercase hex characters of the
/// SHA-256 of a record key (see <see cref="VectorCodec.SourceId(string)"/>), so it
/// is a uniformly distributed 64-bit value. The digest partitions the identifier
/// space by the identifier's leading byte, giving <see cref="PageCount"/> fixed
/// pages whose occupancy is even by construction - the partition needs no
/// rebalancing and no knowledge of the corpus, because the hash already provides
/// the uniformity.
/// </para>
/// <para>
/// A page carries the <b>set</b> of covered identifiers rather than a count or a
/// checksum, and that choice is load-bearing rather than incidental. Membership is
/// written through
/// <see cref="Orleans.Lattice.ILattice.EnableManyAsync(System.Collections.Generic.List{string}, string, System.Threading.CancellationToken)"/>,
/// which reports no per-key transition, so a delta-maintained counter would
/// double-count the routine case of re-adding an already-covered source. A set is
/// idempotent under add, exact (a gap the whole-set scan would have found is still
/// found, with no probabilistic masking), supports removal, and - the property the
/// targeted repair queue is built on - yields the <b>identity</b> of each missing
/// source, not merely the fact that one exists.
/// </para>
/// </summary>
internal static class RepoContextCoveragePage
{
    /// <summary>
    /// The number of digest pages the 64-bit source-identifier space is partitioned
    /// into: one per value of the identifier's leading byte.
    /// <para>
    /// The figure is the detection cost. A scan reads exactly this many rows
    /// regardless of corpus size, against two membership point-reads per source
    /// before this digest existed, so 256 is simultaneously the bound and the
    /// break-even point: below roughly 128 sources the old probe was cheaper, and
    /// above it the saving grows without limit. It is a power of two so the page of
    /// an identifier is its leading byte with no arithmetic, and it is fixed rather
    /// than configurable because a page count that varied per repository would make
    /// a digest written by one deployment unreadable by another.
    /// </para>
    /// </summary>
    internal const int PageCount = 256;

    /// <summary>
    /// The schema tag stamped into the leading byte of every encoded page, so a
    /// future layout change is detected on read rather than silently mis-decoded.
    /// An unrecognised version decodes as an empty page, which under-reports
    /// coverage - the safe direction, costing a redundant embed rather than masking
    /// a gap.
    /// </summary>
    internal const byte SchemaVersion = 1;

    private const int HeaderLength = 1 + 4 + 4;

    /// <summary>
    /// Reports the digest page a source identifier belongs to, or
    /// <see langword="null"/> when the identifier is not a plain 16-character
    /// hexadecimal source id (a reserved marker such as an embedded-memory key is
    /// not a source id and has no page).
    /// </summary>
    /// <param name="sourceId">The candidate source identifier. May be <see langword="null"/>.</param>
    internal static int? PageOf(string? sourceId)
        => TryParse(sourceId, out var value) ? (int)(value >> 56) : null;

    /// <summary>
    /// Reports the digest page a parsed source identifier belongs to: its leading
    /// byte.
    /// </summary>
    /// <param name="sourceId">The parsed 64-bit source identifier.</param>
    internal static int PageOf(ulong sourceId) => (int)(sourceId >> 56);

    /// <summary>
    /// Parses a 16-character lowercase hexadecimal source identifier into the 64-bit
    /// value the digest stores. Returns <see langword="false"/> for any other shape,
    /// which is how the reserved contentless and embedded-memory markers - whose
    /// collection component is not a bare source id - are excluded from the digest.
    /// </summary>
    /// <param name="sourceId">The candidate identifier. May be <see langword="null"/>.</param>
    /// <param name="value">The parsed value on success; zero otherwise.</param>
    internal static bool TryParse(string? sourceId, out ulong value)
    {
        value = 0;
        if (sourceId is null || sourceId.Length != VectorCodec.SourceIdByteLength)
        {
            return false;
        }

        // Parsed character by character rather than through a NumberStyles overload
        // so the accepted shape is exactly what VectorCodec.SourceId emits: sixteen
        // lower-case hex digits, no sign, no whitespace, no "0x". A reserved marker
        // that happens to be sixteen characters long must not squeeze through.
        ulong parsed = 0;
        foreach (var c in sourceId)
        {
            var digit = c switch
            {
                >= '0' and <= '9' => c - '0',
                >= 'a' and <= 'f' => (c - 'a') + 10,
                _ => -1,
            };

            if (digit < 0)
            {
                return false;
            }

            parsed = (parsed << 4) | (uint)digit;
        }

        value = parsed;
        return true;
    }

    /// <summary>
    /// Renders a parsed identifier back to the canonical 16-character lowercase
    /// hexadecimal form <see cref="VectorCodec.SourceId(string)"/> produces, so a
    /// digest-derived identifier is interchangeable with a membership-derived one.
    /// </summary>
    /// <param name="value">The 64-bit source identifier.</param>
    internal static string Format(ulong value) => value.ToString("x16", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// Encodes one page as
    /// <c>[version][embeddedCount][contentlessCount][embedded...][contentless...]</c>
    /// with little-endian fixed-width fields and both identifier runs sorted
    /// ascending, so the same logical page always encodes to the same bytes and a
    /// no-op rewrite is byte-identical (which is what lets the write path skip a
    /// write that would change nothing).
    /// </summary>
    /// <param name="embedded">The embedded source identifiers on this page. Must not be <see langword="null"/>.</param>
    /// <param name="contentless">The contentless-marked source identifiers on this page. Must not be <see langword="null"/>.</param>
    internal static byte[] Encode(IReadOnlyCollection<ulong> embedded, IReadOnlyCollection<ulong> contentless)
    {
        ArgumentNullException.ThrowIfNull(embedded);
        ArgumentNullException.ThrowIfNull(contentless);

        var embeddedSorted = embedded.ToArray();
        var contentlessSorted = contentless.ToArray();
        Array.Sort(embeddedSorted);
        Array.Sort(contentlessSorted);

        var buffer = new byte[HeaderLength + (sizeof(ulong) * (embeddedSorted.Length + contentlessSorted.Length))];
        buffer[0] = SchemaVersion;
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(1, 4), embeddedSorted.Length);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(5, 4), contentlessSorted.Length);

        var offset = HeaderLength;
        foreach (var value in embeddedSorted)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(offset, sizeof(ulong)), value);
            offset += sizeof(ulong);
        }

        foreach (var value in contentlessSorted)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(offset, sizeof(ulong)), value);
            offset += sizeof(ulong);
        }

        return buffer;
    }

    /// <summary>
    /// Decodes a page written by <see cref="Encode"/>. Any malformed, truncated, or
    /// unrecognised-version payload decodes to two empty sets rather than throwing:
    /// the digest is a derived accelerator, and losing a page under-reports
    /// coverage, which costs a redundant embed instead of masking a gap.
    /// </summary>
    /// <param name="payload">The stored page bytes. May be <see langword="null"/>.</param>
    /// <param name="embedded">Receives the embedded identifiers.</param>
    /// <param name="contentless">Receives the contentless-marked identifiers.</param>
    /// <returns><see langword="true"/> when the payload decoded cleanly.</returns>
    internal static bool TryDecode(byte[]? payload, out HashSet<ulong> embedded, out HashSet<ulong> contentless)
    {
        embedded = [];
        contentless = [];

        if (payload is null || payload.Length < HeaderLength || payload[0] != SchemaVersion)
        {
            return false;
        }

        var embeddedCount = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(1, 4));
        var contentlessCount = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(5, 4));
        if (embeddedCount < 0 || contentlessCount < 0)
        {
            return false;
        }

        var expected = (long)HeaderLength + (sizeof(ulong) * ((long)embeddedCount + contentlessCount));
        if (payload.LongLength != expected)
        {
            return false;
        }

        var offset = HeaderLength;
        for (var i = 0; i < embeddedCount; i++)
        {
            embedded.Add(BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(offset, sizeof(ulong))));
            offset += sizeof(ulong);
        }

        for (var i = 0; i < contentlessCount; i++)
        {
            contentless.Add(BinaryPrimitives.ReadUInt64LittleEndian(payload.AsSpan(offset, sizeof(ulong))));
            offset += sizeof(ulong);
        }

        return true;
    }
}
