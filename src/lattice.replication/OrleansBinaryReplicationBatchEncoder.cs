using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Frozen;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationBatchEncoder"/> implementation.
/// Frames a <see cref="ReplicationBatchEnvelope"/> using the Orleans
/// serializer, producing the canonical binary wire format
/// <c>application/x-orleans-lattice-replog+binary</c>. Stamps
/// <see cref="ReplicationBatchEnvelope.CurrentVersion"/> on outbound
/// envelopes whose <see cref="ReplicationBatchEnvelope.WireVersion"/>
/// is the default <c>0</c>; rejects inbound payloads whose
/// <see cref="ReplicationBatchEnvelope.WireVersion"/> is strictly
/// greater than the supported version.
/// <para>
/// The Orleans serializer's <c>byte[]</c> handling is roughly 33% more
/// compact than JSON's base64 encoding on the same payload, which is
/// the bandwidth case the binary-framing seam exists to address. A
/// JSON encoder remains a future option for HTTP-transport
/// debuggability and is wired in by registering an alternative
/// <see cref="IReplicationBatchEncoder"/> via DI.
/// </para>
/// </summary>
internal sealed class OrleansBinaryReplicationBatchEncoder : IReplicationBatchEncoder
{
    /// <summary>
    /// Canonical binary content type stamped on outbound HTTP / gRPC
    /// metadata. The <c>+binary</c> suffix mirrors the convention from
    /// <c>application/foo+json</c> media types so dispatch tables that
    /// match on the <c>+xxx</c> suffix can route to the Orleans
    /// serializer codec without parsing the prefix.
    /// </summary>
    public const string BinaryContentType = "application/x-orleans-lattice-replog+binary";

    private readonly Serializer<ReplicationBatchEnvelope> _serializer;
    private readonly FrozenDictionary<byte, ILatticeCompressor> _compressors;
    private readonly IOptionsMonitor<LatticeReplicationOptions>? _options;

    /// <summary>
    /// Initialises the encoder with the supplied
    /// <see cref="Serializer{T}"/> and the registered framing
    /// compressors. Resolved from DI in the standard silo
    /// registration path; tests construct it directly with a
    /// serializer pulled from
    /// <c>new ServiceCollection().AddSerializer().BuildServiceProvider()</c>.
    /// The <paramref name="compressors"/> sequence is indexed by the
    /// raw byte value of <see cref="ILatticeCompressor.Algorithm"/>
    /// rather than the strongly-typed enum so host-defined algorithms
    /// (whose tag is reserved in the <c>0x80..0xFF</c> range and is
    /// not a defined <see cref="LatticeCompression"/> member) round
    /// trip without core needing to ship an enum value for every
    /// algorithm. Duplicates throw at construction so a host that
    /// double-registers a compressor fails fast at startup rather
    /// than silently shadowing.
    /// <para>
    /// The optional <paramref name="options"/> monitor supplies the
    /// decompression ceiling enforced on the inbound compressed
    /// framing path (see
    /// <see cref="LatticeReplicationOptions.MaxInboundDecompressedBytes"/>).
    /// It is resolved from DI in the standard registration path; when
    /// omitted (the direct-construction test path) the encoder falls
    /// back to <see cref="LatticeReplicationOptions.DefaultMaxInboundDecompressedBytes"/>.
    /// </para>
    /// </summary>
    public OrleansBinaryReplicationBatchEncoder(
        Serializer<ReplicationBatchEnvelope> serializer,
        IEnumerable<ILatticeCompressor>? compressors = null,
        IOptionsMonitor<LatticeReplicationOptions>? options = null)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        _serializer = serializer;
        _options = options;

        if (compressors is null)
        {
            _compressors = FrozenDictionary<byte, ILatticeCompressor>.Empty;
            return;
        }

        var dict = new Dictionary<byte, ILatticeCompressor>();
        var coreAssembly = typeof(LatticeCompression).Assembly;
        foreach (var c in compressors)
        {
            ArgumentNullException.ThrowIfNull(c);
            if (c.Algorithm == LatticeCompression.None)
            {
                throw new ArgumentException(
                    $"An {nameof(ILatticeCompressor)} cannot register {nameof(LatticeCompression)}.{nameof(LatticeCompression.None)}; that value is reserved for the uncompressed pass-through path.",
                    nameof(compressors));
            }
            var tag = (byte)c.Algorithm;
            // The compression tag space is partitioned: [0x00, 0x7F]
            // is reserved for core-shipped algorithms and [0x80,
            // 0xFF] is available to host-defined algorithms. Only
            // types declared in the core Orleans.Lattice assembly
            // may claim a tag in the core-reserved range - this
            // closes two distinct hazards in one rule:
            //   1. A host claiming an undefined core tag (e.g.
            //      0x42) would silently collide with whatever
            //      algorithm a future core release ships at that
            //      tag.
            //   2. A host claiming a defined core tag (e.g.
            //      Zstd, 0x01) with their own non-canonical
            //      implementation would silently squat on the
            //      wire identity of the canonical core algorithm,
            //      producing receiver-side decode failures or
            //      corrupted streams against any peer running the
            //      core implementation.
            // Host-defined algorithms - including Zstd-compatible
            // variants - must use a tag in [0x80, 0xFF]. See
            // docs/lattice/compression.md.
            if (tag < 0x80 && c.GetType().Assembly != coreAssembly)
            {
                throw new ArgumentException(
                    $"{nameof(ILatticeCompressor)} '{c.GetType().FullName}' claims compression tag 0x{tag:X2}, which lies in the core-reserved range [0x00, 0x7F]. "
                    + "Only types declared in the core Orleans.Lattice assembly may claim a tag in that range; host-defined compressors (including alternative implementations of a core algorithm) must use a tag in the reserved [0x80, 0xFF] range.",
                    nameof(compressors));
            }
            if (!dict.TryAdd(tag, c))
            {
                throw new ArgumentException(
                    $"Multiple {nameof(ILatticeCompressor)} registrations for compression tag 0x{tag:X2} ({nameof(LatticeCompression)}.{c.Algorithm}); only one compressor may be registered per algorithm tag.",
                    nameof(compressors));
            }
        }
        _compressors = dict.ToFrozenDictionary();
    }

    /// <inheritdoc />
    public string ContentType => BinaryContentType;

    /// <inheritdoc />
    public int CurrentWireVersion => ReplicationBatchEnvelope.CurrentVersion;

    /// <inheritdoc />
    public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        if (string.IsNullOrEmpty(envelope.TreeName))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatchEnvelope)}.{nameof(ReplicationBatchEnvelope.TreeName)} must be non-empty.",
                nameof(envelope));
        }

        if (string.IsNullOrEmpty(envelope.OriginClusterId))
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatchEnvelope)}.{nameof(ReplicationBatchEnvelope.OriginClusterId)} must be non-empty.",
                nameof(envelope));
        }

        if (envelope.WireVersion < 0)
        {
            throw new ArgumentException(
                $"{nameof(ReplicationBatchEnvelope)}.{nameof(ReplicationBatchEnvelope.WireVersion)} must be non-negative.",
                nameof(envelope));
        }

        // Stamp the current wire version when the caller left it at the
        // default 0; preserve any explicitly-supplied value verbatim so
        // tests and forward-compat producers can author payloads
        // targeting a specific version without the encoder silently
        // overwriting them. Normalise a null Entries collection to an
        // empty array so receivers never have to special-case null.
        var stamped = envelope with
        {
            WireVersion = envelope.WireVersion == 0 ? CurrentWireVersion : envelope.WireVersion,
            Entries = envelope.Entries ?? Array.Empty<WalRecord>(),
        };

        // Hand the buffer writer straight to the Orleans serializer so
        // the envelope's bytes are appended into caller-owned memory
        // (typically a pooled ArrayBufferWriter, or the gRPC stream's
        // writer at the transport layer). No per-batch byte[]
        // allocation in the canonical hot path.
        _serializer.Serialize(stamped, writer);
    }

    /// <inheritdoc />
    public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
    {
        if (payload.IsEmpty)
        {
            throw new ArgumentException(
                "Replication batch payload must be non-empty.",
                nameof(payload));
        }

        ReplicationBatchEnvelope envelope;
        try
        {
            // Serializer<T>.Deserialize accepts ReadOnlySpan<byte>; the
            // caller's ReadOnlyMemory is materialised via .Span so we
            // do not allocate a copy.
            envelope = _serializer.Deserialize(payload.Span);
        }
        catch (Exception inner)
        {
            throw new ArgumentException(
                "Replication batch payload could not be decoded; the bytes are not a valid "
                + $"{nameof(ReplicationBatchEnvelope)} produced by this encoder.",
                nameof(payload),
                inner);
        }

        if (envelope.WireVersion > CurrentWireVersion)
        {
            throw new NotSupportedException(
                $"Replication batch envelope wire version {envelope.WireVersion} is newer than "
                + $"the supported version {CurrentWireVersion}; upgrade the receiver before "
                + "applying payloads from this producer.");
        }

        // Defensive normalisation: a hand-constructed payload may have
        // been encoded with Entries left at the default null. Receivers
        // expect an iterable; substitute an empty list so call sites do
        // not have to add a null guard.
        if (envelope.Entries is null)
        {
            envelope = envelope with { Entries = Array.Empty<WalRecord>() };
        }

        return envelope;
    }

    /// <summary>
    /// Framing-encode override that adds optional tail compression on
    /// top of the canonical wire layout authored by the
    /// <see cref="IReplicationBatchEncoder.EncodeFraming"/> default
    /// implementation. When
    /// <see cref="EncodedBatchHeader.Compression"/> is
    /// <see cref="LatticeCompression.None"/> the bytes are written
    /// verbatim and the layout is identical to the default. When the
    /// header asks for a non-<see cref="LatticeCompression.None"/>
    /// algorithm, the encoder builds the uncompressed tail
    /// (<c>treeName</c> + <c>originClusterId</c> + length-prefixed
    /// entry segments) into a pooled buffer, then writes the fixed
    /// header followed by:
    /// <list type="number">
    /// <item><description>4-byte little-endian uncompressed tail length.</description></item>
    /// <item><description>4-byte little-endian compressed tail length.</description></item>
    /// <item><description>The compressed tail bytes.</description></item>
    /// </list>
    /// An algorithm value with no registered <see cref="ILatticeCompressor"/>
    /// throws <see cref="NotSupportedException"/>.
    /// </summary>
    public void EncodeFraming(
        in EncodedBatchHeader header,
        string treeName,
        string originClusterId,
        ReadOnlyMemory<ArraySegment<byte>> entries,
        IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(treeName);
        ArgumentNullException.ThrowIfNull(originClusterId);

        if (header.Compression == LatticeCompression.None)
        {
            // Inline the canonical uncompressed layout directly
            // here. We cannot call back through the
            // IReplicationBatchEncoder default implementation because
            // this type provides an override of the same method, so
            // the interface dispatch would re-enter this overload
            // and recurse infinitely.
            EncodeUncompressedFraming(header, treeName, originClusterId, entries, writer);
            return;
        }

        // Resolve the effective algorithm + dictionary id, applying
        // graceful local fallback for the dictionary tag: when the
        // requested ZstdDictionary frame cannot be produced on this
        // silo (no dictionary-aware compressor registered, or the
        // requested dictionary id is not resolvable here) we degrade
        // to plain Zstd - still decodable by any peer carrying the
        // core Zstd compressor - or, failing that, to the verbatim
        // uncompressed layout. The plain Zstd and host-defined paths
        // are unchanged and still throw NotSupportedException when
        // their compressor is absent.
        var (effectiveCompression, dictionaryId, compressor) =
            ResolveEffectiveCompression(header.Compression, header.DictionaryId);

        if (effectiveCompression == LatticeCompression.None)
        {
            var verbatimHeader = header with
            {
                Compression = LatticeCompression.None,
                DictionaryId = 0u,
            };
            EncodeUncompressedFraming(verbatimHeader, treeName, originClusterId, entries, writer);
            return;
        }

        var effectiveHeader = header with
        {
            Compression = effectiveCompression,
            DictionaryId = dictionaryId,
        };
        var isDictionary = effectiveCompression == LatticeCompression.ZstdDictionary;

        // Pool the uncompressed-tail buffer through ArrayPool<byte>.Shared
        // so the compressed-encode hot path is allocation-free in
        // steady state. Pre-computing the exact tail size (UTF-8 byte
        // counts of the routing strings + per-entry length prefix +
        // entry body bytes) lets us rent a single right-sized buffer
        // in one call instead of growing an ArrayBufferWriter and
        // discarding the intermediate backing arrays. The compressed
        // bytes are written directly into the caller-supplied
        // IBufferWriter<byte>, then the 4-byte compressed-length
        // prefix is patched in-place once the compressor reports the
        // written count - this avoids a second pool rent for the
        // compressed scratch buffer.
        var uncompressedLength = ComputeTailSize(treeName, originClusterId, entries.Span);
        var uncompressedRented = ArrayPool<byte>.Shared.Rent(uncompressedLength);
        try
        {
            var tailSpan = uncompressedRented.AsSpan(0, uncompressedLength);
            WriteTailIntoSpan(tailSpan, treeName, originClusterId, entries.Span);

            // Fixed header first (plaintext, with the effective
            // Compression set; the fixed 32-byte layout is byte-
            // identical to every prior wire version - DictionaryId
            // rides the tail, not the header).
            var headerSpan = writer.GetSpan(EncodedBatchHeader.WireSize);
            effectiveHeader.WriteTo(headerSpan);
            writer.Advance(EncodedBatchHeader.WireSize);

            // Dictionary tail prepends the 4-byte little-endian
            // dictionary id ahead of the existing uncompressed /
            // compressed length prefixes so the receiver can select
            // the matching dictionary before inflating.
            if (isDictionary)
            {
                var dictSpan = writer.GetSpan(4);
                BinaryPrimitives.WriteUInt32LittleEndian(dictSpan, dictionaryId);
                writer.Advance(4);
            }

            // Uncompressed length prefix.
            var unprefixSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(unprefixSpan, uncompressedLength);
            writer.Advance(4);

            // Reserve the worst-case compressed-body span (4-byte
            // length prefix + compressor's max-compressed-length
            // bound) from the caller's writer in a single GetSpan
            // call. The compressor writes directly into the reserved
            // span; afterwards we patch the length prefix and Advance
            // by the actual written count.
            int compressedLength;
            if (isDictionary)
            {
                var dictCompressor = (ILatticeDictionaryCompressor)compressor!;
                var bound = dictCompressor.GetMaxCompressedLength(uncompressedLength, dictionaryId);
                var compressedSpan = writer.GetSpan(4 + bound);
                compressedLength = dictCompressor.Compress(tailSpan, compressedSpan[4..(4 + bound)], dictionaryId);
                BinaryPrimitives.WriteInt32LittleEndian(compressedSpan[0..4], compressedLength);
                writer.Advance(4 + compressedLength);

                // Before/after ratio observability so an operator can
                // quantify the dictionary win against the dictionary-
                // less baseline.
                LatticeReplicationMetrics.CompressDictionaryBytesIn.Add(
                    uncompressedLength,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    LatticeTenantLabel.ForTree(treeName));
                LatticeReplicationMetrics.CompressDictionaryBytesOut.Add(
                    compressedLength,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeName),
                    LatticeTenantLabel.ForTree(treeName));
            }
            else
            {
                var bound = compressor!.GetMaxCompressedLength(uncompressedLength);
                var compressedSpan = writer.GetSpan(4 + bound);
                compressedLength = compressor.Compress(tailSpan, compressedSpan[4..(4 + bound)]);
                BinaryPrimitives.WriteInt32LittleEndian(compressedSpan[0..4], compressedLength);
                writer.Advance(4 + compressedLength);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(uncompressedRented);
        }
    }

    /// <summary>
    /// Resolves the algorithm actually used to frame a batch given the
    /// requested <paramref name="requested"/> tag and
    /// <paramref name="requestedDictionaryId"/>. For
    /// <see cref="LatticeCompression.ZstdDictionary"/> this applies the
    /// graceful local fallback chain (dictionary -> plain Zstd ->
    /// verbatim) so a silo that lacks the requested dictionary still
    /// ships a decodable frame; for every other non-None tag it
    /// preserves the historical "throw when the compressor is absent"
    /// contract.
    /// </summary>
    private (LatticeCompression Compression, uint DictionaryId, ILatticeCompressor? Compressor) ResolveEffectiveCompression(
        LatticeCompression requested,
        uint requestedDictionaryId)
    {
        if (requested == LatticeCompression.ZstdDictionary)
        {
            if (requestedDictionaryId != 0
                && _compressors.TryGetValue((byte)LatticeCompression.ZstdDictionary, out var dictCandidate)
                && dictCandidate is ILatticeDictionaryCompressor dictCompressor
                && dictCompressor.HasDictionary(requestedDictionaryId))
            {
                return (LatticeCompression.ZstdDictionary, requestedDictionaryId, dictCandidate);
            }

            if (_compressors.TryGetValue((byte)LatticeCompression.Zstd, out var zstd))
            {
                return (LatticeCompression.Zstd, 0u, zstd);
            }

            return (LatticeCompression.None, 0u, null);
        }

        if (!_compressors.TryGetValue((byte)requested, out var compressor))
        {
            throw new NotSupportedException(
                $"No {nameof(ILatticeCompressor)} is registered for compression tag 0x{(byte)requested:X2}; register a singleton via DI before encoding batches with this algorithm.");
        }

        return (requested, 0u, compressor);
    }

    /// <summary>
    /// Framing-decode override that handles the optional compressed
    /// tail layout authored by <see cref="EncodeFraming"/>. The
    /// uncompressed code path delegates to the canonical
    /// <see cref="IReplicationBatchEncoder.TryDecodeFraming"/>
    /// implementation; the compressed code path inflates the tail
    /// bytes into a pooled buffer, then re-runs the canonical
    /// uncompressed parse over the inflated bytes.
    /// </summary>
    public bool TryDecodeFraming(
        ReadOnlyMemory<byte> payload,
        out EncodedBatchHeader header,
        out string treeName,
        out string originClusterId,
        out ReadOnlyMemory<ArraySegment<byte>> entries)
    {
        header = default;
        treeName = string.Empty;
        originClusterId = string.Empty;
        entries = ReadOnlyMemory<ArraySegment<byte>>.Empty;

        if (payload.Length < EncodedBatchHeader.WireSize)
        {
            return false;
        }

        var span = payload.Span;
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(span[0..4]);
        if (magic != EncodedBatchHeader.MagicValue)
        {
            return false;
        }

        var parsed = EncodedBatchHeader.ReadFrom(span);
        if (parsed.WireVersion > EncodedBatchHeader.CurrentWireVersion)
        {
            throw new NotSupportedException(
                $"Framing wire version {parsed.WireVersion} is newer than the supported "
                + $"version {EncodedBatchHeader.CurrentWireVersion}; upgrade the receiver "
                + "before applying payloads from this producer.");
        }

        if (parsed.Compression == LatticeCompression.None)
        {
            return DecodeUncompressedFraming(payload, parsed, out header, out treeName, out originClusterId, out entries);
        }

        if (!_compressors.TryGetValue((byte)parsed.Compression, out var compressor))
        {
            throw new NotSupportedException(
                $"No {nameof(ILatticeCompressor)} is registered for compression tag 0x{(byte)parsed.Compression:X2}; register a singleton via DI before decoding batches with this algorithm.");
        }

        var isDictionary = parsed.Compression == LatticeCompression.ZstdDictionary;
        ILatticeDictionaryCompressor? dictCompressor = null;
        uint dictionaryId = 0;
        var cursor = EncodedBatchHeader.WireSize;

        // The dictionary tail prepends a 4-byte little-endian
        // dictionary id ahead of the two length prefixes so the
        // receiver can select the matching dictionary. A frame that
        // references a dictionary this silo cannot resolve (no
        // dictionary-aware compressor, or an unknown id) surfaces
        // NotSupportedException rather than silently mis-decoding -
        // the consuming pipeline routes that through its existing
        // unknown-tag negotiation/backoff path.
        if (isDictionary)
        {
            if (cursor + 4 > payload.Length)
            {
                throw new ArgumentException(
                    $"Framing payload is truncated at the dictionary-id prefix; expected at least 4 more bytes at offset {cursor}.",
                    nameof(payload));
            }
            dictionaryId = BinaryPrimitives.ReadUInt32LittleEndian(span[cursor..(cursor + 4)]);
            cursor += 4;

            dictCompressor = compressor as ILatticeDictionaryCompressor;
            if (dictCompressor is null)
            {
                throw new NotSupportedException(
                    $"The compressor registered for compression tag 0x{(byte)parsed.Compression:X2} does not implement {nameof(ILatticeDictionaryCompressor)}; cannot decode a dictionary frame.");
            }
            if (!dictCompressor.HasDictionary(dictionaryId))
            {
                throw new NotSupportedException(
                    $"Compression dictionary id {dictionaryId} is not available on this receiver; the matching dictionary must be registered before decoding this frame.");
            }
        }

        // Read the two 4-byte length prefixes from the compressed
        // tail layout. The fixed header was 32 bytes; the next 4
        // bytes are uncompressed length, the 4 after that are
        // compressed length, then the compressed body. For the
        // dictionary tail the cursor has already advanced past the
        // 4-byte dictionary id.
        if (cursor + 8 > payload.Length)
        {
            throw new ArgumentException(
                $"Framing payload is truncated at the compressed-tail length prefixes; expected at least 8 more bytes at offset {cursor}.",
                nameof(payload));
        }
        var uncompressedLength = BinaryPrimitives.ReadInt32LittleEndian(span[cursor..(cursor + 4)]);
        cursor += 4;
        var compressedLength = BinaryPrimitives.ReadInt32LittleEndian(span[cursor..(cursor + 4)]);
        cursor += 4;

        if (uncompressedLength < 0 || compressedLength < 0)
        {
            throw new ArgumentException(
                $"Framing payload reports a negative tail length (uncompressed={uncompressedLength}, compressed={compressedLength}); payload is corrupt.",
                nameof(payload));
        }

        // Bound the declared uncompressed length BEFORE renting a
        // buffer sized to it. The length is a wire field a hostile or
        // corrupt sender can forge independently of how few compressed
        // bytes it actually ships, so an unbounded value drives a
        // multi-gigabyte allocation from a tiny request - the classic
        // decompression-bomb amplification. The gRPC transport decodes
        // framing before the shared-secret auth interceptor body runs,
        // so this allocation is reachable pre-auth.
        var maxDecompressedBytes =
            _options?.CurrentValue.MaxInboundDecompressedBytes
            ?? LatticeReplicationOptions.DefaultMaxInboundDecompressedBytes;
        if (uncompressedLength > maxDecompressedBytes)
        {
            throw new ArgumentException(
                $"Framing payload declares an uncompressed tail length of {uncompressedLength} bytes, "
                + $"which exceeds the configured {nameof(LatticeReplicationOptions.MaxInboundDecompressedBytes)} "
                + $"ceiling of {maxDecompressedBytes} bytes; refusing to allocate a decompression buffer for a "
                + "potential decompression bomb. Raise the ceiling if this reflects a legitimately large batch.",
                nameof(payload));
        }
        // Widen to long before summing: both operands are int and
        // compressedLength is an attacker-controllable wire field (up to
        // int.MaxValue), so cursor + compressedLength can overflow to a
        // negative value and slip past this truncation guard. The 64-bit
        // sum cannot overflow for two non-negative int operands, so an
        // oversized declared length is caught here by the explicit
        // framing-corruption message rather than by the later
        // span.Slice(cursor, compressedLength) bounds check.
        if ((long)cursor + compressedLength > payload.Length)
        {
            throw new ArgumentException(
                $"Framing payload is truncated at the compressed body; declared compressed length {compressedLength} would overrun the payload (remaining {payload.Length - cursor} bytes).",
                nameof(payload));
        }

        // Rent a buffer sized to the uncompressed tail, decompress
        // into it, then parse the routing strings + entry segments
        // directly out of the inflated bytes. The
        // ArraySegment<byte>[] surfaced through `entries` references
        // the rented buffer; we hand a managed copy back to the
        // caller and return the rented array to the pool, so callers
        // can outlive the synchronous return.
        var rented = ArrayPool<byte>.Shared.Rent(uncompressedLength);
        try
        {
            var inflateDest = new Span<byte>(rented, 0, uncompressedLength);
            var compressedSlice = span.Slice(cursor, compressedLength);
            if (isDictionary)
            {
                dictCompressor!.Decompress(compressedSlice, inflateDest, uncompressedLength, dictionaryId);
            }
            else
            {
                compressor.Decompress(compressedSlice, inflateDest, uncompressedLength);
            }

            // Copy into a freshly-allocated byte[] backing buffer so
            // the surfaced ArraySegment<byte> entries remain valid
            // after we return the rented buffer to the pool. This is
            // one allocation per compressed batch on the receive
            // path; eliminating it requires widening the contract to
            // a pooled buffer the caller releases, which is a
            // separate design pass.
            var owned = new byte[uncompressedLength];
            inflateDest.CopyTo(owned);

            ParseTailIntoSegments(
                owned,
                parsed.EntryCount,
                out treeName,
                out originClusterId,
                out var segments);

            header = isDictionary ? parsed with { DictionaryId = dictionaryId } : parsed;
            entries = segments;
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static bool DecodeUncompressedFraming(
        ReadOnlyMemory<byte> payload,
        EncodedBatchHeader parsed,
        out EncodedBatchHeader header,
        out string treeName,
        out string originClusterId,
        out ReadOnlyMemory<ArraySegment<byte>> entries)
    {
        header = default;
        treeName = string.Empty;
        originClusterId = string.Empty;
        entries = ReadOnlyMemory<ArraySegment<byte>>.Empty;

        if (parsed.EntryCount < 0)
        {
            throw new ArgumentException(
                $"Framing header reports a negative entry count ({parsed.EntryCount}); payload is corrupt.",
                nameof(payload));
        }

        // Guard the up-front segment-array allocation against an adversarial
        // entry count. EntryCount is read straight off the fixed header, which
        // a caller can forge independently of the actual payload length; the
        // per-entry truncation checks below run only *after* the array is
        // allocated, so they do not protect this allocation. Every entry
        // contributes at least a 4-byte length prefix, so an EntryCount larger
        // than payload.Length / 4 cannot be satisfied and the payload is
        // necessarily truncated. Rejecting it here turns a multi-gigabyte
        // ArraySegment[] allocation (and the OutOfMemoryException it would
        // raise) into a cheap, catchable ArgumentException.
        var maxPossibleEntries = payload.Length / sizeof(int);
        if (parsed.EntryCount > maxPossibleEntries)
        {
            throw new ArgumentException(
                $"Framing header reports {parsed.EntryCount} entries but the {payload.Length}-byte "
                + $"payload can hold at most {maxPossibleEntries}; payload is truncated or corrupt.",
                nameof(payload));
        }

        // Resolve the payload to a contiguous byte[] so each entry's
        // ArraySegment can point back into it without copying. The
        // canonical caller (gRPC marshaller) always wraps a byte[];
        // for the rare non-array-backed memory we copy once.
        byte[] backing;
        int backingOffset;
        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(payload, out var seg)
            && seg.Array is { } backingArray)
        {
            backing = backingArray;
            backingOffset = seg.Offset;
        }
        else
        {
            backing = payload.ToArray();
            backingOffset = 0;
        }

        var span = payload.Span;
        var cursor = EncodedBatchHeader.WireSize;
        treeName = ReadLengthPrefixedUtf8(payload, span, ref cursor, "treeName");
        originClusterId = ReadLengthPrefixedUtf8(payload, span, ref cursor, "originClusterId");

        var segments = parsed.EntryCount == 0
            ? Array.Empty<ArraySegment<byte>>()
            : new ArraySegment<byte>[parsed.EntryCount];

        for (var i = 0; i < parsed.EntryCount; i++)
        {
            if (cursor + 4 > payload.Length)
            {
                throw new ArgumentException(
                    $"Framing payload is truncated at the length prefix for entry {i} of {parsed.EntryCount}; expected at least 4 more bytes at offset {cursor}.",
                    nameof(payload));
            }
            var length = BinaryPrimitives.ReadInt32LittleEndian(span[cursor..(cursor + 4)]);
            cursor += 4;
            // Widen to long before summing: length is an attacker-controllable
            // wire field (up to int.MaxValue), so cursor + length can overflow to
            // a negative value and slip past this guard, downgrading the precise
            // framing rejection into a raw slice exception. Mirrors the
            // compressed-body check above.
            if (length < 0 || (long)cursor + length > payload.Length)
            {
                throw new ArgumentException(
                    $"Framing payload is truncated at the body for entry {i} of {parsed.EntryCount}; declared length {length} would overrun the payload (remaining {payload.Length - cursor} bytes).",
                    nameof(payload));
            }
            segments[i] = new ArraySegment<byte>(backing, backingOffset + cursor, length);
            cursor += length;
        }

        header = parsed;
        entries = segments;
        return true;
    }

    private static void ParseTailIntoSegments(
        byte[] tail,
        int entryCount,
        out string treeName,
        out string originClusterId,
        out ArraySegment<byte>[] segments)
    {
        var span = tail.AsSpan();
        var cursor = 0;
        treeName = ReadLengthPrefixedUtf8FromTail(tail, span, ref cursor, "treeName");
        originClusterId = ReadLengthPrefixedUtf8FromTail(tail, span, ref cursor, "originClusterId");

        segments = entryCount == 0
            ? Array.Empty<ArraySegment<byte>>()
            : Bound(entryCount, tail.Length);

        for (var i = 0; i < entryCount; i++)
        {
            if (cursor + 4 > tail.Length)
            {
                throw new ArgumentException(
                    $"Inflated framing tail is truncated at the length prefix for entry {i} of {entryCount}; expected at least 4 more bytes at offset {cursor}.",
                    nameof(tail));
            }
            var length = BinaryPrimitives.ReadInt32LittleEndian(span[cursor..(cursor + 4)]);
            cursor += 4;
            // Widen to long before summing: length is an attacker-controllable
            // wire field, so cursor + length can overflow past this guard.
            if (length < 0 || (long)cursor + length > tail.Length)
            {
                throw new ArgumentException(
                    $"Inflated framing tail is truncated at the body for entry {i} of {entryCount}; declared length {length} would overrun the tail (remaining {tail.Length - cursor} bytes).",
                    nameof(tail));
            }
            segments[i] = new ArraySegment<byte>(tail, cursor, length);
            cursor += length;
        }
    }

    /// <summary>
    /// Allocates the per-entry segment array after bounding
    /// <paramref name="entryCount"/> against an adversarial value. Every entry
    /// contributes at least a 4-byte length prefix, so an entry count larger
    /// than <paramref name="tailLength"/> / 4 cannot be satisfied and the
    /// payload is necessarily truncated. Rejecting it here turns a
    /// multi-gigabyte array allocation (the entry count is read straight off the
    /// wire, independent of the real payload length) into a cheap, catchable
    /// <see cref="ArgumentException"/> raised before the allocation.
    /// </summary>
    private static ArraySegment<byte>[] Bound(int entryCount, int tailLength)
    {
        var maxPossibleEntries = tailLength / sizeof(int);
        if (entryCount > maxPossibleEntries)
        {
            throw new ArgumentException(
                $"Inflated framing tail reports {entryCount} entries but the {tailLength}-byte "
                + $"tail can hold at most {maxPossibleEntries}; payload is truncated or corrupt.",
                nameof(entryCount));
        }
        return new ArraySegment<byte>[entryCount];
    }

    private static string ReadLengthPrefixedUtf8(
        ReadOnlyMemory<byte> payload,
        ReadOnlySpan<byte> span,
        ref int cursor,
        string fieldName)
    {
        if (cursor + 4 > payload.Length)
        {
            throw new ArgumentException(
                $"Framing payload is truncated at the length prefix for {fieldName}; expected at least 4 more bytes at offset {cursor}.",
                nameof(payload));
        }
        var length = BinaryPrimitives.ReadInt32LittleEndian(span[cursor..(cursor + 4)]);
        cursor += 4;
        // Widen to long before summing: length is an attacker-controllable wire
        // field, so cursor + length can overflow past this guard.
        if (length < 0 || (long)cursor + length > payload.Length)
        {
            throw new ArgumentException(
                $"Framing payload is truncated at the body for {fieldName}; declared length {length} would overrun the payload (remaining {payload.Length - cursor} bytes).",
                nameof(payload));
        }
        var value = length == 0
            ? string.Empty
            : System.Text.Encoding.UTF8.GetString(span.Slice(cursor, length));
        cursor += length;
        return value;
    }

    private static string ReadLengthPrefixedUtf8FromTail(
        byte[] tail,
        ReadOnlySpan<byte> span,
        ref int cursor,
        string fieldName)
    {
        if (cursor + 4 > tail.Length)
        {
            throw new ArgumentException(
                $"Inflated framing tail is truncated at the length prefix for {fieldName}; expected at least 4 more bytes at offset {cursor}.",
                nameof(tail));
        }
        var length = BinaryPrimitives.ReadInt32LittleEndian(span[cursor..(cursor + 4)]);
        cursor += 4;
        // Widen to long before summing: length is an attacker-controllable wire
        // field, so cursor + length can overflow past this guard.
        if (length < 0 || (long)cursor + length > tail.Length)
        {
            throw new ArgumentException(
                $"Inflated framing tail is truncated at the body for {fieldName}; declared length {length} would overrun the tail (remaining {tail.Length - cursor} bytes).",
                nameof(tail));
        }
        var value = length == 0
            ? string.Empty
            : System.Text.Encoding.UTF8.GetString(span.Slice(cursor, length));
        cursor += length;
        return value;
    }

    private static void EncodeUncompressedFraming(
        in EncodedBatchHeader header,
        string treeName,
        string originClusterId,
        ReadOnlyMemory<ArraySegment<byte>> entries,
        IBufferWriter<byte> writer)
    {
        if (treeName.Length == 0)
        {
            throw new ArgumentException("treeName must be non-empty.", nameof(treeName));
        }
        if (originClusterId.Length == 0)
        {
            throw new ArgumentException("originClusterId must be non-empty.", nameof(originClusterId));
        }
        if (header.EntryCount != entries.Length)
        {
            throw new ArgumentException(
                $"{nameof(EncodedBatchHeader)}.{nameof(EncodedBatchHeader.EntryCount)} ({header.EntryCount}) does not match entries.Length ({entries.Length}).",
                nameof(header));
        }

        var headerSpan = writer.GetSpan(EncodedBatchHeader.WireSize);
        header.WriteTo(headerSpan);
        writer.Advance(EncodedBatchHeader.WireSize);

        WriteUncompressedTail(writer, treeName, originClusterId, entries);
    }

    private static void WriteUncompressedTail(
        IBufferWriter<byte> writer,
        string treeName,
        string originClusterId,
        ReadOnlyMemory<ArraySegment<byte>> entries)
    {
        WriteLengthPrefixedUtf8(writer, treeName);
        WriteLengthPrefixedUtf8(writer, originClusterId);
        var segments = entries.Span;
        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            var lengthSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(lengthSpan, segment.Count);
            writer.Advance(4);
            if (segment.Count > 0)
            {
                var dest = writer.GetSpan(segment.Count);
                segment.AsSpan().CopyTo(dest);
                writer.Advance(segment.Count);
            }
        }
    }

    private static void WriteLengthPrefixedUtf8(IBufferWriter<byte> writer, string value)
    {
        var maxBytes = System.Text.Encoding.UTF8.GetMaxByteCount(value.Length);
        var span = writer.GetSpan(4 + maxBytes);
        var written = System.Text.Encoding.UTF8.GetBytes(value, span[4..]);
        BinaryPrimitives.WriteInt32LittleEndian(span[0..4], written);
        writer.Advance(4 + written);
    }

    /// <summary>
    /// Computes the exact byte size of the uncompressed framing tail
    /// for a given (treeName, originClusterId, entries) triple. Used
    /// by the compressed-encode path to rent a single right-sized
    /// buffer from <see cref="ArrayPool{T}.Shared"/> instead of
    /// growing an <see cref="ArrayBufferWriter{T}"/>.
    /// </summary>
    private static int ComputeTailSize(
        string treeName,
        string originClusterId,
        ReadOnlySpan<ArraySegment<byte>> entries)
    {
        var size = 4 + System.Text.Encoding.UTF8.GetByteCount(treeName);
        size += 4 + System.Text.Encoding.UTF8.GetByteCount(originClusterId);
        for (var i = 0; i < entries.Length; i++)
        {
            size += 4 + entries[i].Count;
        }
        return size;
    }

    /// <summary>
    /// Writes the canonical uncompressed tail layout into a single
    /// pre-sized destination span. Used by the compressed-encode
    /// path; the canonical uncompressed-encode path still goes
    /// through <see cref="WriteUncompressedTail"/> + the caller's
    /// <see cref="IBufferWriter{T}"/> so its allocation profile is
    /// unchanged.
    /// </summary>
    private static void WriteTailIntoSpan(
        Span<byte> destination,
        string treeName,
        string originClusterId,
        ReadOnlySpan<ArraySegment<byte>> entries)
    {
        var cursor = 0;
        cursor += WriteLengthPrefixedUtf8ToSpan(destination[cursor..], treeName);
        cursor += WriteLengthPrefixedUtf8ToSpan(destination[cursor..], originClusterId);
        for (var i = 0; i < entries.Length; i++)
        {
            var entry = entries[i];
            BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(cursor, 4), entry.Count);
            cursor += 4;
            if (entry.Count > 0)
            {
                entry.AsSpan().CopyTo(destination.Slice(cursor, entry.Count));
                cursor += entry.Count;
            }
        }
    }

    private static int WriteLengthPrefixedUtf8ToSpan(Span<byte> destination, string value)
    {
        var written = System.Text.Encoding.UTF8.GetBytes(value, destination[4..]);
        BinaryPrimitives.WriteInt32LittleEndian(destination[0..4], written);
        return 4 + written;
    }
}
