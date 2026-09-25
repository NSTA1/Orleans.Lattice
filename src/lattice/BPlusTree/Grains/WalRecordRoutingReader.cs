using System.Text;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Codecs;
using Orleans.Serialization.Session;
using Orleans.Serialization.WireProtocol;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Reads just the routing prefix of an encoded <see cref="WalRecord"/> - its
/// operation and the UTF-8 bytes of its key - so a storage provider can decide
/// whether a <see cref="WalKeyFilter"/> excludes the record without decoding it
/// (issue #3565).
/// <para>
/// <b>Why a hand-written prefix read rather than a narrower deserialize.</b> The
/// value and delta payloads are what a filtered read exists to avoid
/// allocating, and a generated Orleans deserializer cannot stop early: a type
/// declaring only the routing fields still walks the whole record, and it
/// records a placeholder object for every field it does not recognise so that
/// later back-references resolve. So the saving has to come from reading the
/// fields that precede the payload and then <b>stopping</b>. On the excluded
/// path this allocates nothing: the session is pooled, and the key is hashed and
/// compared as bytes and stack-decoded characters, never as a string.
/// </para>
/// <para>
/// <b>The read follows the wire contract, not a layout assumption.</b>
/// <see cref="WalRecord"/> serializes its members in field-id order, and the
/// three routing members are the lowest ids: <see cref="WalRecord.TreeId"/>
/// (<c>0</c>), <see cref="WalRecord.Op"/> (<c>1</c>) and <see cref="WalRecord.Key"/>
/// (<c>2</c>). Field headers carry id deltas, so an omitted default-valued member
/// is handled the way the generated deserializer handles it, the base-type
/// section a generated serializer opens every record with is skipped the same
/// way, and the operation is read with the very codec the record's own
/// serializer uses.
/// </para>
/// <para>
/// <b>Anything unexpected is "unknown", never "excluded".</b> A key written as a
/// back-reference, a wire type the read does not recognise, or an input
/// truncated before the key ends all report that the prefix could not be read,
/// and the caller decodes the record in full and judges that instead. Excluding
/// a record is an optimisation, so a record is only ever excluded on a proof.
/// <c>WalRecordRoutingReaderTests</c> pins the verdict against the full decode
/// over every record shape the encoder produces.
/// </para>
/// </summary>
internal sealed class WalRecordRoutingReader
{
    private const uint TreeIdFieldId = 0;
    private const uint OpFieldId = 1;
    private const uint KeyFieldId = 2;

    private readonly SerializerSessionPool _sessionPool;
    private readonly IFieldCodec<MutationKind> _kindCodec;

    /// <summary>
    /// Creates a reader over the serializer sessions the WAL records were
    /// encoded with.
    /// </summary>
    /// <param name="sessionPool">The silo's serializer session pool. Must not be <see langword="null"/>.</param>
    public WalRecordRoutingReader(SerializerSessionPool sessionPool)
    {
        ArgumentNullException.ThrowIfNull(sessionPool);
        _sessionPool = sessionPool;
        _kindCodec = sessionPool.CodecProvider.GetCodec<MutationKind>();
    }

    /// <summary>
    /// Decides whether <paramref name="filter"/> excludes the record encoded in
    /// <paramref name="encoded"/>, reading only its routing prefix. Allocates
    /// nothing.
    /// </summary>
    /// <param name="encoded">
    /// The encoded record, or a prefix of it. A prefix that ends before the key
    /// does reports <see langword="false"/>.
    /// </param>
    /// <param name="filter">The reader's ownership.</param>
    /// <param name="excluded"><see langword="true"/> when the record is proved excluded.</param>
    /// <returns>
    /// <see langword="true"/> when the prefix was read and
    /// <paramref name="excluded"/> is the verdict; <see langword="false"/> when
    /// it could not be, in which case the caller must decode the record in full.
    /// </returns>
    public bool TryClassify(ReadOnlySpan<byte> encoded, in WalKeyFilter filter, out bool excluded)
    {
        excluded = false;
        using var session = _sessionPool.GetSession();
        var reader = Reader.Create(encoded, session);
        try
        {
            if (!TryReadPrefix(ref reader, out var kind, out var hasKey, out var keyStart, out var keyLength))
            {
                return false;
            }

            excluded = hasKey && filter.ExcludesUtf8(kind, encoded.Slice(keyStart, keyLength));
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // A malformed or truncated prefix. Not excluding is always safe: the
            // caller falls back to the full decode.
            return false;
        }
    }

    /// <summary>
    /// Reads the routing-only projection of the record encoded in
    /// <paramref name="encoded"/> - its operation and key, every other field
    /// default. Allocates the key string, so it is reserved for the one excluded
    /// record per read that a filtered window delivers.
    /// </summary>
    /// <param name="encoded">The encoded record, or a prefix of it that covers the key.</param>
    /// <param name="routingOnly">The routing-only record.</param>
    /// <returns><see langword="false"/> when the prefix could not be read.</returns>
    public bool TryReadRoutingOnly(ReadOnlySpan<byte> encoded, out WalRecord routingOnly)
    {
        routingOnly = default;
        using var session = _sessionPool.GetSession();
        var reader = Reader.Create(encoded, session);
        try
        {
            if (!TryReadPrefix(ref reader, out var kind, out var hasKey, out var keyStart, out var keyLength)
                || !hasKey)
            {
                return false;
            }

            routingOnly = new WalRecord
            {
                TreeId = string.Empty,
                Op = kind,
                Key = Encoding.UTF8.GetString(encoded.Slice(keyStart, keyLength)),
            };
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return false;
        }
    }

    /// <summary>
    /// Walks the record's leading fields up to its key and stops. On success
    /// <paramref name="hasKey"/> says whether a key is present in the input,
    /// and when it is the key's UTF-8 bytes are
    /// <c>[keyStart, keyStart + keyLength)</c> of the input.
    /// </summary>
    private bool TryReadPrefix(
        ref Reader<SpanReaderInput> reader,
        out MutationKind kind,
        out bool hasKey,
        out int keyStart,
        out int keyLength)
    {
        kind = default;
        hasKey = false;
        keyStart = 0;
        keyLength = 0;

        // The record itself is the top-level field, written as a tag-delimited
        // object. Anything else is not a record this read understands.
        var field = reader.ReadFieldHeader();
        if (field.WireType != WireType.TagDelimited)
        {
            return false;
        }

        var id = 0u;
        while (true)
        {
            reader.ReadFieldHeader(ref field);
            if (field.IsEndBaseFields)
            {
                // Generated serializers write a record's base-type fields first
                // and close them with this marker - even for a struct, which has
                // none - and each type in the hierarchy numbers its own fields
                // from zero, so the ids restart here.
                id = 0;
                continue;
            }

            if (field.IsEndObject)
            {
                // No key at all: the record decodes with a default key, which
                // no filter can prove foreign.
                return true;
            }

            id += field.FieldIdDelta;
            switch (id)
            {
                case TreeIdFieldId:
                    if (!TrySkipString(ref reader, field))
                    {
                        return false;
                    }

                    break;

                case OpFieldId:
                    kind = _kindCodec.ReadValue(ref reader, field);
                    if (!WalKeyFilter.IsKeyScoped(kind))
                    {
                        // Only key-scoped kinds can be excluded, so the key is
                        // not needed.
                        return true;
                    }

                    break;

                case KeyFieldId:
                    return TryLocateKey(ref reader, field, out hasKey, out keyStart, out keyLength);

                default:
                    // A higher id before the key means the key was omitted as a
                    // default value.
                    return true;
            }
        }
    }

    private static bool TrySkipString(ref Reader<SpanReaderInput> reader, Field field)
    {
        switch (field.WireType)
        {
            case WireType.Reference:
                _ = reader.ReadVarUInt32();
                return true;

            case WireType.LengthPrefixed:
                var length = reader.ReadVarUInt32();
                if (length > reader.Length - reader.Position)
                {
                    return false;
                }

                reader.Skip(length);
                return true;

            default:
                return false;
        }
    }

    private static bool TryLocateKey(
        ref Reader<SpanReaderInput> reader,
        Field field,
        out bool hasKey,
        out int keyStart,
        out int keyLength)
    {
        hasKey = false;
        keyStart = 0;
        keyLength = 0;

        switch (field.WireType)
        {
            case WireType.Reference:
                // Reference 0 is null: no key, nothing to exclude. Any other id
                // is a back-reference to an object this read never recorded.
                return reader.ReadVarUInt32() == 0;

            case WireType.LengthPrefixed:
                var length = reader.ReadVarUInt32();
                if (length > reader.Length - reader.Position)
                {
                    // Truncated input: the key runs past the prefix supplied.
                    return false;
                }

                hasKey = true;
                keyStart = (int)reader.Position;
                keyLength = (int)length;
                return true;

            default:
                return false;
        }
    }
}
