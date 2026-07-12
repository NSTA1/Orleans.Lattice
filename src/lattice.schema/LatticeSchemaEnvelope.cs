using System.Buffers.Binary;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The frozen, self-describing per-value schema-version envelope. For an opted-in
/// tree the write path prepends this fixed header to a value's plain body and the
/// read path strips (and optionally upcasts) it before returning bytes to the
/// caller. The framing mirrors two core disciplines:
/// <list type="bullet">
/// <item><description>the <c>ILatticeCompressor</c> / <see cref="LatticeCompression"/>
/// tag-byte pattern - a plaintext discriminator the reader dispatches on <b>before</b>
/// deciphering the body, so an upcaster can be selected without inflating; and</description></item>
/// <item><description>the durable <c>WalRecord.Mode</c> default-omit discipline - the
/// envelope is emitted <b>only</b> for opted-in trees, so an opted-out / unversioned
/// value carries <b>zero</b> extra bytes and keeps its exact steady-state byte shape.</description></item>
/// </list>
/// <para>
/// <b>Frozen layout (10 bytes, big-endian).</b> This byte layout is a durable wire
/// contract and never changes for a given <see cref="FormatVersion"/>:
/// </para>
/// <list type="table">
/// <item><term>offset 0 (1 byte)</term><description><see cref="Magic"/> = <c>0xFE</c>, a reserved discriminator that is <b>not</b> a valid UTF-8 lead byte, so a stored UTF-8 / JSON body never begins with it and an un-stamped legacy value is unambiguously distinguishable from a stamped one.</description></item>
/// <item><term>offset 1 (1 byte)</term><description><see cref="FormatVersion"/> = <c>0x01</c>, the envelope-format version (distinct from the per-value schema version), reserved so a future header shape can coexist.</description></item>
/// <item><term>offset 2..5 (4 bytes)</term><description>the schema id (<see cref="uint"/>, big-endian) - which logical schema family the value belongs to.</description></item>
/// <item><term>offset 6..9 (4 bytes)</term><description>the schema version (<see cref="uint"/>, big-endian) - the monotonic version the body's shape conforms to.</description></item>
/// <item><term>offset 10..</term><description>the plain value body.</description></item>
/// </list>
/// <para>
/// <b>Discriminator caveat.</b> The single-byte magic cleanly disambiguates only
/// UTF-8 / text bodies (the domain the value-transform IR operates on): because
/// <c>0xFE</c> is never a valid UTF-8 lead byte, a valid UTF-8 body never collides.
/// Opted-in trees are expected to carry UTF-8 (JSON / text) values; a tree of
/// arbitrary binary blobs whose bytes might legitimately begin with <c>0xFE</c>
/// should not opt in to envelope versioning.
/// </para>
/// </summary>
public static class LatticeSchemaEnvelope
{
    /// <summary>
    /// The reserved leading discriminator byte identifying a schema-version
    /// envelope. <c>0xFE</c> is not a valid UTF-8 lead byte, so a stored UTF-8 /
    /// JSON body never begins with it.
    /// </summary>
    public const byte Magic = 0xFE;

    /// <summary>
    /// The envelope-format version (distinct from the per-value schema version).
    /// Reserved so a future header shape can be introduced without ambiguity.
    /// </summary>
    public const byte FormatVersion = 0x01;

    /// <summary>The fixed envelope header length in bytes.</summary>
    public const int HeaderLength = 10;

    private const int SchemaIdOffset = 2;
    private const int VersionOffset = 6;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="value"/> begins with a well-formed
    /// schema-version envelope header (the magic and a recognized format version).
    /// A shorter buffer, a different leading byte, or an unrecognized format
    /// version all return <c>false</c>, so an un-stamped legacy value is treated as
    /// plain (unversioned) bytes.
    /// </summary>
    /// <param name="value">The stored value bytes to inspect.</param>
    /// <returns><c>true</c> when the value is a stamped envelope; otherwise <c>false</c>.</returns>
    public static bool IsEnveloped(ReadOnlySpan<byte> value) =>
        value.Length >= HeaderLength && value[0] == Magic && value[1] == FormatVersion;

    /// <summary>
    /// Prepends a schema-version envelope header to <paramref name="body"/>,
    /// returning a fresh array of length <c>HeaderLength + body.Length</c>.
    /// </summary>
    /// <param name="schemaId">The schema-family id to stamp.</param>
    /// <param name="version">The schema version to stamp.</param>
    /// <param name="body">The plain value body to wrap.</param>
    /// <returns>The enveloped bytes.</returns>
    public static byte[] Encode(uint schemaId, uint version, ReadOnlySpan<byte> body)
    {
        var buffer = new byte[HeaderLength + body.Length];
        buffer[0] = Magic;
        buffer[1] = FormatVersion;
        BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(SchemaIdOffset), schemaId);
        BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(VersionOffset), version);
        body.CopyTo(buffer.AsSpan(HeaderLength));
        return buffer;
    }

    /// <summary>
    /// Reads the envelope header from <paramref name="value"/> when present.
    /// </summary>
    /// <param name="value">The stored value bytes.</param>
    /// <param name="schemaId">The decoded schema id when the method returns <c>true</c>.</param>
    /// <param name="version">The decoded schema version when the method returns <c>true</c>.</param>
    /// <returns><c>true</c> when a header was read; <c>false</c> when the value is not enveloped.</returns>
    public static bool TryReadHeader(ReadOnlySpan<byte> value, out uint schemaId, out uint version)
    {
        if (!IsEnveloped(value))
        {
            schemaId = 0;
            version = 0;
            return false;
        }

        schemaId = BinaryPrimitives.ReadUInt32BigEndian(value.Slice(SchemaIdOffset));
        version = BinaryPrimitives.ReadUInt32BigEndian(value.Slice(VersionOffset));
        return true;
    }

    /// <summary>
    /// Returns the plain body of an enveloped <paramref name="value"/> as a fresh
    /// array (the bytes after the header). The caller must have confirmed the value
    /// is enveloped (for example via <see cref="IsEnveloped"/>); a value shorter
    /// than the header throws.
    /// </summary>
    /// <param name="value">The enveloped stored value bytes. Must not be <c>null</c>.</param>
    /// <returns>The stripped body bytes.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="value"/> is shorter than the envelope header.</exception>
    public static byte[] StripToBody(byte[] value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Length < HeaderLength)
        {
            throw new ArgumentException(
                $"The value is {value.Length} bytes, shorter than the {HeaderLength}-byte envelope header.",
                nameof(value));
        }

        return value.AsSpan(HeaderLength).ToArray();
    }
}
