using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// A stable, deterministic fingerprint of the <i>drift-significant</i> fields of
/// a grain-index declaration: the hash the index registry persists so a later
/// silo start can tell whether the declaration it is holding still describes the
/// data already written under it.
/// <para>
/// A grain index is a materialised projection whose on-tree encoding is a
/// function of its declaration. Change which properties are projected, their
/// declared types, their order, the codec that encodes the grain key, or the
/// tree the entries live in, and every entry already written becomes
/// inconsistent with the reader that would now query it. The fingerprint reduces
/// exactly those fields to sixteen bytes so the comparison is a cheap equality
/// check rather than a structural walk.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// The hash is <c>XxHash128</c> - the same non-cryptographic digest hash the core
/// library uses for its tree key digests (see <see cref="LeafProjectionDigest"/>
/// and <see cref="ViewDigest"/>) - taken over a canonical, length-prefixed byte
/// encoding of the drift-significant fields rather than over any serialized
/// form. Hashing a canonical encoding rather than a serializer's output is what
/// makes the value stable across process restarts, across machines, and across
/// serializer churn: adding an unrelated <c>[Id(n)]</c> member to a persisted
/// type cannot move the fingerprint, and only a real declaration change can.
/// Every string is fed as a little-endian <c>int32</c> UTF-8 byte count followed
/// by its bytes, so no two distinct field sequences can produce the same byte
/// stream by running together.
/// </para>
/// <para>
/// The leading <see cref="CurrentVersion"/> stamp is part of the hashed stream.
/// It exists so that a future change to the entry encoding or the on-tree
/// ordering scheme - neither of which is visible in the fields listed below -
/// can invalidate every stored fingerprint deliberately by incrementing the
/// constant, rather than silently leaving indexes readable by a reader that no
/// longer agrees with them.
/// </para>
/// <para>
/// <b>What the fingerprint covers, and what it deliberately does not.</b> The
/// hashed fields are the drift-<i>breaking</i> ones classified by
/// <see cref="GrainIndexDriftClassification"/>: the backing tree name, the
/// indexed grain interface type, the projected state type, the key-codec
/// identity, and the ordered projected-property set with each property's
/// declared type. Drift-safe fields - today the cross-cluster replication opt-in
/// - are excluded by construction, so flipping one leaves the fingerprint equal
/// and the reconciler takes its "update the stored record" branch instead of its
/// "reject" branch. The fingerprint is a drift-detection value, not an
/// authentication tag.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexFingerprint)]
public readonly record struct GrainIndexFingerprint
{
    /// <summary>
    /// The version of the canonical encoding this build stamps into every
    /// fingerprint it computes. Incrementing it invalidates every stored
    /// fingerprint, which is the deliberate way to force a rebuild when a
    /// change outside the hashed fields - the entry encoding or the on-tree
    /// ordering scheme - makes existing index data unreadable.
    /// </summary>
    public const int CurrentVersion = 1;

    /// <summary>The number of bytes an <c>XxHash128</c> digest occupies.</summary>
    private const int HashSize = 16;

    /// <summary>
    /// The largest UTF-8 byte count fed from the stack before renting from
    /// <see cref="ArrayPool{T}"/> instead. Type and property names are far
    /// shorter than this in practice.
    /// </summary>
    private const int StackFeedLimit = 256;

    /// <summary>
    /// Initialises a fingerprint from an already-computed value.
    /// </summary>
    /// <param name="value">
    /// The uppercase hexadecimal rendering of the digest. Must not be
    /// <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is <c>null</c>.</exception>
    public GrainIndexFingerprint(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        Value = value;
    }

    /// <summary>
    /// The digest rendered as uppercase hexadecimal (32 characters for the
    /// 16-byte <c>XxHash128</c> output). Stored as text rather than as a
    /// <c>byte[]</c> so that the compiler-generated record-struct equality
    /// compares the fingerprint by content - an array member would compare by
    /// reference, which is never what a drift check wants.
    /// <para>
    /// Reads as <see cref="string.Empty"/> on a default-constructed value, which
    /// is the "no fingerprint" sentinel and never equals a computed one. The
    /// getter normalises rather than relying on the initialiser because
    /// <c>default(GrainIndexFingerprint)</c> bypasses every constructor, and a
    /// non-nullable property that can hand back <c>null</c> is a trap for a log
    /// line or a diagnostic message.
    /// </para>
    /// </summary>
    [Id(0)]
    public string Value
    {
        get => field ?? string.Empty;
        init;
    } = string.Empty;

    /// <summary>
    /// Computes the fingerprint of <paramref name="descriptor"/> combined with
    /// <paramref name="keyCodecId"/>.
    /// </summary>
    /// <param name="descriptor">
    /// The persisted shape of the declaration. Must not be <c>null</c>.
    /// </param>
    /// <param name="keyCodecId">
    /// The stable identity of the codec that encodes the indexed grain's key,
    /// as produced by <see cref="GrainIndexKeyCodecIdentity.For(IGrainKeyCodec)"/>.
    /// Must not be <c>null</c>.
    /// </param>
    /// <returns>The fingerprint of the drift-significant fields.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public static GrainIndexFingerprint Compute(GrainIndexDescriptor descriptor, string keyCodecId)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(keyCodecId);

        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[4];

        BinaryPrimitives.WriteInt32LittleEndian(scratch, CurrentVersion);
        hasher.Append(scratch);

        FeedString(hasher, descriptor.TreeName, scratch);
        FeedString(hasher, descriptor.GrainInterfaceTypeName, scratch);
        FeedString(hasher, descriptor.StateTypeName, scratch);
        FeedString(hasher, keyCodecId, scratch);

        var properties = descriptor.Properties;
        BinaryPrimitives.WriteInt32LittleEndian(scratch, properties.Count);
        hasher.Append(scratch);

        // Declaration order is hashed, not sorted away: the projected set is an
        // ordered tuple in the entry encoding, so reordering it is as breaking
        // as replacing a member of it.
        for (var i = 0; i < properties.Count; i++)
        {
            var property = properties[i];
            FeedString(hasher, property.Name, scratch);
            FeedString(hasher, property.PropertyTypeName, scratch);
        }

        Span<byte> digest = stackalloc byte[HashSize];
        if (!hasher.TryGetHashAndReset(digest, out var written) || written != HashSize)
        {
            // Defensive: XxHash128 always produces 16 bytes into a 16-byte span.
            throw new InvalidOperationException(
                "XxHash128 did not produce a 16-byte grain-index fingerprint.");
        }

        return new GrainIndexFingerprint(Convert.ToHexString(digest));
    }

    /// <summary>
    /// Returns the hexadecimal digest, so a fingerprint renders readably in a
    /// log line or a drift message.
    /// </summary>
    /// <returns>The value of <see cref="Value"/>.</returns>
    public override string ToString() => Value;

    /// <summary>
    /// Appends <paramref name="value"/> to <paramref name="hasher"/> as a
    /// little-endian <c>int32</c> UTF-8 byte count followed by those bytes, so
    /// two adjacent fields can never run together into the same byte stream as a
    /// different pair.
    /// </summary>
    private static void FeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(scratch, byteCount);
        hasher.Append(scratch);
        if (byteCount == 0)
        {
            return;
        }

        if (byteCount <= StackFeedLimit)
        {
            Span<byte> buffer = stackalloc byte[StackFeedLimit];
            var written = Encoding.UTF8.GetBytes(value, buffer);
            hasher.Append(buffer[..written]);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var written = Encoding.UTF8.GetBytes(value, rented);
            hasher.Append(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
}
