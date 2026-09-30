using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Membership;

/// <summary>
/// The <see cref="MembershipResolutionCache"/> key for a
/// <see cref="LatticeCredential"/>.
/// </summary>
/// <remarks>
/// <para>
/// Resolution is a function of the <em>whole</em> credential and not of its
/// token alone: <see cref="ILatticeCredentialAuthenticator.CanHandle"/> selects
/// on <see cref="LatticeCredential.Scheme"/> - every shipped JWT authenticator
/// does - and the credential contract documents
/// <see cref="LatticeCredential.PrincipalId"/> and
/// <see cref="LatticeCredential.Metadata"/> as inputs an authenticator may
/// resolve from without re-parsing the token. A cache keyed on the token alone
/// therefore serves one credential's subject to a different credential, which
/// is an identity confusion rather than a stale read: the warm path returns
/// before any authenticator is consulted, so nothing downstream can notice.
/// </para>
/// <para>
/// The key is a value type carrying each field separately, so it is injective
/// by construction rather than by an encoding that has to be argued about: no
/// concatenation happens, and therefore no choice of token and scheme can be
/// spliced into the key of a different credential. It also keeps the warm path
/// allocation-free, which a framed string key would not.
/// <see cref="LatticeCredential.Metadata"/> is caller-supplied and unbounded, so
/// it is reduced to a fixed-width digest over a canonical, length-prefixed
/// encoding of its sorted pairs; the length prefixes are what stop two distinct
/// bags encoding identically, and the sort is what stops one bag encoding two
/// ways.
/// </para>
/// </remarks>
internal readonly record struct MembershipCacheKey
{
    /// <summary>The U+001F field separator, ASCII and so one UTF-8 byte.</summary>
    private const byte UnitSeparator = 0x1f;

    /// <summary>
    /// Upper bound on the bytes the two length prefixes and their separators add
    /// to one pair: an <see cref="int"/> is at most 11 decimal digits, and each of
    /// the two prefixes contributes those digits plus one separator byte.
    /// </summary>
    private const int MaxLengthPrefixBytes = (11 + 1) * 2;

    /// <summary>
    /// Canonical-form budget staged on the stack. A constant width keeps the
    /// fixed-size zeroing the JIT unrolls; a metadata bag that does not fit rents
    /// from the shared pool instead.
    /// </summary>
    private const int StackCanonicalBytes = 256;

    private MembershipCacheKey(string? token, string? scheme, string? principalId, string? metadataDigest)
    {
        Token = token;
        Scheme = scheme;
        PrincipalId = principalId;
        MetadataDigest = metadataDigest;
    }

    /// <summary>The credential's opaque token, or <c>null</c> when it carries none.</summary>
    internal string? Token { get; }

    /// <summary>The credential's scheme hint, which selects the authenticator.</summary>
    internal string? Scheme { get; }

    /// <summary>The credential's pre-resolved principal id, when the edge supplied one.</summary>
    internal string? PrincipalId { get; }

    /// <summary>
    /// A fixed-width digest of the credential's metadata bag, or <c>null</c>
    /// when it carries none. An empty bag digests to
    /// <see cref="string.Empty"/>, which is distinct from <c>null</c>.
    /// </summary>
    internal string? MetadataDigest { get; }

    /// <summary>
    /// Builds the key for a credential that carries nothing but
    /// <paramref name="token"/>. Callers holding a real
    /// <see cref="LatticeCredential"/> must use <see cref="For"/> instead, so
    /// that a field the credential does carry cannot be dropped from the key.
    /// </summary>
    /// <param name="token">The opaque token.</param>
    internal static MembershipCacheKey ForToken(string? token) => new(token, null, null, null);

    /// <summary>
    /// Builds the key covering every field of <paramref name="credential"/> that
    /// an authenticator is permitted to resolve from.
    /// </summary>
    /// <param name="credential">The ambient caller credential.</param>
    internal static MembershipCacheKey For(in LatticeCredential credential) => new(
        credential.Token,
        credential.Scheme,
        credential.PrincipalId,
        DigestMetadata(credential.Metadata));

    /// <summary>
    /// Reduces a metadata bag to a fixed-width digest over a canonical encoding.
    /// Pairs are ordered so that a bag cannot encode two ways, and both halves
    /// of every pair are length-prefixed so that two distinct bags cannot encode
    /// the same way.
    /// </summary>
    /// <param name="metadata">The credential's metadata bag, possibly <c>null</c>.</param>
    private static string? DigestMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null)
        {
            return null;
        }

        if (metadata.Count == 0)
        {
            return string.Empty;
        }

        var pairs = new List<KeyValuePair<string, string>>(metadata);
        pairs.Sort(static (left, right) =>
        {
            var byKey = string.CompareOrdinal(left.Key, right.Key);
            return byKey != 0 ? byKey : string.CompareOrdinal(left.Value, right.Value);
        });

        // The canonical form is written straight to UTF-8 and hashed from that
        // buffer. Staging it as a StringBuilder, materialising it with ToString,
        // and re-encoding it with GetBytes allocated three intermediates that
        // nothing outlives the call, on a path that runs for every credential
        // cache probe - which the security invariant on steady-state auth paths
        // (allocate nothing avoidable) rules out. The byte stream is unchanged:
        // UTF-8 of a concatenation equals the concatenation of the parts' UTF-8
        // here, because a length digit or the U+001F separator always falls
        // between two caller-supplied strings, so no surrogate pair can straddle
        // a part boundary.
        var maxBytes = 0;
        foreach (var pair in pairs)
        {
            maxBytes += Encoding.UTF8.GetMaxByteCount(pair.Key.Length + pair.Value.Length)
                + MaxLengthPrefixBytes;
        }

        byte[]? rented = null;
        var canonical = maxBytes <= StackCanonicalBytes
            ? stackalloc byte[StackCanonicalBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxBytes));
        try
        {
            var written = 0;
            foreach (var pair in pairs)
            {
                written += WriteLengthPrefix(canonical[written..], pair.Key.Length);
                written += Encoding.UTF8.GetBytes(pair.Key, canonical[written..]);
                written += WriteLengthPrefix(canonical[written..], pair.Value.Length);
                written += Encoding.UTF8.GetBytes(pair.Value, canonical[written..]);
            }

            Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
            SHA256.HashData(canonical[..written], digest);
            return Convert.ToHexString(digest);
        }
        finally
        {
            if (rented is not null)
            {
                // Caller-supplied metadata values may be sensitive, so the buffer
                // is cleared rather than handed back to the pool still populated.
                ArrayPool<byte>.Shared.Return(rented, clearArray: true);
            }
        }
    }

    /// <summary>
    /// Writes the decimal <paramref name="length"/> followed by the U+001F field
    /// separator into <paramref name="destination"/>, returning the bytes written.
    /// Both are ASCII, so the UTF-8 encoding is the literal characters.
    /// </summary>
    private static int WriteLengthPrefix(Span<byte> destination, int length)
    {
        length.TryFormat(destination, out var written);
        destination[written] = UnitSeparator;
        return written + 1;
    }
}
