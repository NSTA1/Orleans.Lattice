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

        var canonical = new StringBuilder();
        foreach (var pair in pairs)
        {
            canonical.Append(pair.Key.Length).Append('\u001f').Append(pair.Key)
                .Append(pair.Value.Length).Append('\u001f').Append(pair.Value);
        }

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(canonical.ToString())));
    }
}
