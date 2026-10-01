using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The content digest of an app manifest: the identity an operator reviews and an install pins, so
/// a manifest that changes between review and install - a new bridge operation, role, tree or UI
/// bundle - is refused rather than consented to unseen.
/// </summary>
/// <remarks>
/// The digest is the SHA-256, as lower-case hex, of the manifest's JSON in the manifest file
/// shape (camel-case members, the manifest converters) with null members omitted. It covers every
/// declaration, the UI bundle digest and the provenance, so a change anywhere changes it. It is a
/// review pin computed by one server for that server, not a stored artifact identity: two server
/// versions whose manifest model differs may disagree, and that refuses an install whose review and
/// commit straddle the upgrade, which fails closed.
/// </remarks>
internal static class AppManifestDigest
{
    /// <summary>The length of a digest: SHA-256 as lower-case hex.</summary>
    public const int Length = 64;

    private static readonly JsonSerializerOptions DigestOptions = new(AppManifestParser.Options)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Computes the digest of <paramref name="manifest"/>, with its identity's provenance replaced by
    /// <paramref name="provenance"/> when one is supplied - the provenance the source vouched for,
    /// which an install records.
    /// </summary>
    /// <param name="manifest">The manifest to digest.</param>
    /// <param name="provenance">The effective provenance, or <see langword="null"/> to keep the manifest's own.</param>
    /// <returns>The digest, or <see langword="null"/> when the manifest cannot be written in the manifest shape.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="manifest"/> is <see langword="null"/>.</exception>
    public static string? Compute(AppManifest manifest, AppProvenance? provenance = null)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        if (provenance is not null && !ReferenceEquals(provenance, manifest.Identity.Provenance))
        {
            manifest = manifest with { Identity = manifest.Identity with { Provenance = provenance } };
        }

        byte[] utf8;
        try
        {
            // Cold path (describe and install only): one buffer for the manifest JSON.
            utf8 = JsonSerializer.SerializeToUtf8Bytes(manifest, DigestOptions);
        }
        catch (JsonException)
        {
            // A manifest the manifest converters refuse to write (for example an invalid role
            // operation mask) has no digest; a pinned install of it is then refused.
            return null;
        }

        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(utf8, hash);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>Reports whether <paramref name="value"/> has the shape of a digest: 64 lower-case hex characters.</summary>
    /// <param name="value">The candidate digest.</param>
    /// <returns><see langword="true"/> when the value is well formed.</returns>
    public static bool IsWellFormed(string? value)
    {
        if (value is null || value.Length != Length)
        {
            return false;
        }

        foreach (var c in value)
        {
            if (c is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }
}
