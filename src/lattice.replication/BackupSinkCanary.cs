using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Encodes and decodes the tiny per-cluster marker the cross-cluster backup sink
/// sharing probe writes into its own configured backup sink. Reading a peer's
/// marker back out of <b>this</b> cluster's sink is what proves the two clusters
/// resolve the same physical store: sharing is a deployment fact that no local
/// configuration inspection can settle, because two regions can hold
/// identical-looking connection strings that point at different accounts.
/// <para>
/// The marker is a fixed, bounded ASCII record so decoding never allocates a
/// parser and a corrupt or foreign blob is rejected rather than interpreted. The
/// artifact id is derived by hashing the cluster id rather than embedding it,
/// because a cluster id is unconstrained free text while an artifact id must
/// survive both the Azure blob-name rules and the in-cluster sink's key
/// separator. The plaintext cluster id travels in the body so an operator reading
/// the sink by hand can tell which cluster wrote which marker.
/// </para>
/// </summary>
internal static class BackupSinkCanary
{
    /// <summary>
    /// The magic first line every marker starts with. A blob that does not begin
    /// with it is not a marker and is never treated as evidence of sharing.
    /// </summary>
    internal const string Magic = "lattice-backup-sink-canary";

    /// <summary>The artifact-id prefix that namespaces markers inside the sink.</summary>
    internal const string ArtifactIdPrefix = "sys-sink-canary-";

    /// <summary>
    /// The maximum marker size accepted on read. A marker is under a hundred bytes;
    /// the cap stops a large or hostile blob that happens to occupy the id from
    /// being buffered.
    /// </summary>
    internal const int MaxBytes = 4096;

    /// <summary>
    /// Derives the deterministic, collision-resistant artifact id that holds
    /// <paramref name="clusterId"/>'s marker. Both the writer and every reader
    /// derive the id from a <b>locally configured</b> cluster id, never from
    /// anything read out of the sink, so a blob can only ever confirm the identity
    /// the caller already expected.
    /// </summary>
    /// <param name="clusterId">The cluster whose marker id to derive. Must not be <c>null</c> or empty.</param>
    /// <returns>The sink artifact id holding that cluster's marker.</returns>
    /// <exception cref="ArgumentException"><paramref name="clusterId"/> is <c>null</c> or empty.</exception>
    internal static string ArtifactId(string clusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(clusterId);
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(Encoding.UTF8.GetBytes(clusterId), digest);
        return string.Concat(ArtifactIdPrefix, Convert.ToHexStringLower(digest[..16]));
    }

    /// <summary>
    /// Renders the marker body attesting that <paramref name="clusterId"/> wrote to
    /// this sink at <paramref name="writtenAtUtc"/>.
    /// </summary>
    /// <param name="clusterId">The writing cluster's id. Must not be <c>null</c> or empty.</param>
    /// <param name="writtenAtUtc">The write timestamp recorded in the marker.</param>
    /// <returns>The encoded marker bytes.</returns>
    /// <exception cref="ArgumentException"><paramref name="clusterId"/> is <c>null</c> or empty.</exception>
    internal static byte[] Encode(string clusterId, DateTimeOffset writtenAtUtc)
    {
        ArgumentException.ThrowIfNullOrEmpty(clusterId);
        return Encoding.UTF8.GetBytes($"{Magic}\n{clusterId}\n{writtenAtUtc:O}\n");
    }

    /// <summary>
    /// Tests whether <paramref name="body"/> is a well-formed marker written by
    /// <paramref name="expectedClusterId"/>. Fails closed: a body that is oversized,
    /// malformed, lacks the magic line, or attests to a different cluster is not
    /// proof of sharing and returns <see langword="false"/>. The expected cluster id
    /// is supplied by the caller from local configuration, so a blob can never
    /// nominate itself as belonging to a peer it does not name.
    /// </summary>
    /// <param name="body">The bytes read back from the sink. </param>
    /// <param name="expectedClusterId">
    /// The locally configured peer id the marker must attest to. Must not be
    /// <c>null</c> or empty.
    /// </param>
    /// <returns><see langword="true"/> only when the body provably attests to that cluster.</returns>
    /// <exception cref="ArgumentException"><paramref name="expectedClusterId"/> is <c>null</c> or empty.</exception>
    internal static bool Attests(ReadOnlySpan<byte> body, string expectedClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(expectedClusterId);
        if (body.Length is 0 or > MaxBytes)
        {
            return false;
        }

        // Encoding.UTF8 uses the replacement fallback, so malformed bytes decode to
        // U+FFFD rather than throwing - and a replaced byte can never equal the
        // magic line or a peer id, so the comparison below still fails closed.
        //
        // The decode-and-split allocates a string plus a small string[]. That is
        // deliberate: this is a fail-closed security check that runs once per peer
        // on a cold path (silo start, then once per six-hourly health sweep), and a
        // hand-rolled span scan would trade an obviously-correct comparison for an
        // off-by-one risk in exactly the code that must never wrongly return true.
        var text = Encoding.UTF8.GetString(body);
        var lines = text.Split('\n');
        return lines.Length >= 2
            && string.Equals(lines[0], Magic, StringComparison.Ordinal)
            && string.Equals(lines[1], expectedClusterId, StringComparison.Ordinal);
    }
}
