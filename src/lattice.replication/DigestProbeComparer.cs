namespace Orleans.Lattice.Replication;

/// <summary>
/// Pure comparison helper for the anti-entropy digest probe. Compares a
/// locally-read <see cref="LeafProjectionDigest"/> against a remote
/// peer's <see cref="DigestProbeResponse"/> and classifies the result as
/// a <see cref="DigestProbeOutcome"/>. Stateless and allocation-free.
/// </summary>
public static class DigestProbeComparer
{
    /// <summary>
    /// Classifies the comparison between the local digest and the remote
    /// peer's probe response.
    /// </summary>
    /// <param name="local">The local cluster's digest for the shard.</param>
    /// <param name="remote">The remote peer's probe response.</param>
    /// <returns>
    /// <see cref="DigestProbeOutcome.RemoteUnavailable"/> when the remote
    /// could not produce a digest; <see cref="DigestProbeOutcome.VersionSkew"/>
    /// when the contribution-function versions differ;
    /// <see cref="DigestProbeOutcome.Match"/> when the versions agree and
    /// the hashes are byte-identical; otherwise
    /// <see cref="DigestProbeOutcome.Mismatch"/>.
    /// </returns>
    public static DigestProbeOutcome Compare(LeafProjectionDigest local, DigestProbeResponse remote)
    {
        if (!remote.DigestAvailable)
        {
            return DigestProbeOutcome.RemoteUnavailable;
        }

        if (local.Version != remote.Digest.Version)
        {
            return DigestProbeOutcome.VersionSkew;
        }

        var localHash = local.Hash ?? Array.Empty<byte>();
        var remoteHash = remote.Digest.Hash ?? Array.Empty<byte>();
        return localHash.AsSpan().SequenceEqual(remoteHash)
            ? DigestProbeOutcome.Match
            : DigestProbeOutcome.Mismatch;
    }
}
