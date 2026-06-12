namespace Orleans.Lattice.Replication;

/// <summary>
/// Outcome of comparing a local <see cref="LeafProjectionDigest"/> against
/// a remote peer's digest for the same shard during an anti-entropy
/// digest probe pass. Used as the <c>outcome</c> tag value on the
/// per-comparison counter.
/// </summary>
public enum DigestProbeOutcome
{
    /// <summary>
    /// Versions matched and the digest hashes were byte-identical: the
    /// two clusters have applied the same prefix of the same WAL for
    /// this shard.
    /// </summary>
    Match,

    /// <summary>
    /// Versions matched but the digest hashes differed: the two clusters
    /// have diverged for this shard. The per-comparison counter records
    /// this outcome and the dedicated mismatch counter is incremented.
    /// </summary>
    Mismatch,

    /// <summary>
    /// The local and remote digests carry different contribution-function
    /// <see cref="LeafProjectionDigest.Version"/> values, so their hashes
    /// are not comparable. No mismatch is raised; the comparison is
    /// recorded so operators can observe the skew.
    /// </summary>
    VersionSkew,

    /// <summary>
    /// The remote peer could not produce a digest (projection-digest
    /// maintenance disabled or latched off remotely). No mismatch is
    /// raised; the comparison is recorded so operators can observe the
    /// non-comparable peer.
    /// </summary>
    RemoteUnavailable,
}
