namespace Orleans.Lattice.Replication;

/// <summary>
/// Why a read-only Merkle-walk drift-localisation pass stopped before narrowing
/// a shard-level mismatch down to a leaf. Carried as the <c>reason</c> tag on
/// the merkle-walk aborted counter.
/// </summary>
public enum MerkleWalkAbortReason
{
    /// <summary>The walk did not abort.</summary>
    None = 0,

    /// <summary>The configured recursion-depth cap was reached.</summary>
    DepthCapExceeded = 1,

    /// <summary>The per-probe byte budget was exhausted.</summary>
    ByteBudgetExceeded = 2,

    /// <summary>A peer could not answer a key-range subtree-digest probe.</summary>
    RemoteUnavailable = 3,

    /// <summary>
    /// Local and remote digests carry different contribution-function versions,
    /// so their hashes are not comparable.
    /// </summary>
    VersionSkew = 4,
}
