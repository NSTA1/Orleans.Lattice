namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Whether a tree-administration snapshot locks the source tree during the copy.
/// Mirrors the core snapshot engine's own mode as a transport-agnostic value the
/// tree-administration facade can accept without the abstractions package taking a
/// dependency on the core library.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeSnapshotMode)]
public enum TreeSnapshotMode
{
    /// <summary>
    /// The source tree is marked deleted for the duration of the copy, blocking
    /// reads and writes until the snapshot completes and the shards are unmarked.
    /// Guarantees a fully consistent point-in-time copy.
    /// </summary>
    Offline = 0,

    /// <summary>
    /// The source tree stays available for reads and writes throughout. Every
    /// mutation the source accepts before the snapshot completes is shadow-forwarded
    /// to the destination, and a background per-shard drain copies existing entries;
    /// last-writer-wins convergence guarantees no data loss with no distributed lock.
    /// </summary>
    Online = 1,
}
