namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The step a tree snapshot has reached, reported by
/// <see cref="TreeSnapshotStatus.Phase"/>.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeSnapshotPhase)]
public enum TreeSnapshotPhase
{
    /// <summary>
    /// An offline snapshot is taking the source's shards out of service before
    /// the copy starts.
    /// </summary>
    LockSource = 0,

    /// <summary>
    /// An online snapshot is starting to forward the source's live writes to the
    /// destination before the copy starts.
    /// </summary>
    BeginForwarding = 1,

    /// <summary>The source's shards are being copied into the destination.</summary>
    Copy = 2,

    /// <summary>An offline snapshot is returning a copied shard of the source to service.</summary>
    UnlockSource = 3,
}
