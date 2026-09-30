namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The step an online resize has reached, reported by
/// <see cref="TreeResizeStatus.Phase"/>. The steps run in the order declared.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeResizePhase)]
public enum TreeResizePhase
{
    /// <summary>
    /// The tree is being copied, shard by shard, into a destination at the new
    /// capacity while live writes are forwarded to it.
    /// </summary>
    Copy = 0,

    /// <summary>The copy is complete and the tree's name is being pointed at the destination.</summary>
    Swap = 1,

    /// <summary>The old copy's shards are being set to turn away requests that still reach them.</summary>
    RejectOldShards = 2,

    /// <summary>The old copy is being retired: soft-deleted, so the resize can still be undone.</summary>
    RetireOldCopy = 3,

    /// <summary>
    /// An undo has been accepted and is unwinding the resize (see
    /// <see cref="TreeResizeStatus.UndoRequested"/>).
    /// </summary>
    Undo = 4,
}
