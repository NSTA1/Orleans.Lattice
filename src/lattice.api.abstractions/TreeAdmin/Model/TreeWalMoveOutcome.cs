namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The classification of a WAL move or reclaim operation. The control-API mirror
/// of the core WAL move outcome enum.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalMoveOutcome)]
public enum TreeWalMoveOutcome
{
    /// <summary>The partition's log tail was copied to the target and the placement pin flipped.</summary>
    Moved = 0,

    /// <summary>
    /// The placement pin already mapped the partition to the requested target (an
    /// idempotent retry). No copy was performed; the post-flip repair still ran to
    /// guarantee the cutover completed.
    /// </summary>
    AlreadyAtTarget = 1,

    /// <summary>The orphaned source partition was trimmed by an explicit reclaim call.</summary>
    SourceReclaimed = 2,

    /// <summary>Nothing was done (for example a reclaim of a source that held no data).</summary>
    NoOp = 3,
}
