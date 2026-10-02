namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The durable state of the leaf behind the pin holding a tree's WAL floor, read
/// from the leaf's persisted checkpoint without activating it. The control-API
/// mirror of the core WAL GC blocking-pin classification.
/// </summary>
/// <remarks>
/// <see cref="NeverCheckpointed"/> has two opposite readings and is meaningful
/// only beside the pin offset: at offset <c>-1</c> it is the benign sentinel that
/// clears when the leaf checkpoints, while at an offset <c>&gt;= 0</c> it is a
/// stranded pin that holds the floor for good. Read
/// <see cref="TreeWalReclamationReport.IsWedged"/> rather than this value alone.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeWalFloorHolderState)]
public enum TreeWalFloorHolderState
{
    /// <summary>The leaf has durably checkpointed the partition and its pin is known unusable: the snapshot coverage it is floored against is absent.</summary>
    CheckpointedUncovered = 0,

    /// <summary>The leaf has never durably checkpointed the partition (its persisted checkpoint is the <c>-1</c> sentinel).</summary>
    NeverCheckpointed = 1,

    /// <summary>The storage provider answered and holds no durable state for the leaf.</summary>
    NoDurableState = 2,

    /// <summary>The leaf's state could not be read, so no claim is made about it.</summary>
    Unreadable = 3,

    /// <summary>The leaf's durable state carries no tree: the leaf was reclaimed after publishing the pin.</summary>
    Orphaned = 4,

    /// <summary>The leaf has durably checkpointed the partition; whether that checkpoint is covered is not determinable from durable state.</summary>
    CheckpointedCoverageUnknown = 5,
}
