namespace Orleans.Lattice;

/// <summary>
/// What a WAL GC orphan retirement does with one durable materialiser pin, as
/// decided by <see cref="WalOrphanPinRetirementCore.Decide"/>.
/// </summary>
internal enum OrphanPinRemovalDecision
{
    /// <summary>The pinned leaf's row is bound to a tree: the pin protects a live leaf and stays.</summary>
    KeepLive,

    /// <summary>The leaf read failed: fail-closed, the pin stays.</summary>
    KeepUnreadable,

    /// <summary>The leaf is a husk, but the consumer id is not a leaf's own (issue #4238).</summary>
    RefuseMalformedId,

    /// <summary>The leaf is a husk, but the id's partition is ambiguous under the pinned count (issue #4238).</summary>
    RefuseAmbiguousPartition,

    /// <summary>The pin is retirable, but this pass's retirement budget is spent.</summary>
    Defer,

    /// <summary>Remove the pin, through the fence (issue #5358).</summary>
    Retire,
}
