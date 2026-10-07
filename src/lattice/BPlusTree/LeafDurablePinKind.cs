namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Which arm of <see cref="LeafDurablePinCore.Resolve"/> decided a leaf's durable
/// materialiser pin.
/// </summary>
internal enum LeafDurablePinKind : byte
{
    /// <summary>
    /// Nothing applied and nothing to lose (an empty partition, or an empty WAL
    /// behind live rows): the block is released and the leaf's real frontier is
    /// reported with its negative checkpoint.
    /// </summary>
    ReleaseEmpty = 0,

    /// <summary>
    /// A never-written leaf's scanned-through release (issue #3453): a Zero
    /// frontier carrying the persisted checkpoint.
    /// </summary>
    ReleaseNeverWritten = 1,

    /// <summary>
    /// The committed prefix has no durable copy but the WAL: the Zero block pin.
    /// </summary>
    Block = 2,

    /// <summary>
    /// The leaf's frontier with the coverage-gated trim entitlement
    /// <c>min(persisted checkpoint, covered)</c>.
    /// </summary>
    Covered = 3,
}
