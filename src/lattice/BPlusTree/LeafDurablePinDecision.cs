namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The verdict of <see cref="LeafDurablePinCore.Resolve"/> for one WAL partition.
/// </summary>
/// <param name="Kind">Which arm of the rule decided the pin.</param>
/// <param name="Offset">
/// The checkpoint offset the pin carries: the trim entitlement for
/// <see cref="LeafDurablePinKind.Covered"/> and
/// <see cref="LeafDurablePinKind.ReleaseNeverWritten"/>, the (negative) current
/// checkpoint for <see cref="LeafDurablePinKind.ReleaseEmpty"/>, and <c>-1</c>
/// for <see cref="LeafDurablePinKind.Block"/>.
/// </param>
internal readonly record struct LeafDurablePinDecision(LeafDurablePinKind Kind, long Offset)
{
    /// <summary>
    /// Whether the pin's frontier is <c>HybridLogicalClock.Zero</c> rather than
    /// the leaf's clock: true for the block pin and the never-written release.
    /// </summary>
    public bool HasZeroFrontier =>
        Kind is LeafDurablePinKind.Block or LeafDurablePinKind.ReleaseNeverWritten;
}
