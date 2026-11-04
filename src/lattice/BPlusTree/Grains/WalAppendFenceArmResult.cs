namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The result of an arm (<see cref="IWalShardGrain.ArmAppendEpochAsync"/>) at a WAL
/// partition (#5833, revision 2): the epoch claim's outcome, plus what the arming
/// activation must have read to before it may advance live there.
/// </summary>
/// <param name="Accepted">The epoch claim was written durably and raised in memory.</param>
/// <param name="Head">When accepted, the claim head; <c>-1</c> when refused.</param>
/// <param name="Refusal">When refused, the value the epoch claim must exceed; <c>0</c> when accepted.</param>
/// <param name="ForeignHigh">
/// When accepted, the highest offset of a record of the leaf that any other activation
/// may have allocated at the partition, read after the raise (B1); <c>-1</c> when none
/// or refused.
/// </param>
/// <param name="ReclaimHead">
/// When accepted and a deferred hand-off close's generation was resolved, the head of
/// that re-claim (C2); <c>-1</c> otherwise.
/// </param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalAppendFenceArmResult)]
internal readonly record struct WalAppendFenceArmResult(
    [property: Id(0)] bool Accepted,
    [property: Id(1)] long Head,
    [property: Id(2)] long Refusal,
    [property: Id(3)] long ForeignHigh,
    [property: Id(4)] long ReclaimHead)
{
    /// <summary>A refused arm that must exceed <paramref name="refusal"/>.</summary>
    public static WalAppendFenceArmResult Refuse(long refusal) => new(false, -1, refusal, -1, -1);
}
