namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The result of an epoch claim (<see cref="IWalShardGrain.ClaimAppendEpochAsync"/>)
/// or a routing-generation re-claim (<see cref="IWalShardGrain.ClaimRoutingGenerationAsync"/>)
/// at a WAL partition (epic #5605).
/// </summary>
/// <param name="Accepted">The claim was written durably and raised in memory.</param>
/// <param name="Head">
/// When accepted, the allocator's next offset read under the lock that raised the
/// entry: every append allocated before the raise lies below it. <c>-1</c> when
/// refused.
/// </param>
/// <param name="Refusal">
/// When refused, the value the claim must exceed: the higher of the entry and,
/// for an epoch claim, the partition's retired floor. <c>0</c> when accepted.
/// </param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalAppendFenceClaimResult)]
internal readonly record struct WalAppendFenceClaimResult(
    [property: Id(0)] bool Accepted,
    [property: Id(1)] long Head,
    [property: Id(2)] long Refusal)
{
    /// <summary>An accepted claim with claim head <paramref name="head"/>.</summary>
    public static WalAppendFenceClaimResult Accept(long head) => new(true, head, 0);

    /// <summary>A refused claim that must exceed <paramref name="refusal"/>.</summary>
    public static WalAppendFenceClaimResult Refuse(long refusal) => new(false, -1, refusal);
}
