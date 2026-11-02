namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The result of <see cref="IWalShardGrain.AppendBatchStampedAsync"/> (epic
/// #5605). Offsets are parallel to the request's entries; an entry the fence
/// rejected carries <see cref="Rejected"/>. A stamp is checked under the same
/// lock that assigns each entry's offset, so the admitted entries are always a
/// prefix: once one entry is rejected, every later one is too.
/// </summary>
/// <param name="Offsets">Per-entry offsets, <see cref="Rejected"/> for a rejected entry.</param>
/// <param name="AdmittedCount">The number of leading entries that were allocated.</param>
/// <param name="UnstampedWatermark">
/// The highest offset the partition had allocated to an unstamped append when the
/// last admitted entry was allocated; <c>-1</c> when none.
/// </param>
/// <param name="Outcome">
/// <see cref="WalAppendFenceOutcome.Admitted"/> when every entry was allocated,
/// otherwise the reason the first rejected entry was refused.
/// </param>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.WalStampedBatchResult)]
internal readonly record struct WalStampedBatchResult(
    [property: Id(0)] long[] Offsets,
    [property: Id(1)] int AdmittedCount,
    [property: Id(2)] long UnstampedWatermark,
    [property: Id(3)] WalAppendFenceOutcome Outcome)
{
    /// <summary>The offset a rejected entry carries.</summary>
    public const long Rejected = -1;
}
