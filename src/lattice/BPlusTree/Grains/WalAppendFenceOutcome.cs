namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>The outcome of a stamped WAL append (epic #5605).</summary>
/// <remarks>
/// Every member maps to its own <see cref="LatticeMetrics.TagReason"/> value
/// through <see cref="LatticeMetrics.AppendFenceRejectionReason"/>.
/// <see cref="Admitted"/> is mapped for exhaustiveness only: an admitted append is
/// never recorded as a rejection.
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalAppendFenceOutcome)]
[InstrumentedEnum(
    typeof(LatticeMetrics),
    "orleans.lattice.wal.append_fence.rejections",
    LatticeMetrics.TagReason)]
internal enum WalAppendFenceOutcome : byte
{
    /// <summary>Every entry was allocated an offset.</summary>
    Admitted = 0,

    /// <summary>
    /// The stamp's epoch is below the leaf's entry: the activation has been
    /// superseded. The partition's retired floor is not an append-time check (#5635).
    /// A stamped batch is rejected at its first entry, so nothing of it is allocated;
    /// every entry carries <see cref="WalStampedBatchResult.Rejected"/>.
    /// </summary>
    StaleEpoch = 1,

    /// <summary>
    /// The leaf's entry is not loaded at this partition (the shard restarted or
    /// moved since the claim). Nothing was allocated; the leaf re-presents its claim
    /// and retries.
    /// </summary>
    UnloadedEntry = 2,

    /// <summary>The stamp's routing generation is below the initiator's entry.</summary>
    StaleGeneration = 3,

    /// <summary>
    /// The initiator's generation entry is not loaded at this partition. Nothing
    /// was allocated; the initiator re-presents its re-claim and retries.
    /// </summary>
    UnloadedGeneration = 4,
}
