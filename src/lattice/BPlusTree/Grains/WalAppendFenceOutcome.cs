namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>The outcome of a stamped WAL append (epic #5605).</summary>
[GenerateSerializer]
[Alias(TypeAliases.WalAppendFenceOutcome)]
internal enum WalAppendFenceOutcome : byte
{
    /// <summary>Every entry was allocated an offset.</summary>
    Admitted = 0,

    /// <summary>
    /// The stamp's epoch is below the leaf's entry or at or below the partition's
    /// retired floor: the activation has been superseded. Entries allocated before
    /// the rejection keep their offsets; the rest carry <see cref="WalStampedBatchResult.Rejected"/>.
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
