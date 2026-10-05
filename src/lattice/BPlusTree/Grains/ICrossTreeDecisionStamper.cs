namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stamps a cross-tree atomic write's decision (issue #4684). The replication
/// layer registers an implementation that reads, per participating tree, the
/// tree's snapshot export epoch - read after the decision is durable and before
/// any participant finalizes, so an export whose epoch is greater opened after
/// the decision. A receiver compares that stamp with the epoch of the export it
/// imported a participant from. A host without an implementation stamps
/// nothing.
/// </summary>
internal interface ICrossTreeDecisionStamper
{
    /// <summary>
    /// The decision stamp of every tree in <paramref name="participants"/>. A
    /// failure must propagate, so the coordinator retries before it finalizes.
    /// </summary>
    Task<IReadOnlyDictionary<string, long>> StampAsync(
        string operationId, IReadOnlyList<string> participants, CancellationToken cancellationToken = default);
}
