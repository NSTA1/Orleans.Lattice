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

    /// <summary>
    /// Issues the operation's decision sequence on every tree in
    /// <paramref name="participants"/> (issue #4733): the next value of each
    /// tree's monotone decision counter, held pending until
    /// <see cref="ConfirmSequencesAsync"/>, so the tree's purge frontier stays
    /// below it until the sequence is recorded where the frontier reads it.
    /// Idempotent per operation while pending. A failure must propagate.
    /// </summary>
    Task<IReadOnlyDictionary<string, long>> IssueSequencesAsync(
        string operationId, IReadOnlyList<string> participants, CancellationToken cancellationToken = default);

    /// <summary>
    /// Releases the operation's pending sequences once every prepared
    /// participant's transaction registry has recorded them. Idempotent.
    /// </summary>
    Task ConfirmSequencesAsync(
        string operationId, IReadOnlyList<string> participants, CancellationToken cancellationToken = default);
}
