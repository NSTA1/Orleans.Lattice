namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A tree's monotone cross-tree decision counter (issue #4733), keyed by the
/// logical tree name. Every cross-tree decision on the tree takes the next
/// value as its decision sequence, issued pending until the participant
/// registries record it; the tree's purge frontier never passes a pending
/// sequence.
/// </summary>
[Alias(ReplicationTypeAliases.ICrossTreeDecisionSequenceGrain)]
internal interface ICrossTreeDecisionSequenceGrain : IGrainWithStringKey
{
    /// <summary>
    /// The decision sequence of <paramref name="operationId"/>: the one already
    /// pending for it, or the next counter value, durably pending.
    /// </summary>
    Task<long> IssueAsync(string operationId);

    /// <summary>Durably releases the pending sequence of <paramref name="operationId"/>. Idempotent.</summary>
    Task ConfirmAsync(string operationId);

    /// <summary>The counter and the lowest pending sequence.</summary>
    Task<CrossTreeDecisionSequenceSnapshot> GetAsync();
}
