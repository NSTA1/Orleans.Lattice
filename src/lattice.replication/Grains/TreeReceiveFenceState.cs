namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Persistent state for <see cref="ITreeReceiveFenceGrain"/>. Records the saga
/// that has durably paused inbound apply for the tree, so the pause survives an
/// activation restart across the whole cutover window.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.TreeReceiveFenceState)]
internal sealed class TreeReceiveFenceState
{
    /// <summary>
    /// Identifier of the cross-cluster saga that has paused inbound apply for
    /// this tree, or <c>null</c> when inbound apply runs normally.
    /// </summary>
    [Id(0)]
    public string? PauseSagaId { get; set; }

    /// <summary>
    /// The fence's epoch: bumped by every new pause (issue #4593), never by a
    /// resume. Zero for a tree no saga ever paused.
    /// </summary>
    [Id(1)]
    public long Epoch { get; set; }
}
