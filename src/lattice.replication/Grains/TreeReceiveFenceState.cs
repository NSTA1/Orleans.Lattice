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
}
