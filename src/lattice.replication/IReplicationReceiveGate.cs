namespace Orleans.Lattice.Replication;

/// <summary>
/// Read-side gate consulted by the inbound apply path to learn whether a tree's
/// receive fence is currently engaged. Backed by
/// <see cref="Grains.ITreeReceiveFenceGrain"/> but fronted by a short in-memory
/// cache so the apply hot path does not pay a per-entry grain call.
/// </summary>
internal interface IReplicationReceiveGate
{
    /// <summary>
    /// Returns <see langword="true"/> when inbound apply for
    /// <paramref name="treeId"/> is paused by an in-flight restore saga and peer
    /// entries must therefore be deferred rather than applied.
    /// </summary>
    /// <param name="treeId">Physical tree id the inbound entry targets.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<bool> IsReceivePausedAsync(string treeId, CancellationToken cancellationToken = default);
}
