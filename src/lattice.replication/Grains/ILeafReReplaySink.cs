using Orleans.Lattice;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// The seam a targeted leaf re-replay repair pass re-ships selected
/// write-ahead-log entries through. Abstracting delivery behind this interface
/// keeps the <see cref="LeafReReplayer"/> engine unit-testable with an
/// in-memory stub; the production implementation is
/// <see cref="TransportLeafReReplaySink"/>, which frames the entries through
/// the ordinary replication transport so they travel the same causal-stable
/// apply path as ordinary replication.
/// </summary>
internal interface ILeafReReplaySink
{
    /// <summary>
    /// Re-ships the supplied entries to <paramref name="peer"/> for the named
    /// tree and returns the number of entries the peer accepted (zero when the
    /// delivery was rejected).
    /// </summary>
    /// <param name="peer">The diverged peer cluster id to re-ship to.</param>
    /// <param name="treeName">The logical replicated-tree name.</param>
    /// <param name="entries">The entries to re-ship, in apply order.</param>
    /// <param name="cancellationToken">A token to observe for cancellation.</param>
    ValueTask<int> ReplayAsync(
        string peer,
        string treeName,
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken);
}
