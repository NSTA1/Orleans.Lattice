using Orleans.Concurrency;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Durable per-tree receiver-side poison set for sagas whose prepare can no
/// longer be applied on this receiver. Grain key: <c>{treeId}</c>.
/// </summary>
[Alias(ReplicationTypeAliases.IReceiverSagaPoisonGrain)]
internal interface IReceiverSagaPoisonGrain : IGrainWithStringKey
{
    /// <summary>
    /// Records that records from <paramref name="originClusterId"/> for
    /// <paramref name="transactionId"/> must be parked with the poisoned-saga
    /// reason. Returns <see langword="false"/> when the bounded set is full.
    /// A successful write also records that a re-seed from the origin is owed
    /// until the caller clears it after a bootstrap kickoff is accepted.
    /// </summary>
    Task<bool> PoisonAsync(string originClusterId, Guid transactionId, string reason);

    /// <summary>
    /// Returns the subset of <paramref name="transactionIds"/> that are poisoned
    /// for <paramref name="originClusterId"/>.
    /// </summary>
    [ReadOnly]
    [AlwaysInterleave]
    Task<IReadOnlyCollection<Guid>> FilterPoisonedAsync(string originClusterId, IReadOnlyCollection<Guid> transactionIds);

    /// <summary>
    /// Returns every poisoned transaction id for <paramref name="originClusterId"/>.
    /// Used by the bootstrap coordinator to settle exactly the set captured at
    /// the start of the drain.
    /// </summary>
    [ReadOnly]
    [AlwaysInterleave]
    Task<IReadOnlyCollection<Guid>> GetPoisonedAsync(string originClusterId);

    /// <summary>
    /// Removes the specified poisoned <paramref name="transactionIds"/> for
    /// <paramref name="originClusterId"/> after a completed full bootstrap from
    /// that origin has settled their pending buckets.
    /// </summary>
    Task RetireAsync(string originClusterId, IReadOnlyCollection<Guid> transactionIds);

    /// <summary>
    /// Returns origins whose poisoned saga set still requires a re-seed kickoff.
    /// </summary>
    [ReadOnly]
    [AlwaysInterleave]
    Task<IReadOnlyCollection<string>> GetReseedOwedOriginsAsync();

    /// <summary>
    /// Sets or clears the durable re-seed owed marker for
    /// <paramref name="originClusterId"/>.
    /// </summary>
    Task SetReseedOwedAsync(string originClusterId, bool owed);
}
