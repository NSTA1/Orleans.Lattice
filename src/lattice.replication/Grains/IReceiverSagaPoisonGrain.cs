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
    /// Which of <paramref name="transactionIds"/> from
    /// <paramref name="originClusterId"/> are poisoned and which are quarantined
    /// (issue #4692), in one read.
    /// </summary>
    [AlwaysInterleave]
    Task<ReceiverSagaPoisonClassification> ClassifyAsync(string originClusterId, IReadOnlyCollection<Guid> transactionIds);

    /// <summary>
    /// Whether a completed re-seed has already retired a poison of
    /// <paramref name="transactionId"/> from <paramref name="originClusterId"/>
    /// (issue #4692), so poisoning it again could not settle it.
    /// </summary>
    [AlwaysInterleave]
    Task<bool> IsRetiredAsync(string originClusterId, Guid transactionId);

    /// <summary>
    /// Quarantines <paramref name="transactionId"/> from
    /// <paramref name="originClusterId"/> durably (issue #4692): its records are
    /// parked without being applied, and it is never re-seeded again for that
    /// cause. Returns <see langword="false"/> when the bounded quarantine set is
    /// full.
    /// </summary>
    Task<bool> QuarantineAsync(string originClusterId, Guid transactionId, string reason);

    /// <summary>The quarantined sagas from <paramref name="originClusterId"/>.</summary>
    [AlwaysInterleave]
    Task<IReadOnlyCollection<Guid>> GetQuarantinedAsync(string originClusterId);

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
