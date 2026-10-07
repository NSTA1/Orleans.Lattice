using Orleans.Concurrency;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Origin-side bookkeeping of the cross-tree decision purge hold (issue
/// #4684) for one cross-tree atomic write, keyed by its operation id. Each
/// participant tree records the write-ahead log boundary its own decision was
/// forgotten below; once every peer of every participant has durably
/// acknowledged past that participant's boundary, the barrier on every peer
/// has the participant's arrival and the decision rows may go. Once every
/// participant was released the record collapses to a completed marker, which
/// releases a participant that asks again.
/// </summary>
[Alias(ReplicationTypeAliases.ICrossTreeHoldTrackerGrain)]
internal interface ICrossTreeHoldTrackerGrain : IGrainWithStringKey
{
    /// <summary>The recorded boundaries, or a completed marker.</summary>
    [AlwaysInterleave]
    Task<CrossTreeHoldSnapshot> GetAsync();

    /// <summary>
    /// Durably records <paramref name="boundary"/> for <paramref name="treeId"/>
    /// when none is recorded or the recorded one names another physical log; a
    /// boundary on the same log is kept, so the first (lowest) one stands.
    /// </summary>
    Task RecordBoundaryAsync(string treeId, CrossTreeHoldBoundary boundary);

    /// <summary>
    /// Durably records that the hold released <paramref name="treeId"/>; once
    /// every one of <paramref name="participants"/> was released the record
    /// collapses to the completed marker.
    /// </summary>
    Task MarkReleasedAsync(string treeId, IReadOnlyCollection<string> participants);
}
