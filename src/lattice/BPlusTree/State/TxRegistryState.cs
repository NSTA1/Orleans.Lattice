namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TxRegistryGrain"/>. Holds the
/// recorded commit/abort decisions for atomic-write sagas on a single
/// tree. Entries are added when the saga grain calls
/// <c>MarkCommittedAsync</c> / <c>MarkAbortedAsync</c> and removed when
/// every touched leaf has applied its terminal (the saga grain's
/// post-fan-out cleanup invokes <c>ForgetAsync</c>). The persisted
/// footprint is therefore bounded by the number of in-flight + recently-
/// completed-but-not-yet-cleaned-up sagas, not by the lifetime size of
/// the tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TxRegistryState)]
internal sealed class TxRegistryState
{
    /// <summary>
    /// Map from saga txid to its recorded outcome. Sagas that have been
    /// forgotten (post-cleanup) are absent from this map and resolve to
    /// <see cref="TxStatus.InFlight"/> on lookup — by which point no
    /// leaf has the txid in its pending bucket anymore so that
    /// observation is consistent with the absence of any pending
    /// mutation.
    /// </summary>
    [Id(0)] public Dictionary<Guid, TxStatus> Decisions { get; set; } = [];
}
