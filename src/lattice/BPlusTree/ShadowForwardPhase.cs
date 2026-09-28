namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Per-shard lifecycle phase for the online shadow-forwarding primitive.
/// Stored on <c>ShardRootState.ShadowForward</c>; drives the mutation-path
/// prologue and epilogue on <c>ShardRootGrain</c>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShadowForwardPhase)]
internal enum ShadowForwardPhase
{
    /// <summary>
    /// Drain is in progress on this shard. The source shard is authoritative
    /// for reads and writes; each of its last-writer-wins mutations (not a
    /// typed CRDT delta or a bulk append) is mirrored in parallel to the
    /// destination shard with the same index. A background drain concurrently
    /// copies the source shard's existing entries to the destination with a
    /// last-writer-wins merge. The drained entries keep their source HLC
    /// timestamps, but a live forward is stamped by the destination leaf's own
    /// clock (see the shadow-forward notes on <c>ShardRootGrain</c>), so which
    /// version of a key survives can depend on which of the two arrives first.
    /// </summary>
    Draining = 1,

    /// <summary>
    /// The background drain has completed for this shard, but the alias has
    /// not yet been swapped. The shard continues to mirror the same mutations
    /// as in <see cref="Draining"/>, so that those writes landing during the
    /// remaining swap window are not lost. Reads are still served locally.
    /// </summary>
    Drained = 2,

    /// <summary>
    /// The registry alias has been atomically redirected to the destination
    /// tree. Every new operation on this shard throws
    /// <see cref="StaleTreeRoutingException"/>, which the caller's
    /// <c>LatticeGrain</c> catches to refresh its cached routing snapshot
    /// and retry against the destination tree. No further forwarding is
    /// required because the source is no longer serving client traffic.
    /// </summary>
    Rejecting = 3,
}
