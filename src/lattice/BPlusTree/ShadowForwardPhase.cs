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
    /// for reads and writes; every accepted mutation is mirrored in parallel
    /// to the destination shard. A background drain concurrently copies the
    /// source shard's existing entries to the destination using the raw-entry
    /// bulk-load path; LWW convergence is guaranteed because both the live
    /// forwards and the drain entries carry their original HLC timestamps.
    /// </summary>
    Draining = 1,

    /// <summary>
    /// The background drain has completed for this shard, but the alias has
    /// not yet been swapped. The shard continues to mirror every accepted
    /// mutation so that writes landing during the remaining swap window are
    /// not lost. Reads are still served locally.
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
