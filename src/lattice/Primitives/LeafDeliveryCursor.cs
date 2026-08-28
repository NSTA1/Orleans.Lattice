namespace Orleans.Lattice.Primitives;

/// <summary>
/// Activation-scoped, in-memory delivery cursor used by
/// <c>LeafCacheGrain</c> to pull only the entries it has not yet
/// observed from its primary <c>BPlusLeafGrain</c>. Decouples the
/// leaf-to-cache delivery path from the LWW HLC ordering used for
/// receiver-side conflict resolution.
/// </summary>
/// <remarks>
/// <para>
/// The motivating bug: under cross-cluster atomic apply the leaf
/// preserves the source cluster's HLC verbatim on every committed
/// entry (required for LWW convergence across clusters with skewed
/// wall clocks). The cache's pre-cursor delta filter
/// <c>lww.Timestamp &gt; callerClock</c> then silently drops those
/// entries whenever the source HLC is below the destination leaf's
/// already-published <c>Version[ReplicaId]</c> - which manifests as a
/// stale cache that never re-fetches the cross-cluster value. The
/// cursor records each entry's per-key write sequence at the moment
/// of the write, irrespective of the LWW HLC, so the cache pulls
/// every write strictly newer than its last delivered sequence even
/// when the underlying HLC has rewound.
/// </para>
/// <para>
/// The cursor's <see cref="Epoch"/> is bumped on every leaf
/// activation. An epoch mismatch on the leaf (or a caller-supplied
/// <see cref="Empty"/> cursor) forces a full-projection delivery: the
/// leaf returns every live entry and the cache resets its position
/// to the leaf's current sequence. The cursor is intentionally
/// <em>not</em> persisted - on a leaf re-activation the cache's saved
/// cursor mismatches, the cache snaps back to a fresh full delivery,
/// and the WAL replay path remains the source of truth for the
/// projection. This keeps the cursor free of any per-write durable
/// I/O.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LeafDeliveryCursor)]
[Immutable]
internal readonly record struct LeafDeliveryCursor
{
    /// <summary>
    /// Per-activation epoch identifier. Bumped on every leaf
    /// activation from a per-process randomly-seeded monotonic
    /// counter, so a cache holding a stale cursor across a leaf
    /// re-activation falls straight back to the full-snapshot delivery
    /// path. The seed is randomised rather than starting at zero
    /// because the epoch is compared across processes: a counter
    /// starting from zero in every silo would hand out the same low
    /// integers everywhere, and two activations in different processes
    /// minting the same epoch would suppress exactly the mismatch this
    /// field exists to signal. The leaf additionally treats a cursor
    /// whose <see cref="Sequence"/> is ahead of its own as stale, so a
    /// residual collision still fails safe.
    /// </summary>
    [Id(0)] public long Epoch { get; init; }

    /// <summary>
    /// Monotonically increasing per-activation write sequence. Bumped
    /// once per <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> store / remove inside the
    /// leaf, regardless of the value's LWW HLC. The cache stores the
    /// highest sequence it has consumed; a key whose sequence is
    /// strictly greater is part of the cache's pending delivery.
    /// </summary>
    [Id(1)] public long Sequence { get; init; }

    /// <summary>
    /// The sentinel cursor used by a freshly-activated cache (or any
    /// caller that has never spoken to this leaf). Triggers a full
    /// snapshot on the next <c>GetDeltaSinceCursorAsync</c>.
    /// </summary>
    public static LeafDeliveryCursor Empty => default;
}
