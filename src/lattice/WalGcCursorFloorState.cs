namespace Orleans.Lattice;

/// <summary>
/// Why a WAL GC pass's consumer-cursor trim branch was, or was not, usable.
/// <para>
/// <see cref="LatticeWalGcReport.MinCursor"/> alone cannot answer this. It is
/// <see langword="null"/> both when no consumer has ever reported a cursor and
/// when an unusable durable materialiser pin short-circuited the floor,
/// and those two states call for opposite responses: the first is an ordinary
/// quiet tree, the second is a tree that <i>cannot reclaim at all</i> and whose
/// WAL therefore grows without bound. Collapsing them is what let a stranded
/// tree and a healthy one present identically in telemetry (issue #2702).
/// </para>
/// <para>
/// This discriminator is diagnostic and scheduling-facing only. It never widens
/// or narrows the trim predicate: a pass reclaims exactly the entries it would
/// have reclaimed before this value existed.
/// </para>
/// </summary>
public enum WalGcCursorFloorState
{
    /// <summary>
    /// A usable consumer-cursor floor was in effect for this pass, so the cursor
    /// branch of the trim predicate ran. The ordinary healthy value, and the
    /// default for a report that does not state otherwise.
    /// </summary>
    Available = 0,

    /// <summary>
    /// No consumer has reported a cursor, so there was no cursor floor to trim
    /// against. A tree nobody is consuming is legitimately quiet: the WAL is
    /// bounded by <see cref="LatticeOptions.WalRetention"/> if one is
    /// configured, and there is nothing for a GC pass to do until a consumer
    /// appears. Backing off on this state is correct.
    /// </summary>
    NoCursorReported = 1,

    /// <summary>
    /// No durable leaf-materialiser pin for this tree carried a usable offset,
    /// so the cursor branch was disabled for the whole tree and no entry can be
    /// trimmed by cursor no matter how far every other consumer has advanced.
    /// <para>
    /// The cause is deliberately left open in the name, because there is more
    /// than one route in and naming only the obvious one would repeat the very
    /// defect this enum exists to fix. The routes are not equally likely, so
    /// they are given here in the order a diagnosis should consider them.
    /// </para>
    /// <para>
    /// The route that occurs in practice is a leaf that is <i>fully
    /// checkpointed</i> but holds no durable snapshot. The gate reads
    /// <c>min(checkpoint, covered)</c>, and <c>covered</c> is per-activation
    /// in-memory state populated only on snapshot capture or load, so a leaf
    /// can have scanned arbitrarily far and still offer nothing usable, and a
    /// fresh activation starts uncovered regardless of how much it has
    /// persisted. On the deployments measured for issue #2702 this accounted
    /// for the blocked population in full: a census of one affected tree found
    /// no leaf that lacked a checkpoint at all.
    /// </para>
    /// <para>
    /// A leaf carrying no usable checkpoint is the other route. On those same
    /// deployments it was confined to the admin projection rebuild, which
    /// resets the checkpoint deliberately before replaying, so it is not where
    /// a diagnosis should start. Read this value as "the pins are unusable",
    /// not as "the leaf never wrote anything".
    /// </para>
    /// <para>
    /// This is a defect state, not a quiet one. The tree retains its entire WAL
    /// head and keeps growing, and one leaf with an unusable pin is enough to
    /// strand every other leaf in the same tree. A pass that ends here reclaimed
    /// nothing because it was <i>blocked</i>, which is the opposite of having
    /// had nothing to do - so the scheduler must not treat it as quiescent and
    /// must not relax its cadence. See
    /// <see cref="LatticeMetrics.OutcomeBlocked"/>.
    /// </para>
    /// </summary>
    BlockedByUnusablePin = 2,
}
