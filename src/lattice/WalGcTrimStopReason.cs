namespace Orleans.Lattice;

/// <summary>
/// Why a WAL GC pass's per-shard trim scan stopped advancing, and therefore why
/// it reclaimed the entries it did - or, in the case that matters, none at all.
/// <para>
/// The trim scan walks a shard's entries in ascending offset order and stops at
/// the first entry it may not trim, because offsets are dense and the
/// conservative shape forbids jumping over a retained entry to reclaim a later
/// one. When that first entry is the very first one scanned the pass reclaims
/// nothing, and before this enum existed the two reasons it could happen were
/// indistinguishable from each other and from a shard that had legitimately
/// nothing to do (issue #3149).
/// </para>
/// <para>
/// That ambiguity is not the same one <see cref="WalGcCursorFloorState"/>
/// resolves, and reading the two as interchangeable is what made this defect
/// hard to see. <see cref="WalGcCursorFloorState"/> describes the
/// <i>consumer-cursor</i> floor only. The offset floor - the lowest durably
/// applied leaf-checkpoint offset - is an independent gate applied in the same
/// scan, so a tree can report <see cref="WalGcCursorFloorState.Available"/>,
/// classify every pass as <see cref="LatticeMetrics.OutcomeOverCeiling"/>, and
/// still reclaim nothing because the offset floor sits below its whole retained
/// range. On that path the blocking-pin diagnostic is never written, so the tree
/// presents as breaching its ceiling with a healthy floor and no stated cause.
/// </para>
/// <para>
/// Diagnostic only. It never widens or narrows the trim predicate: a pass
/// reclaims exactly the entries it would have reclaimed before this value
/// existed.
/// </para>
/// </summary>
[InstrumentedEnum(
    typeof(LatticeWalGc),
    "orleans.lattice.wal.gc.trim_stop",
    LatticeMetrics.TagReason)]
internal enum WalGcTrimStopReason
{
    /// <summary>
    /// The scan consumed every entry the provider offered without meeting one it
    /// had to retain. The healthy value: everything present was eligible, so the
    /// shard trimmed as far as it physically could.
    /// </summary>
    Exhausted = 0,

    /// <summary>
    /// The shard held no entries at all, so there was nothing to trim and
    /// nothing to retain. Distinct from <see cref="Exhausted"/> because a shard
    /// that is empty has no backlog to explain, whereas one that exhausted a
    /// non-empty log has just reclaimed something.
    /// </summary>
    Empty = 1,

    /// <summary>
    /// The scan stopped because the first entry it could not trim lay strictly
    /// above the durable materialiser <i>offset</i> floor - the lowest
    /// last-applied leaf-checkpoint offset across the tree's reporting leaves.
    /// <para>
    /// This is the defect state, and it is the one that was previously
    /// unobservable. The offset floor is a <b>minimum over leaves</b>, so a
    /// single leaf whose checkpoint offset never advances holds the floor down
    /// for the entire tree no matter how far every other leaf has progressed.
    /// Every pass then stops on the first entry above that stale floor, trims
    /// nothing, and - because the consumer-cursor floor is untouched and
    /// therefore still <see cref="WalGcCursorFloorState.Available"/> - classifies
    /// as <see cref="LatticeMetrics.OutcomeOverCeiling"/> rather than as
    /// <see cref="LatticeMetrics.OutcomeBlocked"/>. The WAL then grows without
    /// bound while every published floor-state series reads healthy.
    /// </para>
    /// <para>
    /// A transient tick is benign: a floor that is merely a little behind the
    /// head stops the scan having already trimmed a prefix. It is a
    /// <em>sustained</em> run of this reason with zero bytes reclaimed that
    /// identifies a tree whose floor covers none of its retained range.
    /// </para>
    /// </summary>
    OffsetFloor = 2,

    /// <summary>
    /// The scan stopped because the first entry it could not trim failed the
    /// HLC eligibility predicate - the consumer-cursor floor, the TTL ceiling,
    /// the causally stable frontier, or a Zero-HLC block pin.
    /// <para>
    /// Separated from <see cref="OffsetFloor"/> because the two demand different
    /// investigations entirely. This one points at a consumer that has not
    /// advanced its cursor; the other points at a leaf that has not advanced its
    /// durable checkpoint. Collapsing them would send a diagnosis to the wrong
    /// subsystem, which is the failure mode this enum exists to prevent.
    /// </para>
    /// </summary>
    NotEligible = 3,
}
