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
    /// This <b>was</b> the defect state and is no longer one (issue #3172).
    /// While entitlement was HLC-only the offset floor could only ever subtract
    /// trim rights: the scan stopped at the floor and the entries below it still
    /// had to win the cursor clause on their own. A floor that is a
    /// <b>minimum over leaves</b> - so a single leaf whose checkpoint offset
    /// never advances holds it down for the entire tree - then produced a pass
    /// that trimmed nothing while every published floor-state series read
    /// healthy, classifying as <see cref="LatticeMetrics.OutcomeOverCeiling"/>
    /// rather than <see cref="LatticeMetrics.OutcomeBlocked"/> because the
    /// consumer-cursor floor was untouched and therefore still
    /// <see cref="WalGcCursorFloorState.Available"/>.
    /// </para>
    /// <para>
    /// Now that the floor also GRANTS entitlement, this reason means the scan
    /// reclaimed everything the durable floor entitles it to and the floor is the
    /// binding constraint - an honest boundary rather than a silent stall. It is
    /// also the reason a healthy tree is expected to report once the offset axis
    /// is doing the work, because a pass whose cursor is behind its floor now
    /// walks to the floor instead of stopping at its first entry. What remains
    /// diagnostic is the SUSTAINED case with zero bytes reclaimed, which still
    /// identifies a floor that covers none of the retained range - the leaf-level
    /// stall, not the entitlement rule.
    /// </para>
    /// </summary>
    OffsetFloor = 2,

    /// <summary>
    /// The scan stopped because the first entry it could not trim failed the
    /// entitlement clause of the eligibility predicate: neither the minimum
    /// consumer cursor, nor the retention TTL ceiling, nor the durable
    /// materialiser offset floor accepted it.
    /// <para>
    /// Indicts the <b>consumer-cursor</b> subsystem - a reader that has not
    /// acknowledged this far, or a retention window that has not aged the entry
    /// out. Separated from <see cref="OffsetFloor"/> because the two demand
    /// different investigations entirely. This one points at a consumer that has
    /// not advanced its cursor; the other points at a leaf that has not advanced
    /// its durable checkpoint. Collapsing them would send a diagnosis to the wrong
    /// subsystem, which is the failure mode this enum exists to prevent.
    /// </para>
    /// <para>
    /// Since issue #3172 this reason additionally means the offset axis did not
    /// rescue the entry, which narrows rather than muddies what it indicts: a
    /// tree reporting it either has no durable offset floor at all (an
    /// unreachable pin store, an all-sentinel pin set, a blocked partition), or
    /// has one and is held by a consumer that reports cursors but no offsets -
    /// a view maintainer, a log subscriber, the backup capture service, the
    /// replication shipper. Both are consumer-side, which is why the name still
    /// fits.
    /// </para>
    /// <para>
    /// Separated from <see cref="CausalFrontier"/> and <see cref="BlockPin"/> on
    /// exactly that same reasoning, applied to the boundary inside the predicate
    /// rather than the one around it (issue #3155). Those three were one arm,
    /// <c>not_eligible</c>, until a tree stopping every scan on it could be shown
    /// to be stranded with no way to say by what.
    /// </para>
    /// </summary>
    CursorFloor = 3,

    /// <summary>
    /// The scan stopped because the first entry it could not trim failed the
    /// causal-stable clause: a per-origin frontier has been reported and it does
    /// not dominate that entry's version vector.
    /// <para>
    /// Indicts the <b>causal frontier</b> - a replication origin whose stability
    /// has not advanced. It is not a restatement of <see cref="CursorFloor"/>: a
    /// consumer's HLC cursor can be arbitrarily far ahead while one origin's entry
    /// in its stable vector stays behind, so a tree held here has a healthy cursor
    /// and an unhealthy origin, and looking at the cursor would clear the wrong
    /// subsystem.
    /// </para>
    /// </summary>
    CausalFrontier = 4,

    /// <summary>
    /// The scan stopped because the first entry it could not trim sits at or above
    /// a consumer's buffer-pin floor.
    /// <para>
    /// Indicts a <b>buffering receiver</b> that published a non-null pin so it can
    /// recover from buffer state. Distinct from the other two arms in kind as well
    /// as in subsystem: this is a deliberate hold rather than a lag, so the
    /// question it raises is whether the pin is still live, not whether something
    /// is still moving. A sustained run of this arm against a consumer that is no
    /// longer buffering is a leaked pin.
    /// </para>
    /// </summary>
    BlockPin = 5,
}
