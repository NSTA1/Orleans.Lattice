namespace Orleans.Lattice;

/// <summary>
/// The verdict <see cref="WalGcTrimCore.ClassifyEntry"/> reaches for a single WAL
/// entry: either the entry may be trimmed, or the one clause of the eligibility
/// predicate that refused it.
/// <para>
/// The four rejection members exist because the predicate's clauses are
/// independent and indict entirely different subsystems (issue #3155). A bare
/// <see langword="false"/> answers "may I trim this?" correctly and answers "who
/// is holding my WAL?" not at all, which is the question an operator is actually
/// asking when a tree stops every scan on its first entry and reclaims nothing.
/// </para>
/// <para>
/// <b>Diagnostic only, with one exception.</b> For every member but
/// <see cref="DurableOffsetRefusal"/>, the clause an entry is rejected by never
/// changes whether it is rejected: <see cref="WalGcTrimCore.IsEntryEligible"/> is
/// exactly <c>ClassifyEntry(...) == Eligible</c>, so a pass reclaims precisely
/// the entries it would have reclaimed before this enum existed.
/// <see cref="DurableOffsetRefusal"/> (issue #3300) is a verdict the predicate
/// reaches because it now narrows, and is called out rather than left implicit
/// because "diagnostic only" was a load-bearing guarantee of this enum.
/// </para>
/// </summary>
internal enum WalGcTrimEligibility
{
    /// <summary>
    /// Every clause accepted the entry, so it may be trimmed.
    /// </summary>
    Eligible = 0,

    /// <summary>
    /// The entitlement clause refused the entry: neither the minimum consumer
    /// cursor, nor the retention TTL ceiling, nor the durable materialiser offset
    /// floor accepted it.
    /// <para>
    /// Indicts the <b>consumer-cursor</b> subsystem - a reader that has not
    /// acknowledged this far, or a retention window that has not aged this entry
    /// out. It is the only one of the three rejections that can be cleared by a
    /// consumer simply catching up.
    /// </para>
    /// <para>
    /// The name is retained from when the clause was HLC-only (issue #3172 added
    /// the offset axis as a disjunct) because the cursor remains the axis a
    /// caller can act on: an entry reported here was refused by the cursor and
    /// either had no offset floor to appeal to or sat above it - and in the
    /// second case the scan stops at the floor before reaching this verdict
    /// anyway.
    /// </para>
    /// <para>
    /// Not to be confused with <see cref="DurableOffsetRefusal"/>, which is the
    /// exact converse (issue #3300): there the cursor ACCEPTED the entry and the
    /// durable offset floor overruled it. The two are separated because they
    /// indict opposite subsystems - this one a consumer that has not caught up,
    /// that one a leaf that has not made its applied state durable - and because
    /// only this one clears itself when a reader advances.
    /// </para>
    /// </summary>
    CursorFloor = 1,

    /// <summary>
    /// The causal-stable clause refused the entry: a per-origin frontier has been
    /// reported and it does not dominate the entry's version vector.
    /// <para>
    /// Indicts the <b>causal frontier</b> - a replication origin whose stability
    /// has not advanced. Distinct from <see cref="CursorFloor"/> because a
    /// consumer's HLC cursor can be arbitrarily far ahead while one origin's
    /// entry in its stable vector stays behind, so "the consumer is caught up" and
    /// "the frontier dominates" are not the same statement.
    /// </para>
    /// </summary>
    CausalFrontier = 2,

    /// <summary>
    /// The blocked-floor clause refused the entry: a consumer reported a non-null
    /// buffer pin and the entry's HLC is at or above that floor.
    /// <para>
    /// Indicts a <b>buffering receiver</b> holding a pin so it can recover from
    /// buffer state. Unlike the other two this is a deliberate hold rather than a
    /// lag, so the investigation is whether the pin is still live rather than
    /// whether some cursor is moving.
    /// </para>
    /// </summary>
    BlockPin = 3,

    /// <summary>
    /// The entitlement clause refused the entry because the durable materialiser
    /// offset floor overruled a consumer cursor that would have admitted it: the
    /// minimum consumer cursor dominates the entry, no retention TTL ceiling
    /// covers it, and the supplied <see cref="WalGcOffsetAdmission"/> declined it
    /// (issue #3300).
    /// <para>
    /// Indicts the <b>durable materialiser</b>, not the consumer. Every other
    /// rejection on this enum names something that has not advanced far enough;
    /// this one names a tree where something HAS advanced - the in-memory
    /// consumer cursor - past what any durable evidence supports. That cursor
    /// tracks what a leaf folded into its CACHE, so it advances the moment a
    /// write lands, and before this verdict existed the collector released the
    /// WAL entry on that alone. The WAL copy was the only copy, so the rows
    /// survived in memory until the next process boundary and then were gone -
    /// with the pass reporting a healthy arm throughout.
    /// </para>
    /// <para>
    /// <b>Unlike the other three, this rejection is not purely diagnostic.</b>
    /// The other members label a decision the scan would have taken anyway; this
    /// one exists because the predicate now narrows. It is reachable only where a
    /// durable offset floor was actually established for the partition, so a tree
    /// that reports no offsets is untouched, and a configured retention TTL still
    /// admits independently - an operator who has stated that data past a window
    /// may be dropped still gets that honoured against a floor that has stalled.
    /// </para>
    /// <para>
    /// A sustained run of this arm with nothing reclaimed is a leaf whose durable
    /// checkpoint or snapshot coverage has stopped advancing, which is the state
    /// to investigate - and naming it is the point. A silent hold here would be
    /// indistinguishable from the stalled-floor state of issue #3094, and those
    /// two demand opposite responses: this one means data is being CORRECTLY
    /// retained pending durability, that one means retention has no path to
    /// clear.
    /// </para>
    /// </summary>
    DurableOffsetRefusal = 4,
}
