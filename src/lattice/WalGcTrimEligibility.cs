namespace Orleans.Lattice;

/// <summary>
/// The verdict <see cref="WalGcTrimCore.ClassifyEntry"/> reaches for a single WAL
/// entry: either the entry may be trimmed, or the one clause of the eligibility
/// predicate that refused it.
/// <para>
/// The three rejection members exist because the predicate's clauses are
/// independent and indict entirely different subsystems (issue #3155). A bare
/// <see langword="false"/> answers "may I trim this?" correctly and answers "who
/// is holding my WAL?" not at all, which is the question an operator is actually
/// asking when a tree stops every scan on its first entry and reclaims nothing.
/// </para>
/// <para>
/// Diagnostic only. The clause an entry is rejected by never changes whether it
/// is rejected: <see cref="WalGcTrimCore.IsEntryEligible"/> is exactly
/// <c>ClassifyEntry(...) == Eligible</c>, so a pass reclaims precisely the
/// entries it would have reclaimed before this enum existed.
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
    /// caller can act on: the offset axis only ever ADDS entitlement, so an entry
    /// reported here was refused by the cursor and either had no offset floor to
    /// appeal to or sat above it - and in the second case the scan stops at the
    /// floor before reaching this verdict anyway.
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
}
