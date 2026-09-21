namespace Orleans.Lattice;

/// <summary>
/// Pure, allocation-free decision core for the write-ahead-log garbage
/// collector's per-entry trim eligibility. Extracted verbatim from
/// <see cref="LatticeWalGc"/> so the exact production rule can be driven under
/// systematic (Coyote) interleaving without a silo: a violation the model finds
/// is a violation of the real trim path.
/// </summary>
/// <remarks>
/// <para>
/// The GC trims the largest dense, append-only <em>prefix</em> of a shard's WAL
/// whose every entry is eligible; the scan stops at the first non-eligible entry
/// (offsets are dense, so a conservative "stop at first miss" walk can never
/// jump a still-pinned entry to reap a later one). Eligibility itself is the
/// conjunction of three independent clauses, each of which is <b>load-bearing</b>:
/// removing any one lets the GC trim an entry a live consumer still needs.
/// </para>
/// <list type="number">
///   <item>
///     <b>Entitlement clause</b> - two independent axes, at least one of which
///     must accept the entry (issue #3172), with the offset axis <em>overruling</em>
///     the consumer cursor whenever it is available (issue #3300).
///     <list type="bullet">
///       <item>
///         The <b>HLC axis</b>: a reported consumer cursor above
///         <see cref="HybridLogicalClock.Zero"/> accepts every entry it
///         dominates, OR (independently) a configured TTL ceiling accepts every
///         entry older than the retention window.
///       </item>
///       <item>
///         The <b>offset axis</b>: a <see cref="WalGcOffsetAdmission"/> accepts
///         every entry at or below the durable materialiser offset floor that no
///         consumer outside that floor's population still needs.
///       </item>
///     </list>
///     The two axes advance independently - a tombstone-compaction reap advances
///     a leaf's applied offset while its HLC checkpoint stays flat - so before
///     the offset axis could grant entitlement, a tree making durable progress
///     predominantly through offset-only advance retained its WAL forever.
///     <para>
///     <b>The disjunction is not symmetric, and issue #3300 is what that
///     asymmetry cost.</b> The offset admission is the STRONGER proof of
///     application: it says "every consumer folded into this minimum has
///     durably applied through here", whereas the consumer cursor says only
///     "an in-memory reader has seen this far". Between a strong proof and a
///     weak one, a plain disjunction is just the weak one - so while the offset
///     axis could only ever ADD entitlement, an entry the in-memory cursor had
///     passed was released with no durable evidence required at all, and the
///     WAL copy it destroyed was the only copy. The offset axis may now refuse
///     what the cursor alone would have admitted, reported as
///     <see cref="WalGcTrimEligibility.DurableOffsetRefusal"/>.
///     </para>
///     <para>
///     Two carve-outs keep that narrowing from reintroducing the unbounded
///     growth of issue #3094. A <see langword="null"/> admission changes
///     nothing whatsoever - a tree with no durable floor reporting evaluates
///     the byte-identical pre-#3172 predicate - and the TTL ceiling stays an
///     INDEPENDENT admit, so an operator who has configured a retention window
///     still gets it honoured against a floor that has stalled. Only the cursor
///     arm is overrulable.
///     </para>
///   </item>
///   <item>
///     <b>Causal-stable clause</b> - once any consumer has reported a per-origin
///     frontier, an entry may be trimmed only if that frontier dominates the
///     entry's <see cref="LatticeMutation.VectorClock"/>. A <see langword="null"/>
///     entry vector (legacy / range-delete) is the empty VC and is dominated by
///     every non-null frontier.
///   </item>
///   <item>
///     <b>Blocked-floor clause</b> - once any consumer reports a non-null buffer
///     pin, every entry whose HLC is at or after the floor is held back so a
///     buffering receiver can recover. The comparison is strict-less so the
///     floor entry itself (the buffer's lowest staged entry) survives.
///   </item>
/// </list>
/// </remarks>
internal static class WalGcTrimCore
{
    /// <summary>
    /// Decides whether a single WAL entry is eligible to be trimmed, given the
    /// floors sampled once at the start of a GC pass. This is the exact
    /// predicate <see cref="LatticeWalGc"/> applies to every entry it scans.
    /// </summary>
    /// <param name="entryTimestamp">The entry's Hybrid Logical Clock stamp.</param>
    /// <param name="entryVectorClock">
    /// The entry's per-origin version vector, or <see langword="null"/> for a
    /// legacy or range-delete entry (treated as the empty, always-dominated VC).
    /// </param>
    /// <param name="entryOffset">The entry's dense WAL offset.</param>
    /// <param name="minCursor">
    /// The minimum consumer cursor across all reporting consumers, or
    /// <see langword="null"/> when no consumer has reported one.
    /// </param>
    /// <param name="ttlCeiling">
    /// The retention TTL ceiling, or <see langword="null"/> when retention is
    /// disabled.
    /// </param>
    /// <param name="causalStable">
    /// The causal-stable frontier across consumers, or <see langword="null"/>
    /// when none has been reported (degrades to the HLC-only predicate).
    /// </param>
    /// <param name="blockedFloor">
    /// The lowest buffer-pin HLC across consumers, or <see langword="null"/>
    /// when no consumer is buffering.
    /// </param>
    /// <param name="offsetAdmission">
    /// The offset-space entitlement, or <see langword="null"/> when the durable
    /// materialiser offset floor is unavailable - in which case it neither
    /// admits nor refuses and the predicate is byte-identical to its pre-#3172
    /// behaviour.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the entry may be trimmed; otherwise
    /// <see langword="false"/> (the scan stops at the first such entry).
    /// </returns>
    public static bool IsEntryEligible(
        HybridLogicalClock entryTimestamp,
        VersionVector? entryVectorClock,
        long entryOffset,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        VersionVector? causalStable,
        HybridLogicalClock? blockedFloor,
        WalGcOffsetAdmission? offsetAdmission)
        => ClassifyEntry(
            entryTimestamp,
            entryVectorClock,
            entryOffset,
            minCursor,
            ttlCeiling,
            causalStable,
            blockedFloor,
            offsetAdmission) == WalGcTrimEligibility.Eligible;

    /// <summary>
    /// Decides whether a single WAL entry is eligible to be trimmed and, when it
    /// is not, <em>which</em> of the three independent clauses refused it. This is
    /// the predicate; <see cref="IsEntryEligible"/> is this method with the
    /// attribution discarded.
    /// </summary>
    /// <remarks>
    /// The attribution exists because the clauses indict different subsystems and
    /// demand different investigations (issue #3155). Reporting all three as one
    /// rejection lets a tree be observed to be stranded without its holder ever
    /// being nameable, which is the failure mode the stop-reason instrument was
    /// introduced to remove one level up.
    /// <para>
    /// Clause order is evaluation order and is load-bearing for the attribution
    /// but not for the verdict: an entry refused by more than one clause is
    /// attributed to the first that refuses it, exactly as the scan's
    /// short-circuit already behaved.
    /// </para>
    /// <para>
    /// <see cref="WalGcTrimEligibility.CursorFloor"/> now indicts the entitlement
    /// clause as a whole (issue #3172): it means neither the HLC axis nor the
    /// offset axis accepted the entry. It is still named for the cursor because
    /// that remains the only axis a caller can be holding open when an offset
    /// admission is unavailable, which is the state it is reported in most often.
    /// It is distinct from <see cref="WalGcTrimEligibility.DurableOffsetRefusal"/>,
    /// which is the opposite reading: there the cursor DID accept the entry and
    /// the durable offset floor overruled it (issue #3300).
    /// </para>
    /// </remarks>
    /// <param name="entryTimestamp">The entry's Hybrid Logical Clock stamp.</param>
    /// <param name="entryVectorClock">
    /// The entry's per-origin version vector, or <see langword="null"/> for a
    /// legacy or range-delete entry (treated as the empty, always-dominated VC).
    /// </param>
    /// <param name="entryOffset">The entry's dense WAL offset.</param>
    /// <param name="minCursor">
    /// The minimum consumer cursor across all reporting consumers, or
    /// <see langword="null"/> when no consumer has reported one.
    /// </param>
    /// <param name="ttlCeiling">
    /// The retention TTL ceiling, or <see langword="null"/> when retention is
    /// disabled.
    /// </param>
    /// <param name="causalStable">
    /// The causal-stable frontier across consumers, or <see langword="null"/>
    /// when none has been reported (degrades to the HLC-only predicate).
    /// </param>
    /// <param name="blockedFloor">
    /// The lowest buffer-pin HLC across consumers, or <see langword="null"/>
    /// when no consumer is buffering.
    /// </param>
    /// <param name="offsetAdmission">
    /// The offset-space entitlement, or <see langword="null"/> when the durable
    /// materialiser offset floor is unavailable - in which case it neither
    /// admits nor refuses and the predicate is byte-identical to its pre-#3172
    /// behaviour.
    /// </param>
    /// <returns>
    /// <see cref="WalGcTrimEligibility.Eligible"/> when the entry may be trimmed;
    /// otherwise the clause that refused it.
    /// </returns>
    public static WalGcTrimEligibility ClassifyEntry(
        HybridLogicalClock entryTimestamp,
        VersionVector? entryVectorClock,
        long entryOffset,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        VersionVector? causalStable,
        HybridLogicalClock? blockedFloor,
        WalGcOffsetAdmission? offsetAdmission)
    {
        // Entitlement clause, half one - the HLC axis: cursor OR TTL must accept
        // the entry (the legacy HLC-only behaviour).
        //
        // The two arms are evaluated into separate flags rather than collapsed
        // into one, because half two below treats them differently (issue
        // #3300): the TTL ceiling is an operator retention policy and stays
        // independently sufficient, whereas the cursor is the weaker proof the
        // offset axis may overrule. Their disjunction is exactly the value the
        // single `accepted` flag carried before, so the verdict on every input
        // that does not reach half two is unchanged.
        var cursorAccepts = minCursor is { } mc
            && mc > HybridLogicalClock.Zero
            && entryTimestamp <= mc;
        var ttlAccepts = ttlCeiling is { } ceiling && entryTimestamp <= ceiling;
        var accepted = cursorAccepts || ttlAccepts;

        // Entitlement clause, half two - the offset axis (issues #3172, #3300).
        //
        // The durable materialiser offset floor is a stronger proof of
        // application than the HLC frontier is ("every consumer folded into this
        // minimum has durably applied through here"), and the two axes advance
        // independently, so an entry the HLC axis refuses may still be entitled
        // on offset evidence alone (#3172).
        //
        // The converse now also holds (#3300). Consulting the stronger proof
        // only when the weaker one had already refused made it an ADMIT-ONLY
        // axis, which is the same thing as not consulting it: an entry the
        // in-memory consumer cursor had passed was released with no durable
        // evidence required. That cursor tracks what a leaf folded into its
        // CACHE, so it advances the moment a write lands - the WAL copy was
        // destroyed seconds later and the rows vanished at the next process
        // boundary, which is the loss issue #3300 measured. The offset axis may
        // therefore refuse, and a refusal is attributed to its own verdict so
        // the hold is nameable in a scrape rather than silently
        // indistinguishable from a stalled floor (issue #3094).
        //
        // Note what is NOT overruled. A ttlAccepts entry is still trimmed: an
        // operator who configured a retention window has stated that data past
        // it may be dropped, and honouring that keeps retention bounded on a
        // tree whose floor has stalled. Only the cursor arm is overrulable.
        //
        // It is scoped to the entitlement clause and no further. The
        // causal-stable and blocked-floor clauses below stay conjunctive,
        // because they guard replication origins and buffering receivers - which
        // the offset floor knows nothing about - rather than durable
        // application.
        //
        // A null admission (no durable offset floor on this pass) neither admits
        // nor refuses, so an unavailable floor leaves this predicate exactly as
        // it was.
        if (offsetAdmission is { } admission)
        {
            if (admission.Admits(entryTimestamp, entryOffset))
            {
                accepted = true;
            }
            else if (cursorAccepts && !ttlAccepts)
            {
                return WalGcTrimEligibility.DurableOffsetRefusal;
            }
        }

        if (!accepted)
        {
            return WalGcTrimEligibility.CursorFloor;
        }

        // Causal-stable clause: when at least one consumer has reported
        // a per-origin frontier, the entry's VectorClock must be
        // dominated by it. A null entry vector means the entry pre-dates
        // causal+ stamping or carries the empty frontier by design (range
        // delete) - both are dominated by every non-null frontier. When
        // causalStable itself is null, no consumer has reported a vector
        // and the GC degrades cleanly to the HLC-only predicate.
        if (causalStable is not null)
        {
            if (entryVectorClock is not null && !causalStable.DominatesOrEquals(entryVectorClock))
            {
                return WalGcTrimEligibility.CausalFrontier;
            }
        }

        // Blocked-floor clause: when at least one consumer reports a
        // non-null buffer pin, every WAL entry whose HLC is at or after
        // the floor is held back so the receiver can recover from buffer
        // state. Strict-less semantics protect the buffered entry itself.
        if (blockedFloor is { } floor && entryTimestamp >= floor)
        {
            return WalGcTrimEligibility.BlockPin;
        }

        return WalGcTrimEligibility.Eligible;
    }
}
