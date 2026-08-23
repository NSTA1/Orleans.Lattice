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
///     <b>HLC clause</b> - a reported consumer cursor above
///     <see cref="HybridLogicalClock.Zero"/> accepts every entry it dominates,
///     OR (independently) a configured TTL ceiling accepts every entry older
///     than the retention window. At least one must accept the entry.
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
    /// <returns>
    /// <see langword="true"/> when the entry may be trimmed; otherwise
    /// <see langword="false"/> (the scan stops at the first such entry).
    /// </returns>
    public static bool IsEntryEligible(
        HybridLogicalClock entryTimestamp,
        VersionVector? entryVectorClock,
        HybridLogicalClock? minCursor,
        HybridLogicalClock? ttlCeiling,
        VersionVector? causalStable,
        HybridLogicalClock? blockedFloor)
    {
        // HLC-shaped clause: cursor OR TTL must accept the entry
        // (existing legacy HLC-only behaviour).
        var hlcAccepted = false;
        if (minCursor is { } mc && mc > HybridLogicalClock.Zero && entryTimestamp <= mc)
        {
            hlcAccepted = true;
        }
        else if (ttlCeiling is { } ceiling && entryTimestamp <= ceiling)
        {
            hlcAccepted = true;
        }

        if (!hlcAccepted)
        {
            return false;
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
                return false;
            }
        }

        // Blocked-floor clause: when at least one consumer reports a
        // non-null buffer pin, every WAL entry whose HLC is at or after
        // the floor is held back so the receiver can recover from buffer
        // state. Strict-less semantics protect the buffered entry itself.
        if (blockedFloor is { } floor && entryTimestamp >= floor)
        {
            return false;
        }

        return true;
    }
}
