namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free rule deciding what moved-away seal a leaf must carry
/// when it is born from, or seeded by, another leaf that already holds one.
/// Extracted so the decision the production leaf grain executes on its birth seam
/// (<c>BPlusLeafGrain.InitializeSiblingAsync</c>) is the same artifact the Coyote
/// model drives under systematic schedule exploration, with no possibility of
/// drift.
/// <para>
/// <b>Why a division needs this rule at all.</b> The seal recorded by
/// <c>MarkSlotsMovedAwayAsync</c> is what stops a source leaf resurfacing the
/// orphan snapshot of a slot that has migrated to another shard. It is keyed by
/// the key's HASH (<c>ShardMap.GetVirtualSlot</c>), not by the leaf's declared
/// range, so a sealed slot is a residue class scattered across the whole keyspace
/// rather than a contiguous span. A division hands <c>[pivot, High)</c> to a
/// freshly minted sibling along with every row at or above the pivot, so - for
/// exactly the reason <c>BPlusLeafGrain.HasWidenBlockingState</c> gives for
/// testing a seal whole rather than intersecting it with a range - <em>any</em>
/// non-trivial split of a sealed donor hands the sibling part of a sealed residue
/// class. There is no pivot that avoids it.
/// </para>
/// <para>
/// <b>What goes wrong without it.</b> The sibling is born unsealed while holding
/// rows whose keys hash into migrated slots, and it serves them through every read
/// path. The authoritative value lives on the destination shard, so the stale copy
/// never heals: writes for that slot route to the new owner and never reach the
/// sibling. Issue 3121.
/// </para>
/// <para>
/// <b>Why the existing seal guards do not cover it.</b>
/// <c>HasReclaimBlockingState</c> asks whether a leaf may be REMOVED and
/// <c>HasWidenBlockingState</c> asks whether a leaf may TAKE OVER another range
/// (the asymmetry issue #2143 reported). Both concern leaves that already exist. A
/// division poses a third question - may a NEW leaf be minted over part of a
/// sealed leaf's range without the seal - and the sibling does not exist yet at
/// the decision point, so neither predicate is reachable.
/// </para>
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no wall-clock, and no Orleans types.
/// <see cref="TryInherit"/> is a total function of two seals that allocates
/// nothing on any path except a genuine merge, where it allocates exactly one
/// exactly-sized array. The universal case - a freshly minted sibling with no seal
/// of its own - hands back the donor's array <em>by reference</em>; see the
/// remarks on that method for why sharing it is safe.
/// </para>
/// </summary>
internal static class MovedAwaySealInheritance
{
    /// <summary>
    /// Computes the seal a leaf must carry once it inherits
    /// <paramref name="donorSlots"/> / <paramref name="donorVsc"/> on top of the
    /// seal it already holds, and reports whether that differs from what it holds
    /// today.
    /// <para>
    /// Returns <see langword="false"/> - with the outputs left at the existing
    /// values - whenever nothing needs to change, which lets the caller skip a
    /// persist entirely. That covers the donor having no seal, a re-call against an
    /// already-seeded sibling, and the incomparable slot-space case below.
    /// </para>
    /// <para>
    /// <b>The merge is monotonic and never drops a slot.</b> A seal is sticky by
    /// design: once a slot has migrated, the source side must never surface its
    /// orphan snapshot again. Inheritance is therefore a union, not an assignment,
    /// so it is safe to run against a sibling that has somehow already been sealed
    /// and safe to re-run after a partial birth.
    /// </para>
    /// <para>
    /// <b>Incomparable slot spaces are left alone rather than merged.</b> A slot
    /// index only means anything under the virtual shard count it was recorded
    /// with, so when both sides hold a seal under different counts there is no
    /// correct union to take - merging would blend two different slot spaces and
    /// overwriting would silently drop live seals. Declining is the safe answer,
    /// and it is not a hole in practice: a sibling that has already been sealed
    /// independently is one the shard-side walk already knows about, so that walk
    /// re-seals it under the current count.
    /// </para>
    /// <para>
    /// <b>Allocation.</b> Nothing is allocated when the donor has no seal, when the
    /// receiver has no seal (the donor's array is handed back by reference), when
    /// every donor slot is already present, or when the slot spaces are
    /// incomparable. Only a genuine union allocates, and then exactly one array
    /// sized to the exact union count by a counting pre-pass - the same discipline
    /// <c>UnmarkSlotsMovedAwayAsync</c> uses.
    /// </para>
    /// </summary>
    /// <remarks>
    /// <b>The donor's array is handed back by reference rather than copied</b>, which
    /// keeps this core a pure, allocation-free decision on its dominant path. Whether
    /// that reference may be <em>retained</em> is the caller's question, not this
    /// core's: <c>BPlusLeafGrain.InitializeSiblingAsync</c> copies before persisting,
    /// because the array reached it inside an <c>[Immutable]</c> payload whose
    /// same-silo deep copy is elided, and two leaves must not share one persisted
    /// array. A caller that only reads the result needs no copy at all.
    /// </remarks>
    /// <param name="existingSlots">The receiving leaf's current sealed slots, or <see langword="null"/>.</param>
    /// <param name="existingVsc">The virtual shard count the receiver's seal was recorded under, or <see langword="null"/>.</param>
    /// <param name="donorSlots">The donor's sealed slots, or <see langword="null"/>.</param>
    /// <param name="donorVsc">The virtual shard count the donor's seal was recorded under, or <see langword="null"/>.</param>
    /// <param name="mergedSlots">The slots the receiver must carry when this returns <see langword="true"/>.</param>
    /// <param name="mergedVsc">The virtual shard count the receiver must record when this returns <see langword="true"/>.</param>
    public static bool TryInherit(
        int[]? existingSlots,
        int? existingVsc,
        int[]? donorSlots,
        int? donorVsc,
        out int[]? mergedSlots,
        out int? mergedVsc)
    {
        mergedSlots = existingSlots;
        mergedVsc = existingVsc;

        // The donor carries no seal, so there is nothing to inherit. A virtual
        // shard count with no slots is the "seal just lifted" stamp, which is
        // deliberately inert and must not be propagated as though it were a seal.
        if (donorSlots is not { Length: > 0 } donor || donorVsc is not { } donorCount || donorCount <= 0)
        {
            return false;
        }

        // The universal case: a freshly minted sibling holds no seal of its own, so
        // it takes the donor's verbatim. Handing back the donor's array by
        // reference is what keeps the birth seam allocation-free; see the remarks.
        if (existingSlots is not { Length: > 0 } existing)
        {
            mergedSlots = donor;
            mergedVsc = donorCount;
            return true;
        }

        // Both sides hold a seal, but under slot spaces that cannot be compared.
        if (existingVsc != donorCount)
        {
            return false;
        }

        // Counting pre-pass: nothing is allocated when the donor's slots are
        // already present, which is what makes a re-call against a partially
        // seeded sibling free as well as correct.
        //
        // A slot equal to its predecessor is skipped rather than counted twice.
        // Both writers keep the persisted arrays sorted and distinct, so this
        // cannot fire in production - but the count sizes the result array exactly,
        // and over-counting would leave trailing zeros that read as sealed slot 0.
        // One comparison buys total correctness on any sorted input.
        var additional = 0;
        for (var i = 0; i < donor.Length; i++)
        {
            if (i > 0 && donor[i] == donor[i - 1])
            {
                continue;
            }

            if (Array.BinarySearch(existing, donor[i]) < 0)
            {
                additional++;
            }
        }

        if (additional == 0)
        {
            return false;
        }

        // Linear merge of two sorted arrays into an exactly-sized result. Donor
        // duplicates are skipped on the same rule the counting pass used, so the
        // two always agree on the length and the result stays distinct.
        var union = new int[existing.Length + additional];
        int left = 0, right = 0, write = 0;
        while (left < existing.Length && right < donor.Length)
        {
            if (right > 0 && donor[right] == donor[right - 1])
            {
                right++;
                continue;
            }

            var a = existing[left];
            var b = donor[right];
            if (a == b)
            {
                union[write++] = a;
                left++;
                right++;
            }
            else if (a < b)
            {
                union[write++] = a;
                left++;
            }
            else
            {
                union[write++] = b;
                right++;
            }
        }

        while (left < existing.Length)
        {
            union[write++] = existing[left++];
        }

        while (right < donor.Length)
        {
            if (right > 0 && donor[right] == donor[right - 1])
            {
                right++;
                continue;
            }

            union[write++] = donor[right++];
        }

        mergedSlots = union;
        mergedVsc = donorCount;
        return true;
    }
}
