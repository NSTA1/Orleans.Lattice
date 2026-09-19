namespace Orleans.Lattice;

/// <summary>
/// The offset-space half of the WAL GC's trim entitlement: the durable
/// materialiser offset floor, together with the cursor of the consumers that
/// floor does <b>not</b> speak for. Supplying it to
/// <see cref="WalGcTrimCore.ClassifyEntry"/> lets an entry the HLC clause refuses
/// still be trimmed, on the strictly stronger evidence that every consumer which
/// could replay it has already durably applied it (issue #3172).
/// </summary>
/// <remarks>
/// <para>
/// Before this existed the offset floor was wired in as an additional
/// <em>stop</em> only: it could subtract trim entitlement and never grant it, so
/// entitlement was derived solely from the HLC frontier. The two axes advance
/// independently by design - a tombstone-compaction reap advances a leaf's
/// applied offset while its HLC checkpoint stays flat - so a tree whose consumers
/// make durable progress predominantly through offset-only advance could never
/// reclaim a byte, no matter how much durable progress it proved.
/// </para>
/// <para>
/// <b>Why <see cref="UncoveredCursor"/> is not optional decoration.</b> The
/// offset floor is a minimum over the <i>leaf materialisers</i> that reported an
/// offset, and nothing else. View maintainers, WAL log subscribers, the backup
/// capture service and the replication shipper are all WAL consumers that report
/// an HLC cursor and never report an offset, so an admission rule that looked at
/// the floor alone would trim straight past them - the fall-off-the-log data-loss
/// class the Coyote trim-floor model and the shipping chaos suite both pin.
/// <see cref="UncoveredCursor"/> is the minimum cursor across exactly those
/// consumers the floor does not cover, so the admission is sound consumer by
/// consumer: a covered consumer is protected by the floor, and an uncovered one
/// is protected by this cursor.
/// </para>
/// <para>
/// <b>Fail closed.</b> A caller that cannot establish both halves passes
/// <see langword="null"/> instead of this value, which restores byte-identical
/// pre-#3172 behaviour: an unavailable offset floor admits nothing.
/// </para>
/// </remarks>
/// <param name="Floor">
/// The lowest last-applied leaf-checkpoint offset across the tree's reporting
/// leaves. An entry at or below it has been durably applied by every consumer
/// folded into the minimum.
/// </param>
/// <param name="UncoveredCursor">
/// The minimum HLC cursor across the retention consumers the offset floor does
/// not speak for, or <see langword="null"/> when there are none - in which case
/// the floor alone is a complete proof and every entry at or below it may be
/// admitted. This is the one place in the predicate where <see langword="null"/>
/// widens rather than narrows, and it is sound only because it means "no consumer
/// is left to protect", never "we do not know".
/// </param>
internal readonly record struct WalGcOffsetAdmission(
    long Floor,
    HybridLogicalClock? UncoveredCursor)
{
    /// <summary>
    /// Whether this admission accepts an entry on offset-space evidence alone,
    /// independently of whether the HLC clause accepted it.
    /// </summary>
    /// <param name="entryTimestamp">The entry's Hybrid Logical Clock stamp.</param>
    /// <param name="entryOffset">The entry's WAL offset.</param>
    /// <returns>
    /// <see langword="true"/> when the entry is at or below <see cref="Floor"/>
    /// and no uncovered consumer still needs it.
    /// </returns>
    public bool Admits(HybridLogicalClock entryTimestamp, long entryOffset)
    {
        if (entryOffset > Floor)
        {
            return false;
        }

        if (UncoveredCursor is not { } cursor)
        {
            // Nothing the floor fails to speak for, so the floor is the whole
            // proof.
            return true;
        }

        // An uncovered consumer still constrains the trim, so the entry must
        // clear its cursor as well. The strictly-greater-than-Zero guard mirrors
        // the HLC clause: a Zero cursor is "never reported" rather than "consumed
        // everything", and range-delete entries carry Zero by design, so a Zero
        // cursor must not flush them the moment they land.
        return cursor > HybridLogicalClock.Zero && entryTimestamp <= cursor;
    }
}
