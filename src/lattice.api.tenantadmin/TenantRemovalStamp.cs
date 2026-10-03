using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Stamps for removing an entry from a tenant's admin set or member set. A removal
/// is a last-writer-wins tombstone, so it takes effect only when its stamp
/// supersedes the slot it removes. The local clock alone does not guarantee that:
/// the slot may have been written by another silo whose clock runs ahead of this
/// one, and a removal stamped behind it would lose the merge while the facade
/// reported the entry removed. Every removal is therefore stamped strictly later
/// than both the local clock and the slot it supersedes.
/// </summary>
internal static class TenantRemovalStamp
{
    /// <summary>A stamp that supersedes <paramref name="subjectId"/>'s admin-set slot in <paramref name="record"/>.</summary>
    /// <param name="clock">The facade's clock.</param>
    /// <param name="record">The record the removal is applied to.</param>
    /// <param name="subjectId">The stored entry id being removed.</param>
    /// <returns>The removal stamp.</returns>
    internal static HybridLogicalClock ForAdminEntry(ITenantAdminClock clock, TenantRecord record, string subjectId) =>
        Supersede(clock.Next(), record.Subjects, subjectId);

    /// <summary>A stamp that supersedes <paramref name="subjectId"/>'s member-set slot in <paramref name="record"/>.</summary>
    /// <param name="clock">The facade's clock.</param>
    /// <param name="record">The record the removal is applied to.</param>
    /// <param name="subjectId">The stored entry id being removed.</param>
    /// <returns>The removal stamp.</returns>
    internal static HybridLogicalClock ForMemberEntry(ITenantAdminClock clock, TenantRecord record, string subjectId) =>
        Supersede(clock.Next(), record.MemberSlots, subjectId);

    private static HybridLogicalClock Supersede(
        HybridLogicalClock next, Dictionary<string, TenantSubjectSlot> slots, string subjectId)
    {
        if (!slots.TryGetValue(subjectId, out var slot) || next > slot.Clock)
        {
            return next;
        }

        // Tick is strictly greater than its argument, so the tombstone wins the
        // merge on the clock alone, whatever the writer ids.
        return HybridLogicalClock.Tick(slot.Clock);
    }
}
